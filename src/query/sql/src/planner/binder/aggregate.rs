// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Literal;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use indexmap::Equivalent;
use itertools::Itertools;

use super::Any;
use super::ExprContext;
use super::Finder;
use super::GROUPING_ID_COLUMN_NAME;
use super::is_grouping_id_item;
use super::prune_by_children;
use super::reject_grouping_functions;
use crate::BindContext;
use crate::MetadataRef;
use crate::Symbol;
use crate::binder::AliasLookup;
use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::binder::ColumnBindingBuilder;
use crate::binder::GroupingKeyScope;
use crate::binder::GroupingScopeState;
use crate::binder::NameResolutionResult;
use crate::binder::ScalarFoldingPolicy;
use crate::binder::SelectClauseExprs;
use crate::binder::SelectItems;
use crate::binder::Visibility;
use crate::binder::project_set::SetReturningAnalyzer;
use crate::binder::scalar::ScalarBinder;
use crate::binder::select::SelectList;
use crate::optimizer::ir::SExpr;
use crate::planner::semantic::CoreExprArena;
use crate::planner::semantic::CoreExprId;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::TypeChecker;
use crate::planner::semantic::normalize_identifier;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::GroupingSets;
use crate::plans::MAX_GROUPING_KEYS;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UDAFCall;
use crate::plans::Visitor;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

/// Information for `GROUPING SETS`.
///
/// `GROUPING SETS` will generate several `GROUP BY` sets, and union their results. For example:
///
/// ```sql
/// SELECT min(a), b, c FROM t GROUP BY GROUPING SETS (b, c);
/// ```
///
/// is equal to:
///
/// ```sql
/// (SELECT min(a), b, NULL FROM t GROUP BY b) UNION (SELECT min(a), NULL, c FROM t GROUP BY c);
/// ```
///
/// In Databend, we do not really rewrite the plan to a `UNION` plan.
/// We will add a new virtual column `_grouping_id` to the group by items,
/// where `_grouping_id` is the result value of function [grouping](https://docs.databend.com/sql/sql-functions/other-functions/grouping).
///
/// For example, we will rewrite the SQL above to:
///
/// ```sql
/// SELECT min(a), b, c FROM t GROUP BY (b, c, _grouping_id);
/// ```
///
/// To get the right result, we also need to fill dummy data for each grouping set.
///
/// The above SQL again, if the columns' data is:
///
///  a  |  b  |  c
/// --- | --- | ---
///  1  |  2  |  3
///  4  |  5  |  6
///
/// We will expand the data to:
///
/// - Grouping sets (b):
///
///  a  |  b  |   c    | grouping(b,c)
/// --- | --- |  ---   |    ---
///  1  |  2  |  NULL  |     2 (0b10)
///  4  |  5  |  NULL  |     2 (0b10)
///
/// - Grouping sets (c):
///
///  a  |   b    |  c  | grouping(b,c)
/// --- |  ---   | --- |   ---
///  1  |  NULL  |  3  |    1 (0b01)
///  4  |  NULL  |  6  |    1 (0b01)
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GroupingSetsInfo {
    /// Index for virtual column `grouping_id`.
    pub grouping_id_column: ColumnBinding,
    /// The keys of each grouping set, as the output symbols in `group_items`.
    ///
    /// Each key of a grouping-sets aggregate has a dedicated nullable output
    /// symbol that receives the injected NULLs; its group item keeps the
    /// pre-aggregate expression. Aggregate arguments and sort keys never read
    /// the output symbols.
    pub sets: Vec<Vec<Symbol>>,
}

/// A GROUP BY item, lowered with the other clauses of its SELECT, see
/// `select_clauses.rs`.
#[derive(Clone)]
pub(crate) enum GroupItem {
    /// The SELECT item at a zero-based position, written as a one-based
    /// position or standing for one in GROUP BY ALL.
    Position(usize),
    Expr {
        root: CoreExprId,
        /// The name of the key output: the expression as written.
        name: String,
    },
}

impl GroupItem {
    /// Lower a GROUP BY item of a SELECT with `select_len` items. An integer
    /// literal is a one-based position in the SELECT list.
    fn lower<'a>(arena: &mut CoreExprArena<'a>, expr: &'a Expr, select_len: usize) -> Result<Self> {
        Ok(match expr {
            Expr::Literal {
                span,
                value: Literal::UInt64(position),
            } => {
                if *position < 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "GROUP BY position {position} is illegal"
                    ))
                    .set_span(*span));
                }
                if *position > select_len as u64 {
                    return Err(ErrorCode::SemanticError(format!(
                        "GROUP BY position {position} is not in select list"
                    ))
                    .set_span(*span));
                }
                Self::Position(*position as usize - 1)
            }
            expr => Self::Expr {
                root: arena.lower_ast_expr(expr)?,
                name: format!("{:#}", expr),
            },
        })
    }
}

/// A GROUP BY with ALL replaced by positions, and ROLLUP, CUBE and
/// combinations of them expanded into grouping sets.
#[derive(Clone)]
pub(crate) enum ExpandedGroup {
    Normal(Vec<GroupItem>),
    GroupingSets(Vec<Vec<GroupItem>>),
}

/// A lowered GROUP BY. ALL is expanded once the aggregates of the SELECT
/// items are known, see [`Binder::expand_group_by`].
pub(crate) enum LoweredGroupBy {
    All,
    Expanded(ExpandedGroup),
}

impl LoweredGroupBy {
    /// Lower the items of `group_by` of a SELECT with `select_len` items, and
    /// expand it but for a top-level ALL. An item in several grouping sets is
    /// lowered once.
    pub(crate) fn lower<'a>(
        arena: &mut CoreExprArena<'a>,
        group_by: &'a GroupBy,
        select_len: usize,
    ) -> Result<Self> {
        match group_by {
            GroupBy::All => Ok(Self::All),
            group_by => Ok(Self::Expanded(Binder::expand_group(
                arena, group_by, select_len,
            )?)),
        }
    }
}

impl ExpandedGroup {
    /// The folding policy of the keys, and so of the pre-aggregate forms
    /// matched against them, which must fold identically. Only grouping sets
    /// preserve column-dependent structure, as NULLs are injected into their
    /// outputs: `a IS NULL` over a NOT NULL `a` is not constant there.
    pub(crate) fn folding_policy(&self) -> ScalarFoldingPolicy {
        match self {
            ExpandedGroup::Normal(_) => ScalarFoldingPolicy::Normal,
            ExpandedGroup::GroupingSets(_) => ScalarFoldingPolicy::PreserveColumnStructure,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum AggregateCall {
    Aggregate(ScalarItem),
    Udaf(ScalarItem),
}

impl AggregateCall {
    fn item(&self) -> &ScalarItem {
        match self {
            AggregateCall::Aggregate(item) | AggregateCall::Udaf(item) => item,
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct AggregateInfo {
    /// Aggregate calls in ascending column index order.
    /// Column indices are allocated monotonically, so registration order preserves this ordering.
    aggregate_calls: Vec<AggregateCall>,

    /// Arguments of aggregation functions
    aggregate_arguments: Vec<ScalarItem>,

    /// SortDesc of aggregation functions
    aggregate_sort_descs: Vec<ScalarItem>,

    /// Group items of aggregation
    group_items: Vec<ScalarItem>,

    /// Output columns of aggregation, including group items and aggregate functions.
    output_columns: Vec<ColumnBinding>,

    /// Mapping: (group item) -> (index of group item in `group_items`)
    /// This is used to check if a scalar expression is a group item.
    /// For example, `SELECT count(*) FROM t GROUP BY a+1 HAVING a+1+1`.
    /// The group item `a+1` is involved in `a+1+1`, so it's a valid `HAVING`.
    /// We will check the validity by lookup this map with display name.
    /// The Index is the index of the group item in `group_items`, eg [0, 1, 2]
    group_items_map: HashMap<ScalarExpr, usize>,

    /// Information of grouping sets
    grouping_sets: Option<GroupingSetsInfo>,

    /// SELECT items that GROUP BY refers to by position or alias.
    /// Such an item is the key itself.
    select_item_keys: Vec<usize>,
}

impl AggregateInfo {
    pub fn has_group_items(&self) -> bool {
        !self.group_items.is_empty()
    }

    pub fn group_items(&self) -> &[ScalarItem] {
        &self.group_items
    }

    pub fn grouping_sets(&self) -> Option<&GroupingSetsInfo> {
        self.grouping_sets.as_ref()
    }

    pub fn lookup_group_item(&self, expr: &ScalarExpr) -> Option<&ScalarItem> {
        self.group_items_map
            .get(expr)
            .and_then(|index| self.group_items.get(*index))
    }

    pub fn contains_group_item(&self, expr: &ScalarExpr) -> bool {
        self.group_items_map.contains_key(expr)
    }

    pub fn has_group_item_index(&self, index: Symbol) -> bool {
        self.group_items.iter().any(|item| item.index == index)
    }

    /// The output binding of the key whose input expression is exactly `expr`.
    pub fn lookup_grouping_key_output(&self, expr: &ScalarExpr) -> Option<ColumnBinding> {
        let item = self.lookup_group_item(expr)?;
        if let Some(sets) = &self.grouping_sets {
            return (item.index != sets.grouping_id_column.index)
                .then(|| grouping_key_output_binding(item, "group_item", Visibility::Visible));
        }
        Some(if let ScalarExpr::BoundColumnRef(column) = &item.scalar {
            column.column.clone()
        } else {
            ColumnBindingBuilder::new(
                "group_item".to_string(),
                item.index,
                Box::new(item.scalar.data_type().into_owned()),
                Visibility::Visible,
            )
            .build()
        })
    }

    /// Whether `index` is the output symbol of a grouping key.
    pub fn is_grouping_key_output(&self, index: Symbol) -> bool {
        self.has_group_item_index(index)
            && self
                .grouping_sets
                .as_ref()
                .is_none_or(|sets| index != sets.grouping_id_column.index)
    }

    /// SELECT items that GROUP BY refers to by position or alias.
    pub fn select_item_keys(&self) -> &[usize] {
        &self.select_item_keys
    }

    /// Register `scalar` as a key unless it is one. A new key outputs the
    /// column it is, or a new symbol named `name`. Under GROUPING SETS every
    /// key outputs a new symbol: NULLs are injected into the output, while
    /// aggregate arguments still read the input, and no symbol may change type
    /// across the aggregate.
    fn register_group_item(
        &mut self,
        metadata: &MetadataRef,
        scalar: ScalarExpr,
        name: String,
        grouping_sets: bool,
    ) {
        let Entry::Vacant(entry) = self.group_items_map.entry(scalar) else {
            return;
        };
        let scalar = entry.key();
        let index = match scalar {
            ScalarExpr::BoundColumnRef(column) if !grouping_sets => column.column.index,
            _ => {
                let data_type = if grouping_sets {
                    scalar.data_type().wrap_nullable()
                } else {
                    scalar.data_type().into_owned()
                };
                metadata.write().add_derived_column(name, data_type)
            }
        };
        self.group_items.push(ScalarItem {
            scalar: scalar.clone(),
            index,
        });
        entry.insert(self.group_items.len() - 1);
    }

    pub fn replace_group_items(&mut self, group_items: Vec<ScalarItem>) {
        self.group_items = group_items;
        self.group_items_map.clear();
        for (index, item) in self.group_items.iter().enumerate() {
            self.group_items_map.insert(item.scalar.clone(), index);
        }
    }

    pub fn lookup_aggregate_function(&self, aggregate: &AggregateFunction) -> Option<&ScalarItem> {
        let scalar = ScalarExpr::AggregateFunction(aggregate.clone());
        self.aggregate_calls
            .iter()
            .filter_map(|call| match call {
                AggregateCall::Aggregate(item) => Some(item),
                AggregateCall::Udaf(_) => None,
            })
            .find(|item| item.scalar.equivalent(&scalar))
    }

    pub fn lookup_udaf_call(&self, udaf: &UDAFCall) -> Option<&ScalarItem> {
        let scalar = ScalarExpr::UDAFCall(udaf.clone());
        self.aggregate_calls
            .iter()
            .filter_map(|call| match call {
                AggregateCall::Aggregate(_) => None,
                AggregateCall::Udaf(item) => Some(item),
            })
            .find(|item| item.scalar.equivalent(&scalar))
    }

    pub fn lookup_aggregate_function_column(
        &self,
        aggregate: &AggregateFunction,
        new_name: &str,
    ) -> Option<ColumnBinding> {
        self.lookup_aggregate_function(aggregate)
            .map(|scalar_item| {
                debug_assert_eq!(
                    scalar_item.scalar.data_type().as_ref(),
                    aggregate.return_type.as_ref()
                );
                build_replaced_aggregate_column(new_name, scalar_item.index, &aggregate.return_type)
            })
    }

    pub fn lookup_udaf_call_column(
        &self,
        udaf: &UDAFCall,
        new_name: &str,
    ) -> Option<ColumnBinding> {
        self.lookup_udaf_call(udaf).map(|scalar_item| {
            debug_assert_eq!(
                scalar_item.scalar.data_type().as_ref(),
                udaf.return_type.as_ref()
            );
            build_replaced_aggregate_column(new_name, scalar_item.index, &udaf.return_type)
        })
    }

    pub fn has_aggregate_calls(&self) -> bool {
        !self.aggregate_calls.is_empty()
    }

    pub fn aggregate_calls(&self) -> impl Iterator<Item = &ScalarItem> {
        self.aggregate_calls.iter().map(AggregateCall::item)
    }

    pub fn has_aggregate_call_index(&self, index: Symbol) -> bool {
        self.aggregate_calls().any(|item| item.index == index)
    }

    fn push_aggregate_call(&mut self, call: AggregateCall) {
        debug_assert!(
            self.aggregate_calls
                .last()
                .is_none_or(|last| last.item().index < call.item().index),
            "aggregate calls must be registered in column index order"
        );
        self.aggregate_calls.push(call);
    }

    fn lookup_existing_aggregate_function_column(
        &self,
        aggregate: &AggregateFunction,
        new_name: &str,
    ) -> Result<Option<ColumnBinding>> {
        let Some(replaced_args) =
            self.try_replace_function_args(&aggregate.args, &aggregate.func_name)?
        else {
            return Ok(None);
        };
        let Some(replaced_sort_descs) =
            self.try_replace_sort_descs(&aggregate.sort_descs, &aggregate.func_name)?
        else {
            return Ok(None);
        };

        let replaced_agg = AggregateFunction {
            span: aggregate.span,
            display_name: aggregate.display_name.clone(),
            func_name: aggregate.func_name.clone(),
            distinct: aggregate.distinct,
            params: aggregate.params.clone(),
            args: replaced_args,
            return_type: aggregate.return_type.clone(),
            sort_descs: replaced_sort_descs,
        };

        Ok(self.lookup_aggregate_function_column(&replaced_agg, new_name))
    }

    fn lookup_existing_udaf_call_column(
        &self,
        udaf: &UDAFCall,
        new_name: &str,
    ) -> Result<Option<ColumnBinding>> {
        let Some(replaced_args) = self.try_replace_function_args(&udaf.arguments, &udaf.name)?
        else {
            return Ok(None);
        };

        let replaced_udaf = UDAFCall {
            span: udaf.span,
            name: udaf.name.clone(),
            display_name: udaf.display_name.clone(),
            arg_types: udaf.arg_types.clone(),
            state_fields: udaf.state_fields.clone(),
            return_type: udaf.return_type.clone(),
            arguments: replaced_args,
            udf_type: udaf.udf_type.clone(),
        };

        Ok(self.lookup_udaf_call_column(&replaced_udaf, new_name))
    }

    /// Deduplicate, rewrite, and register an aggregate function in `AggregateInfo`.
    fn intern_aggregate_function_column(
        &mut self,
        metadata: &MetadataRef,
        aggregate: &AggregateFunction,
    ) -> Result<ColumnBinding> {
        // `grouping()` reads `_grouping_id`, which exists only after the aggregate.
        reject_grouping_functions(aggregate.exprs(), "Aggregate function arguments")?;
        let replaced_args =
            self.replace_function_args(metadata, &aggregate.args, &aggregate.func_name)?;
        let replaced_sort_desc =
            self.replace_sort_descs(metadata, &aggregate.sort_descs, &aggregate.func_name)?;

        let replaced_agg = AggregateFunction {
            span: aggregate.span,
            display_name: aggregate.display_name.clone(),
            func_name: aggregate.func_name.clone(),
            distinct: aggregate.distinct,
            params: aggregate.params.clone(),
            args: replaced_args,
            return_type: aggregate.return_type.clone(),
            sort_descs: replaced_sort_desc,
        };

        if let Some(column) =
            self.lookup_aggregate_function_column(&replaced_agg, &aggregate.display_name)
        {
            return Ok(column);
        }

        let index = metadata.write().add_derived_column(
            aggregate.display_name.clone(),
            *aggregate.return_type.clone(),
        );

        self.push_aggregate_call(AggregateCall::Aggregate(ScalarItem {
            scalar: replaced_agg.into(),
            index,
        }));

        Ok(build_replaced_aggregate_column(
            &aggregate.display_name,
            index,
            &aggregate.return_type,
        ))
    }

    /// Deduplicate, rewrite, and register a UDAF call in `AggregateInfo`.
    fn intern_udaf_call_column(
        &mut self,
        metadata: &MetadataRef,
        udaf: &UDAFCall,
    ) -> Result<ColumnBinding> {
        reject_grouping_functions(&udaf.arguments, "Aggregate function arguments")?;
        let replaced_args = self.replace_function_args(metadata, &udaf.arguments, &udaf.name)?;

        let replaced_udaf = UDAFCall {
            span: udaf.span,
            name: udaf.name.clone(),
            display_name: udaf.display_name.clone(),
            arg_types: udaf.arg_types.clone(),
            state_fields: udaf.state_fields.clone(),
            return_type: udaf.return_type.clone(),
            arguments: replaced_args,
            udf_type: udaf.udf_type.clone(),
        };

        if let Some(column) = self.lookup_udaf_call_column(&replaced_udaf, &udaf.display_name) {
            return Ok(column);
        }

        let index = metadata
            .write()
            .add_derived_column(udaf.display_name.clone(), *udaf.return_type.clone());

        self.push_aggregate_call(AggregateCall::Udaf(ScalarItem {
            scalar: replaced_udaf.into(),
            index,
        }));

        Ok(build_replaced_aggregate_column(
            &udaf.display_name,
            index,
            &udaf.return_type,
        ))
    }

    /// Group items whose output symbols aggregate arguments and sort keys may
    /// reuse. Grouping-sets keys receive injected NULLs, so aggregates must
    /// keep their own pre-aggregate symbols.
    fn reusable_group_items(&self) -> impl Iterator<Item = &ScalarItem> {
        self.group_items
            .iter()
            .filter(|_| self.grouping_sets.is_none())
    }

    fn replace_sort_descs(
        &mut self,
        metadata: &MetadataRef,
        sort_descs: &[AggregateFunctionScalarSortDesc],
        func_name: &str,
    ) -> Result<Vec<AggregateFunctionScalarSortDesc>> {
        sort_descs
            .iter()
            .enumerate()
            .map(|(i, desc)| {
                let name = format!("{}_sort_desc_{}", func_name, i);
                let expr = &desc.expr;

                let column = if let ScalarExpr::BoundColumnRef(column_ref) = expr {
                    let index = column_ref.column.index;
                    self.aggregate_sort_descs.push(ScalarItem {
                        index,
                        scalar: expr.clone(),
                    });
                    column_ref.clone()
                } else if let Some(item) = self
                    .aggregate_arguments
                    .iter()
                    .chain(self.reusable_group_items())
                    .chain(self.aggregate_sort_descs.iter())
                    .find(|x| x.scalar.equivalent(expr))
                {
                    let column_binding = ColumnBindingBuilder::new(
                        name,
                        item.index,
                        Box::new(expr.data_type().into_owned()),
                        Visibility::Visible,
                    )
                    .build();

                    BoundColumnRef {
                        span: expr.span(),
                        column: column_binding,
                    }
                } else {
                    let data_type = expr.data_type().into_owned();
                    let index = metadata
                        .write()
                        .add_derived_column(name.clone(), data_type.clone());

                    let column_binding = ColumnBindingBuilder::new(
                        name,
                        index,
                        Box::new(data_type),
                        Visibility::Visible,
                    )
                    .build();

                    self.aggregate_sort_descs.push(ScalarItem {
                        index,
                        scalar: expr.clone(),
                    });

                    BoundColumnRef {
                        span: expr.span(),
                        column: column_binding.clone(),
                    }
                };
                Ok(AggregateFunctionScalarSortDesc {
                    expr: column.into(),
                    nulls_first: desc.nulls_first,
                    asc: desc.asc,
                })
            })
            .collect()
    }

    fn try_replace_sort_descs(
        &self,
        sort_descs: &[AggregateFunctionScalarSortDesc],
        func_name: &str,
    ) -> Result<Option<Vec<AggregateFunctionScalarSortDesc>>> {
        let mut replaced_sort_descs = Vec::with_capacity(sort_descs.len());
        for (i, desc) in sort_descs.iter().enumerate() {
            let name = format!("{}_sort_desc_{}", func_name, i);
            let expr = &desc.expr;

            let replaced = if let ScalarExpr::BoundColumnRef(column_ref) = expr {
                Some(column_ref.clone())
            } else if let Some(item) = self
                .aggregate_arguments
                .iter()
                .chain(self.reusable_group_items())
                .chain(self.aggregate_sort_descs.iter())
                .find(|x| x.scalar.equivalent(expr))
                .cloned()
            {
                let column_binding = ColumnBindingBuilder::new(
                    name,
                    item.index,
                    Box::new(expr.data_type().into_owned()),
                    Visibility::Visible,
                )
                .build();

                Some(BoundColumnRef {
                    span: expr.span(),
                    column: column_binding,
                })
            } else {
                None
            };

            let Some(column) = replaced else {
                return Ok(None);
            };

            replaced_sort_descs.push(AggregateFunctionScalarSortDesc {
                expr: column.into(),
                nulls_first: desc.nulls_first,
                asc: desc.asc,
            });
        }

        Ok(Some(replaced_sort_descs))
    }

    fn replace_function_args(
        &mut self,
        metadata: &MetadataRef,
        args: &[ScalarExpr],
        func_name: &str,
    ) -> Result<Vec<ScalarExpr>> {
        args.iter()
            .enumerate()
            .map(|(i, arg)| {
                let name = format!("{}_arg_{}", func_name, i);
                let data_type = arg.data_type().into_owned();
                if let ScalarExpr::BoundColumnRef(column_ref) = arg {
                    self.aggregate_arguments.push(ScalarItem {
                        index: column_ref.column.index,
                        scalar: arg.clone(),
                    });
                    return Ok(column_ref.clone());
                }

                if let Some(item) = self
                    .reusable_group_items()
                    .chain(self.aggregate_arguments.iter())
                    .find(|x| &x.scalar == arg)
                {
                    let column_binding = ColumnBindingBuilder::new(
                        name,
                        item.index,
                        Box::new(data_type),
                        Visibility::Visible,
                    )
                    .build();

                    return Ok(BoundColumnRef {
                        span: arg.span(),
                        column: column_binding,
                    });
                }

                let index = metadata
                    .write()
                    .add_derived_column(name.clone(), data_type.clone());

                let column_binding = ColumnBindingBuilder::new(
                    name,
                    index,
                    Box::new(data_type),
                    Visibility::Visible,
                )
                .build();

                self.aggregate_arguments.push(ScalarItem {
                    index,
                    scalar: arg.clone(),
                });

                Ok(BoundColumnRef {
                    span: arg.span(),
                    column: column_binding.clone(),
                })
            })
            .map(|x| x.map(|x| x.into()))
            .collect()
    }

    fn try_replace_function_args(
        &self,
        args: &[ScalarExpr],
        func_name: &str,
    ) -> Result<Option<Vec<ScalarExpr>>> {
        let mut replaced_args = Vec::with_capacity(args.len());
        for (i, arg) in args.iter().enumerate() {
            let name = format!("{}_arg_{}", func_name, i);
            let data_type = arg.data_type().into_owned();
            if let ScalarExpr::BoundColumnRef(column_ref) = arg {
                replaced_args.push(column_ref.clone().into());
                continue;
            }

            let Some(item) = self
                .reusable_group_items()
                .chain(self.aggregate_arguments.iter())
                .find(|x| &x.scalar == arg)
            else {
                return Ok(None);
            };

            replaced_args.push(
                BoundColumnRef {
                    span: arg.span(),
                    column: ColumnBindingBuilder::new(
                        name,
                        item.index,
                        Box::new(data_type),
                        Visibility::Visible,
                    )
                    .build(),
                }
                .into(),
            );
        }

        Ok(Some(replaced_args))
    }

    /// The error for an argument of `grouping()` that is not a key.
    pub(crate) fn grouping_argument_error(&self) -> ErrorCode {
        if self.grouping_sets.is_none() {
            ErrorCode::SemanticError(
                "grouping can only be called in GROUP BY GROUPING SETS clauses",
            )
        } else {
            ErrorCode::BadArguments("Arguments of grouping should be group by expressions")
        }
    }

    pub(crate) fn replace_grouping(&self, function: &FunctionCall) -> Result<FunctionCall> {
        // `grouping<...>(_grouping_id)` is the internal rewritten form. Alias-expanded
        // QUALIFY expressions can bind to it directly and must not be rewritten again.
        if !function.params.is_empty() {
            return Ok(function.clone());
        }

        if function.arguments.is_empty() {
            return Err(ErrorCode::BadArguments(
                "grouping requires at least one argument",
            ));
        }

        let Some(grouping_sets) = &self.grouping_sets else {
            return Err(self.grouping_argument_error());
        };
        let grouping_id_column = grouping_sets.grouping_id_column.clone();

        let mut replaced_params = Vec::with_capacity(function.arguments.len());
        for arg in &function.arguments {
            // An argument that is a key is bound to its output symbol.
            let position = match arg {
                ScalarExpr::BoundColumnRef(column_ref)
                    if self.is_grouping_key_output(column_ref.column.index) =>
                {
                    self.group_items
                        .iter()
                        .position(|item| item.index == column_ref.column.index)
                }
                _ => None,
            };
            let Some(index) = position else {
                return Err(self.grouping_argument_error());
            };
            replaced_params.push(Scalar::Number(NumberScalar::Int64(index as _)));
        }

        Ok(FunctionCall {
            span: function.span,
            func_name: function.func_name.clone(),
            params: replaced_params,
            arguments: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: function.span,
                column: grouping_id_column,
            })],
            return_type: function.return_type.clone(),
        })
    }
}

pub(crate) struct AggregateRewriter<'a> {
    pub aggregate_info: &'a mut AggregateInfo,
    pub metadata: MetadataRef,
}

impl<'a> AggregateRewriter<'a> {
    pub fn new(aggregate_info: &'a mut AggregateInfo, metadata: MetadataRef) -> Self {
        Self {
            aggregate_info,
            metadata,
        }
    }
}

impl AggregateRewriter<'_> {
    pub fn rewrite_expr(
        aggregate_info: &mut AggregateInfo,
        metadata: MetadataRef,
        expr: &mut ScalarExpr,
    ) -> Result<()> {
        let mut rewriter = AggregateRewriter::new(aggregate_info, metadata);
        rewriter.visit(expr)
    }

    pub fn rewrite_existing_expr(
        aggregate_info: &AggregateInfo,
        expr: &mut ScalarExpr,
        error_message: &str,
    ) -> Result<()> {
        let mut rewriter = ExistingAggregateRewriter {
            aggregate_info,
            error_message,
        };
        rewriter.visit(expr)
    }

    pub fn check_no_aggregate_calls(expr: &ScalarExpr, error_message: &str) -> Result<()> {
        let f = |scalar: &ScalarExpr| scalar.is_aggregate();
        let mut any = Any::new(&f);
        any.visit(expr)?;
        if any.result() {
            return Err(ErrorCode::Internal(error_message.to_string()));
        }

        Ok(())
    }
}

struct ExistingAggregateRewriter<'a> {
    aggregate_info: &'a AggregateInfo,
    error_message: &'a str,
}

impl<'a> VisitorMut<'a> for ExistingAggregateRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::AggregateFunction(aggregate) => {
                let Some(column) = self
                    .aggregate_info
                    .lookup_existing_aggregate_function_column(
                        aggregate,
                        &aggregate.display_name,
                    )?
                else {
                    return Err(ErrorCode::SemanticError(self.error_message.to_string()));
                };
                *expr = BoundColumnRef { span: None, column }.into();
                Ok(())
            }
            ScalarExpr::UDAFCall(udaf) => {
                let Some(column) = self
                    .aggregate_info
                    .lookup_existing_udaf_call_column(udaf, &udaf.display_name)?
                else {
                    return Err(ErrorCode::SemanticError(self.error_message.to_string()));
                };
                *expr = BoundColumnRef { span: None, column }.into();
                Ok(())
            }
            _ => walk_expr_mut(self, expr),
        }
    }

    fn visit_function_call(&mut self, func: &'a mut FunctionCall) -> Result<()> {
        if func.func_name.eq_ignore_ascii_case("grouping") {
            let replaced = self.aggregate_info.replace_grouping(func)?;
            *func = replaced;
            return Ok(());
        }

        for expr in &mut func.arguments {
            self.visit(expr)?;
        }

        Ok(())
    }

    fn visit_window_function(&mut self, _window: &'a mut crate::plans::WindowFunc) -> Result<()> {
        Ok(())
    }

    fn visit_subquery_expr(&mut self, _subquery: &'a mut crate::plans::SubqueryExpr) -> Result<()> {
        Ok(())
    }
}

impl<'a> VisitorMut<'a> for AggregateRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::AggregateFunction(aggregate) => {
                let span = aggregate.span;
                let column = self
                    .aggregate_info
                    .intern_aggregate_function_column(&self.metadata, aggregate)?;
                *expr = BoundColumnRef { span, column }.into();
                Ok(())
            }
            ScalarExpr::UDAFCall(udaf) => {
                let span = udaf.span;
                let column = self
                    .aggregate_info
                    .intern_udaf_call_column(&self.metadata, udaf)?;
                *expr = BoundColumnRef { span, column }.into();
                Ok(())
            }
            _ => walk_expr_mut(self, expr),
        }
    }

    fn visit_function_call(&mut self, func: &'a mut FunctionCall) -> Result<()> {
        if func.func_name.eq_ignore_ascii_case("grouping") {
            *func = self.aggregate_info.replace_grouping(func)?;
            return Ok(());
        }

        for expr in &mut func.arguments {
            self.visit(expr)?;
        }

        Ok(())
    }

    fn visit_window_function(&mut self, _window: &'a mut crate::plans::WindowFunc) -> Result<()> {
        Ok(())
    }

    fn visit_subquery_expr(&mut self, _subquery: &'a mut crate::plans::SubqueryExpr) -> Result<()> {
        // TODO(leiysky): should we recursively process subquery here?
        Ok(())
    }
}

impl Binder {
    /// Check the bound SELECT list, see [`check_post_aggregate_expr`].
    pub(crate) fn analyze_aggregate_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            check_post_aggregate_expr(&bind_context.aggregate_info, &mut item.scalar)?;
        }

        Ok(())
    }

    /// Expand the GROUP BY of a SELECT. ALL stands for the positions of the
    /// SELECT items that contain no aggregate function, see
    /// [`SelectClauseExprs::group_by_all`].
    pub(crate) fn expand_group_by(
        &self,
        clauses: &SelectClauseExprs<'_>,
        udaf_names: &HashSet<String>,
        input_context: &BindContext,
    ) -> Option<ExpandedGroup> {
        match clauses.group_by()? {
            LoweredGroupBy::All => Some(ExpandedGroup::Normal(
                clauses
                    .group_by_all(&self.name_resolution_ctx, udaf_names, input_context)
                    .into_iter()
                    .map(GroupItem::Position)
                    .collect(),
            )),
            LoweredGroupBy::Expanded(group) => Some(group.clone()),
        }
    }

    /// Bind the keys of GROUP BY. An item is one of:
    ///
    ///   - A position, an integer literal, e.g. `GROUP BY 1`: the key is the
    ///     SELECT item at that position.
    ///   - A bare name that no input column has but the explicit alias of
    ///     SELECT items, e.g. `b` in `SELECT a AS b, count(a) FROM t GROUP BY
    ///     b`: the key is those items, as for a position.
    ///   - Any other expression, over input columns, e.g. `a` and `a + 1` in
    ///     `SELECT a AS b, count(a) FROM t GROUP BY a, a + 1`. A name in it is
    ///     an input column first, and otherwise an explicit SELECT alias,
    ///     which stands for the pre-aggregate form of its item.
    ///
    /// The keys are bound with the folding policy of the SELECT items, which
    /// their pre-aggregate forms are bound with as well.
    pub(crate) fn analyze_group_items(
        &mut self,
        bind_context: &mut BindContext,
        mut select_items: SelectItems<'_>,
        group_by_aliases: AliasLookup<'_>,
        group_by: &ExpandedGroup,
    ) -> Result<()> {
        let original_context = bind_context.replace_expr_context(ExprContext::GroupClaue);

        let result = match group_by {
            ExpandedGroup::Normal(items) => self.resolve_group_items(
                bind_context,
                select_items.reborrow(),
                items,
                group_by_aliases,
                false,
                &mut vec![],
            ),
            ExpandedGroup::GroupingSets(sets) => self.resolve_grouping_sets(
                bind_context,
                select_items.reborrow(),
                sets,
                group_by_aliases,
            ),
        };
        bind_context.expr_context = original_context;
        result
    }

    /// Lower and expand a GROUP BY other than a top-level ALL, which only
    /// [`Self::expand_group_by`] can expand.
    fn expand_group<'a>(
        arena: &mut CoreExprArena<'a>,
        group_by: &'a GroupBy,
        select_len: usize,
    ) -> Result<ExpandedGroup> {
        match group_by {
            GroupBy::Normal(exprs) => Ok(ExpandedGroup::Normal(Self::lower_group_items(
                arena, exprs, select_len,
            )?)),
            // A top-level ALL is expanded by the caller, so ALL is only
            // reached in a combined GROUP BY.
            GroupBy::All => Err(ErrorCode::SyntaxException(format!(
                "COMBINED GROUP BY does not support {:?}",
                GroupBy::All
            ))),
            GroupBy::GroupingSets(sets) => Ok(ExpandedGroup::GroupingSets(
                sets.iter()
                    .map(|set| Self::lower_group_items(arena, set, select_len))
                    .collect::<Result<_>>()?,
            )),
            GroupBy::Cube(exprs) => {
                // Expand CUBE to GroupingSets
                Ok(ExpandedGroup::GroupingSets(Self::generate_cube_sets(
                    Self::lower_group_items(arena, exprs, select_len)?,
                )))
            }
            GroupBy::Rollup(exprs) => {
                // Expand ROLLUP to GroupingSets
                Ok(ExpandedGroup::GroupingSets(Self::generate_rollup_sets(
                    Self::lower_group_items(arena, exprs, select_len)?,
                )))
            }
            GroupBy::Combined(groups) => {
                // Flatten and expand all nested GroupBy variants
                let mut combined_sets = Vec::new();
                for group in groups {
                    match Self::expand_group(arena, group, select_len)? {
                        ExpandedGroup::Normal(items) => {
                            combined_sets = Self::cartesian_product(combined_sets, vec![items]);
                        }
                        ExpandedGroup::GroupingSets(sets) => {
                            combined_sets = Self::cartesian_product(combined_sets, sets);
                        }
                    }
                }
                Ok(ExpandedGroup::GroupingSets(combined_sets))
            }
        }
    }

    fn lower_group_items<'a>(
        arena: &mut CoreExprArena<'a>,
        exprs: &'a [Expr],
        select_len: usize,
    ) -> Result<Vec<GroupItem>> {
        exprs
            .iter()
            .map(|expr| GroupItem::lower(arena, expr, select_len))
            .collect()
    }

    /// Generate GroupingSets from CUBE (expr1, expr2, ...)
    fn generate_cube_sets(items: Vec<GroupItem>) -> Vec<Vec<GroupItem>> {
        (0..=items.len())
            .flat_map(|count| items.clone().into_iter().combinations(count))
            .collect::<Vec<_>>()
    }

    /// Generate GroupingSets from ROLLUP (expr1, expr2, ...)
    fn generate_rollup_sets(items: Vec<GroupItem>) -> Vec<Vec<GroupItem>> {
        let mut result = Vec::new();
        for i in (0..=items.len()).rev() {
            result.push(items[..i].to_vec());
        }
        result
    }

    /// Perform Cartesian product of two sets of grouping sets
    fn cartesian_product(
        set1: Vec<Vec<GroupItem>>,
        set2: Vec<Vec<GroupItem>>,
    ) -> Vec<Vec<GroupItem>> {
        if set1.is_empty() {
            return set2;
        }

        if set2.is_empty() {
            return set1;
        }

        let mut result = Vec::new();
        for s1 in set1 {
            for s2 in &set2 {
                let mut combined = s1.clone();
                combined.extend(s2.clone());
                result.push(combined);
            }
        }
        result
    }

    /// Prepare to bind the SELECT list after the keys are known. Before the
    /// aggregate, only the GROUP BY, the WHERE and the SELECT items they refer
    /// to are bound, which either fail on aggregate functions or register
    /// what is evaluated before the aggregate. `input_folding_policy` is the
    /// policy the keys were bound with.
    pub(crate) fn prepare_grouping_scope(
        bind_context: &mut BindContext,
        input_folding_policy: ScalarFoldingPolicy,
    ) {
        bind_context.grouping_key_scope = Some(GroupingKeyScope {
            state: GroupingScopeState::Active,
            input_columns: bind_context.column_set(),
            input_folding_policy,
        });
    }

    pub fn bind_aggregate(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        // Build a ProjectPlan, which will produce aggregate arguments and group items
        let agg_info = &bind_context.aggregate_info;
        let mut scalar_items: Vec<ScalarItem> = Vec::with_capacity(
            agg_info.aggregate_arguments.len()
                + agg_info.aggregate_sort_descs.len()
                + agg_info.group_items.len(),
        );

        for arg in agg_info.aggregate_arguments.iter() {
            scalar_items.push(arg.clone());
        }

        for sort_desc_expr in agg_info.aggregate_sort_descs.iter() {
            scalar_items.push(sort_desc_expr.clone());
        }

        let grouping_id_index = agg_info
            .grouping_sets
            .as_ref()
            .map(|g| g.grouping_id_column.index);
        let mut group_items = Vec::with_capacity(agg_info.group_items.len());
        for item in agg_info.group_items.iter() {
            if grouping_id_index.is_some_and(|idx| is_grouping_id_item(item, idx)) {
                group_items.push(item.clone());
                continue;
            }
            if grouping_id_index.is_none() {
                scalar_items.push(item.clone());
                group_items.push(item.clone());
                continue;
            }

            // A grouping-sets key outputs its own nullable symbol, computed from
            // the pre-aggregate expression below the aggregate.
            let output = grouping_key_output_binding(item, "group_item", Visibility::Visible);
            let input = if item.scalar.data_type().is_nullable_or_null() {
                item.scalar.clone()
            } else {
                ScalarExpr::CastExpr(CastExpr {
                    span: item.scalar.span(),
                    is_try: false,
                    argument: Box::new(item.scalar.clone()),
                    target_type: output.data_type.clone(),
                })
            };
            scalar_items.push(ScalarItem {
                index: item.index,
                scalar: input,
            });
            group_items.push(ScalarItem {
                index: item.index,
                scalar: BoundColumnRef {
                    span: item.scalar.span(),
                    column: output,
                }
                .into(),
            });
        }

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            scalar_items.sort_by_key(|item| item.index);
            scalar_items.dedup_by_key(|item| item.index);
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(new_expr));
        }

        let aggregate_plan = Aggregate {
            group_items,
            aggregate_functions: agg_info.aggregate_calls().cloned().collect(),
            from_distinct: false,
            rank_limit: None,

            grouping_sets: agg_info.grouping_sets.as_ref().map(|g| GroupingSets {
                grouping_id_index: g.grouping_id_column.index,
                sets: g.sets.clone(),
            }),
        };
        new_expr = SExpr::create_unary(Arc::new(aggregate_plan.into()), Arc::new(new_expr));

        Ok(new_expr)
    }

    fn resolve_grouping_sets(
        &mut self,
        bind_context: &mut BindContext,
        mut select_items: SelectItems<'_>,
        sets: &[Vec<GroupItem>],
        group_by_aliases: AliasLookup<'_>,
    ) -> Result<()> {
        let mut grouping_sets = Vec::with_capacity(sets.len());
        for set in sets {
            self.resolve_group_items(
                bind_context,
                select_items.reborrow(),
                set,
                group_by_aliases,
                true,
                &mut grouping_sets,
            )?;
        }
        let agg_info = &mut bind_context.aggregate_info;
        if agg_info.group_items.len() > MAX_GROUPING_KEYS {
            return Err(ErrorCode::SemanticError(format!(
                "GROUPING SETS, ROLLUP and CUBE support at most {MAX_GROUPING_KEYS} distinct grouping expressions, got {}",
                agg_info.group_items.len()
            )));
        }
        // `grouping_sets` stores formatted `ScalarExpr` for each grouping set.
        let grouping_sets = grouping_sets
            .into_iter()
            .map(|set| {
                let mut set = set
                    .into_iter()
                    .map(|s| {
                        let offset = *agg_info.group_items_map.get(&s).unwrap();
                        agg_info.group_items[offset].index
                    })
                    .collect::<Vec<_>>();
                // Grouping sets with the same items should be treated as the same.
                set.sort();
                set
            })
            .collect::<Vec<_>>();

        // Because we are not using union all to implement grouping sets
        // We will remove the duplicated grouping sets here.
        // For example: SELECT  brand, segment,  SUM (quantity) FROM     sales GROUP BY  GROUPING sets(brand, segment),  GROUPING sets(brand, segment);
        // brand X segment will not appear twice in the result, the results are not standard but acceptable.
        let grouping_sets = grouping_sets.into_iter().unique().collect();
        // Add a virtual column `_grouping_id` to group items.
        let grouping_id_column = self.create_derived_column_binding(
            GROUPING_ID_COLUMN_NAME.to_string(),
            DataType::Number(NumberDataType::UInt32),
        );

        let bound_grouping_id_col = BoundColumnRef {
            span: None,
            column: grouping_id_column.clone(),
        };

        agg_info.group_items_map.insert(
            bound_grouping_id_col.clone().into(),
            agg_info.group_items.len(),
        );
        agg_info.group_items.push(ScalarItem {
            index: grouping_id_column.index,
            scalar: bound_grouping_id_col.into(),
        });

        let grouping_sets_info = GroupingSetsInfo {
            grouping_id_column,
            sets: grouping_sets,
        };

        agg_info.grouping_sets = Some(grouping_sets_info);

        Ok(())
    }

    fn resolve_group_items(
        &mut self,
        bind_context: &mut BindContext,
        mut select_items: SelectItems<'_>,
        group_by: &[GroupItem],
        group_by_aliases: AliasLookup<'_>,
        collect_grouping_sets: bool,
        grouping_sets: &mut Vec<Vec<ScalarExpr>>,
    ) -> Result<()> {
        if collect_grouping_sets {
            grouping_sets.push(Vec::with_capacity(group_by.len()));
        }
        let arena = select_items.exprs().arena();
        for item in group_by.iter() {
            // An item that refers to a SELECT item, by position or by alias,
            // is that item, which is a key itself.
            let positions = match item {
                GroupItem::Position(position) => Ok((None, vec![*position])),
                GroupItem::Expr { root, name } => self
                    .group_item_alias_positions(bind_context, arena, *root, group_by_aliases)
                    .ok_or((*root, name)),
            };
            let (scalar, name, select_item_positions) = match positions {
                Ok((span, positions)) => {
                    let scalar = self.resolve_select_item_key(
                        bind_context,
                        select_items.reborrow(),
                        span,
                        &positions,
                    )?;
                    let alias = select_items.exprs().alias(positions[0]).to_string();
                    (scalar, alias, positions)
                }
                Err((root, name)) => {
                    let (mut scalar, _) = {
                        let mut type_checker = TypeChecker::try_create_with_aliases(
                            bind_context,
                            self.ctx.clone(),
                            &self.name_resolution_ctx,
                            self.metadata.clone(),
                            group_by_aliases,
                            false,
                        )?;
                        type_checker.set_folding_policy(select_items.folding_policy());
                        type_checker.set_select_items(select_items.reborrow());
                        *type_checker.resolve_checked_core(arena, root)?
                    };
                    let mut analyzer =
                        SetReturningAnalyzer::new(bind_context, self.metadata.clone());
                    analyzer.visit(&mut scalar)?;
                    (scalar, name.clone(), vec![])
                }
            };

            let agg_info = &mut bind_context.aggregate_info;
            for position in select_item_positions {
                if !agg_info.select_item_keys.contains(&position) {
                    agg_info.select_item_keys.push(position);
                }
            }
            if collect_grouping_sets && !grouping_sets.last().unwrap().contains(&scalar) {
                grouping_sets.last_mut().unwrap().push(scalar.clone());
            }
            agg_info.register_group_item(&self.metadata, scalar, name, collect_grouping_sets);
        }

        // Check group by contains aggregate functions or not
        let f = |scalar: &ScalarExpr| {
            scalar.is_aggregate() || matches!(scalar, ScalarExpr::WindowFunction(_))
        };

        reject_grouping_functions(
            bind_context
                .aggregate_info
                .group_items
                .iter()
                .map(|item| &item.scalar),
            "GROUP BY items",
        )?;

        for item in bind_context.aggregate_info.group_items.iter() {
            let mut finder = Finder::new(&f);
            finder.visit(&item.scalar)?;
            if let Some(scalar) = finder.scalars().first().copied() {
                let display_name = match scalar {
                    ScalarExpr::AggregateFunction(agg) => agg.display_name.clone(),
                    ScalarExpr::UDAFCall(udaf) => udaf.display_name.clone(),
                    ScalarExpr::WindowFunction(window) => window.display_name.clone(),
                    _ => format!("{scalar:?}"),
                };
                return Err(ErrorCode::SemanticError(format!(
                    "GROUP BY items can't contain aggregate functions or window functions: {}",
                    display_name
                ))
                .set_span(scalar.span()));
            }
        }

        // If it's `GROUP BY GROUPING SETS`, ignore the optimization below.
        if collect_grouping_sets {
            return Ok(());
        }

        // Remove dependent group items, group by a, f(a, b), f(a), b ---> group by a,b
        let mut results = vec![];
        let mut other_group_items = bind_context
            .aggregate_info
            .group_items
            .iter()
            .map(|item| item.scalar.clone())
            .collect::<HashSet<_>>();
        for item in bind_context.aggregate_info.group_items.iter() {
            other_group_items.remove(&item.scalar);
            if prune_by_children(&item.scalar, &other_group_items) {
                other_group_items.insert(item.scalar.clone());
                continue;
            }
            results.push(item.clone());
            other_group_items.insert(item.scalar.clone());
        }

        bind_context.aggregate_info.group_items_map.clear();
        for (i, item) in results.iter().enumerate() {
            bind_context
                .aggregate_info
                .group_items_map
                .insert(item.scalar.clone(), i);
        }
        bind_context.aggregate_info.group_items = results;

        Ok(())
    }

    /// The SELECT items that a GROUP BY item names, and the span of the
    /// name, if the item is a bare name that no input column has but the
    /// explicit alias of SELECT items.
    fn group_item_alias_positions(
        &self,
        bind_context: &BindContext,
        arena: &CoreExprArena<'_>,
        root: CoreExprId,
        aliases: AliasLookup<'_>,
    ) -> Option<(Span, Vec<usize>)> {
        let name = normalize_identifier(arena.bare_name(root)?, &self.name_resolution_ctx);
        // Other results, errors included, are left to the type checker.
        match bind_context.resolve_name(None, None, &name, aliases, &self.name_resolution_ctx) {
            Ok(NameResolutionResult::SelectItems { positions, .. }) => Some((name.span, positions)),
            _ => None,
        }
    }

    /// The key that the SELECT items at `positions` are: their pre-aggregate
    /// form, which must be the same for all of them. Several items share an
    /// alias, e.g. `SELECT a AS x, a AS x ... GROUP BY x`.
    fn resolve_select_item_key(
        &self,
        bind_context: &mut BindContext,
        mut select_items: SelectItems<'_>,
        span: Span,
        positions: &[usize],
    ) -> Result<ScalarExpr> {
        let mut key: Option<ScalarExpr> = None;
        for position in positions {
            let form =
                self.resolve_pre_aggregate_form(bind_context, select_items.reborrow(), *position)?;
            match &key {
                Some(key) if key != &form => {
                    return Err(ErrorCode::SemanticError(format!(
                        "column {} reference or alias is ambiguous, please use another alias name",
                        select_items.exprs().alias(*position)
                    ))
                    .set_span(span));
                }
                Some(_) => {}
                None => key = Some(form),
            }
        }
        key.ok_or_else(|| ErrorCode::Internal("a GROUP BY alias names no SELECT item"))
    }

    fn resolve_pre_aggregate_form(
        &self,
        bind_context: &mut BindContext,
        items: SelectItems<'_>,
        index: usize,
    ) -> Result<ScalarExpr> {
        TypeChecker::resolve_pre_aggregate_form(
            bind_context,
            &FullTypeCheckAdapter::new(self.ctx.clone())?,
            &self.name_resolution_ctx,
            &self.metadata,
            items,
            index,
        )?
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "SELECT item {} has no pre-aggregate form before the aggregate",
                index + 1
            ))
        })
    }

    /// Bind an expression of a clause after the aggregate. In a grouped
    /// SELECT, the type checker rewrites the aggregate functions and
    /// `grouping()` in it to the outputs of the aggregate as it binds them,
    /// see `TypeChecker::rewrite_grouped_output`.
    pub(super) fn bind_post_aggregate_expr(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        select_items: Option<SelectItems<'_>>,
        expr_context: ExprContext,
        arena: &CoreExprArena<'_>,
        root: CoreExprId,
    ) -> Result<ScalarExpr> {
        let original_context = bind_context.replace_expr_context(expr_context);
        let result = (|| {
            let mut scalar_binder = ScalarBinder::new(
                bind_context,
                self.ctx.clone(),
                &self.name_resolution_ctx,
                self.metadata.clone(),
                aliases,
            );
            let (mut result, _) = scalar_binder.bind_lowered(arena, root, select_items)?;
            check_post_aggregate_expr(&bind_context.aggregate_info, &mut result)?;
            Ok(result)
        })();
        bind_context.expr_context = original_context;
        result
    }
}

/// Check an expression bound after the aggregate. The type checker rewrites
/// the aggregate functions and `grouping()` of a grouped SELECT as it binds
/// them, so no aggregate function is left to register, and `grouping()`
/// outside a grouped SELECT is rejected.
pub(crate) fn check_post_aggregate_expr(
    aggregate_info: &AggregateInfo,
    expr: &mut ScalarExpr,
) -> Result<()> {
    AggregateRewriter::rewrite_existing_expr(
        aggregate_info,
        expr,
        "aggregate functions must be registered while they are bound",
    )
}

/// Grouping-sets keys output a dedicated nullable symbol. It only identifies
/// the aggregate output, so it carries no table or virtual-column information
/// of the input it was computed from.
fn grouping_key_output_binding(
    item: &ScalarItem,
    fallback_name: &str,
    visibility: Visibility,
) -> ColumnBinding {
    let name = match &item.scalar {
        ScalarExpr::BoundColumnRef(column_ref) => column_ref.column.column_name.clone(),
        _ => fallback_name.to_string(),
    };
    ColumnBindingBuilder::new(
        name,
        item.index,
        Box::new(item.scalar.data_type().wrap_nullable()),
        visibility,
    )
    .build()
}

fn build_replaced_aggregate_column(
    new_name: &str,
    index: Symbol,
    return_type: &DataType,
) -> ColumnBinding {
    ColumnBindingBuilder::new(
        new_name.to_string(),
        index,
        Box::new(return_type.clone()),
        Visibility::Visible,
    )
    .build()
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;
    use crate::plans::ConstantExpr;
    use crate::plans::UDFType;

    fn test_column(name: &str, index: Symbol) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                name.to_string(),
                index,
                Box::new(DataType::Number(NumberDataType::Int64)),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    fn add_one(expr: ScalarExpr) -> ScalarExpr {
        FunctionCall {
            span: None,
            func_name: "plus".to_string(),
            params: vec![],
            arguments: vec![
                expr,
                ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int64(1)),
                }
                .into(),
            ],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
        }
        .into()
    }

    #[test]
    fn aggregate_lookup_reuses_original_and_rewritten_forms() {
        let original = AggregateFunction {
            span: None,
            display_name: "sum(a + 1)".to_string(),
            func_name: "sum".to_string(),
            distinct: false,
            params: vec![],
            args: vec![add_one(test_column("a", Symbol::new(0)))],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
            sort_descs: vec![],
        };
        let replaced = AggregateFunction {
            args: vec![test_column("sum_arg_0", Symbol::new(1))],
            ..original.clone()
        };

        let mut agg_info = AggregateInfo::default();
        agg_info.push_aggregate_call(AggregateCall::Aggregate(ScalarItem {
            scalar: replaced.clone().into(),
            index: Symbol::new(42),
        }));

        assert!(agg_info.lookup_aggregate_function(&original).is_none());
        assert_eq!(
            agg_info.lookup_aggregate_function(&replaced).unwrap().index,
            Symbol::new(42)
        );
    }

    #[test]
    fn udaf_lookup_reuses_original_and_rewritten_forms() {
        let original = UDAFCall {
            span: None,
            name: "my_udaf".to_string(),
            display_name: "my_udaf(a + 1)".to_string(),
            arg_types: vec![DataType::Number(NumberDataType::Int64)],
            state_fields: vec![],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
            arguments: vec![add_one(test_column("a", Symbol::new(0)))],
            udf_type: UDFType::Server("http://127.0.0.1:9000".to_string()),
        };
        let replaced = UDAFCall {
            arguments: vec![test_column("my_udaf_arg_0", Symbol::new(1))],
            ..original.clone()
        };

        let mut agg_info = AggregateInfo::default();
        agg_info.push_aggregate_call(AggregateCall::Udaf(ScalarItem {
            scalar: replaced.clone().into(),
            index: Symbol::new(84),
        }));

        assert!(agg_info.lookup_udaf_call(&original).is_none());
        assert_eq!(
            agg_info.lookup_udaf_call(&replaced).unwrap().index,
            Symbol::new(84)
        );
    }

    #[test]
    fn rewrite_existing_expr_reuses_registered_aggregate() {
        let replaced = AggregateFunction {
            span: None,
            display_name: "sum(a + 1)".to_string(),
            func_name: "sum".to_string(),
            distinct: false,
            params: vec![],
            args: vec![test_column("sum_arg_0", Symbol::new(1))],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
            sort_descs: vec![],
        };

        let mut agg_info = AggregateInfo::default();
        agg_info.push_aggregate_call(AggregateCall::Aggregate(ScalarItem {
            scalar: replaced.clone().into(),
            index: Symbol::new(42),
        }));

        let mut expr: ScalarExpr = replaced.into();
        AggregateRewriter::rewrite_existing_expr(
            &agg_info,
            &mut expr,
            "unexpected aggregate registration",
        )
        .unwrap();

        assert!(matches!(
            expr,
            ScalarExpr::BoundColumnRef(BoundColumnRef {
                column: ColumnBinding { index, .. },
                ..
            }) if index == Symbol::new(42)
        ));
    }

    #[test]
    fn rewrite_existing_expr_rejects_new_aggregate_registration() {
        let mut expr: ScalarExpr = AggregateFunction {
            span: None,
            display_name: "sum(a)".to_string(),
            func_name: "sum".to_string(),
            distinct: false,
            params: vec![],
            args: vec![test_column("a", Symbol::new(0))],
            return_type: Box::new(DataType::Number(NumberDataType::Int64)),
            sort_descs: vec![],
        }
        .into();

        let agg_info = AggregateInfo::default();
        let err =
            AggregateRewriter::rewrite_existing_expr(&agg_info, &mut expr, "reject new aggregate")
                .unwrap_err();

        assert_eq!(err.message(), "reject new aggregate");
    }
}
