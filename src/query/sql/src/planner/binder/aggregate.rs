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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::SelectTarget;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use indexmap::Equivalent;
use itertools::Itertools;

use super::prune_by_children;
use super::ExprContext;
use super::Finder;
use crate::binder::project_set::SetReturningAnalyzer;
use crate::binder::scalar::ScalarBinder;
use crate::binder::select::SelectList;
use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::GroupingSets;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UDAFCall;
use crate::plans::Visitor;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::IndexType;
use crate::MetadataRef;

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
    /// Each grouping set is a list of column indices in `group_items`.
    pub sets: Vec<Vec<IndexType>>,
    /// The indices generated to identify the duplicate group items in the execution of the `GROUPING SETS` plan (not including `_grouping_id`).
    ///
    /// If the aggregation function argument is an item in the grouping set, for example:
    ///
    /// ```sql
    /// SELECT min(a) FROM t GROUP BY GROUPING SETS (a, ...);
    /// ```
    ///
    /// we should use the original column `a` data instead of the column data after filling dummy NULLs.
    pub dup_group_items: Vec<(IndexType, DataType)>,
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct AggregateInfo {
    /// Aggregation functions
    pub aggregate_functions: Vec<ScalarItem>,

    /// Arguments of aggregation functions
    pub aggregate_arguments: Vec<ScalarItem>,

    /// SortDesc of aggregation functions
    pub aggregate_sort_descs: Vec<ScalarItem>,

    /// Group items of aggregation
    pub group_items: Vec<ScalarItem>,

    /// Output columns of aggregation, including group items and aggregate functions.
    pub output_columns: Vec<ColumnBinding>,

    /// Mapping: (aggregate function display name) -> (index of agg func in `aggregate_functions`)
    /// This is used to find a aggregate function in current context.
    aggregate_functions_map: HashMap<String, usize>,

    /// Mapping: (group item) -> (index of group item in `group_items`)
    /// This is used to check if a scalar expression is a group item.
    /// For example, `SELECT count(*) FROM t GROUP BY a+1 HAVING a+1+1`.
    /// The group item `a+1` is involved in `a+1+1`, so it's a valid `HAVING`.
    /// We will check the validity by lookup this map with display name.
    /// The Index is the index of the group item in `group_items`, eg [0, 1, 2]
    pub group_items_map: HashMap<ScalarExpr, usize>,

    /// Information of grouping sets
    pub grouping_sets: Option<GroupingSetsInfo>,
}

impl AggregateInfo {
    fn push_aggregate_function(&mut self, item: ScalarItem, display_name: String) {
        self.aggregate_functions.push(item);
        self.aggregate_functions_map
            .insert(display_name, self.aggregate_functions.len() - 1);
    }

    pub fn get_aggregate_function(&self, display_name: &str) -> Option<&ScalarItem> {
        self.aggregate_functions_map
            .get(display_name)
            .map(|index| &self.aggregate_functions[*index])
    }
}

pub(super) struct AggregateRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
}

impl<'a> AggregateRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    /// Replace the arguments of aggregate function with a BoundColumnRef, and
    /// add the replaced aggregate function and the arguments into `AggregateInfo`.
    fn replace_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<ScalarExpr> {
        if let Some(column) = find_replaced_aggregate_function(
            &self.bind_context.aggregate_info,
            &aggregate.display_name,
            &aggregate.return_type,
            &aggregate.display_name,
        ) {
            return Ok(BoundColumnRef { span: None, column }.into());
        }

        let replaced_args = self.replace_function_args(&aggregate.args, &aggregate.func_name)?;
        let replaced_sort_desc =
            self.replace_sort_descs(&aggregate.sort_descs, &aggregate.func_name)?;

        let index = self.metadata.write().add_derived_column(
            aggregate.display_name.clone(),
            *aggregate.return_type.clone(),
            Some(ScalarExpr::AggregateFunction(aggregate.clone())),
        );

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

        self.bind_context.aggregate_info.push_aggregate_function(
            ScalarItem {
                scalar: replaced_agg.clone().into(),
                index,
            },
            replaced_agg.display_name.clone(),
        );

        Ok(replaced_agg.into())
    }

    fn replace_udaf_call(&mut self, udaf: &UDAFCall) -> Result<ScalarExpr> {
        if let Some(column) = find_replaced_aggregate_function(
            &self.bind_context.aggregate_info,
            &udaf.display_name,
            &udaf.return_type,
            &udaf.display_name,
        ) {
            return Ok(BoundColumnRef { span: None, column }.into());
        }

        let replaced_args = self.replace_function_args(&udaf.arguments, &udaf.name)?;

        let index = self.metadata.write().add_derived_column(
            udaf.display_name.clone(),
            *udaf.return_type.clone(),
            Some(ScalarExpr::UDAFCall(udaf.clone())),
        );

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

        self.bind_context.aggregate_info.push_aggregate_function(
            ScalarItem {
                scalar: replaced_udaf.clone().into(),
                index,
            },
            replaced_udaf.display_name.clone(),
        );

        Ok(replaced_udaf.into())
    }

    fn replace_sort_descs(
        &mut self,
        sort_descs: &[AggregateFunctionScalarSortDesc],
        func_name: &str,
    ) -> Result<Vec<AggregateFunctionScalarSortDesc>> {
        let AggregateInfo {
            ref mut aggregate_sort_descs,
            ref aggregate_arguments,
            ref group_items,
            ..
        } = self.bind_context.aggregate_info;

        sort_descs
            .iter()
            .enumerate()
            .map(|(i, desc)| {
                let name = format!("{}_sort_desc_{}", func_name, i);
                let expr = &desc.expr;

                let (is_reuse_index, column) = if let ScalarExpr::BoundColumnRef(column_ref) = expr
                {
                    let index = column_ref.column.index;
                    // the columns required for sort describe always come after aggregate_sort_descs
                    aggregate_sort_descs.push(ScalarItem {
                        index,
                        scalar: expr.clone(),
                    });
                    (true, column_ref.clone())
                } else if let Some(item) = aggregate_arguments
                    .iter()
                    .chain(group_items.iter())
                    .chain(aggregate_sort_descs.iter())
                    .find(|x| x.scalar.equivalent(expr))
                {
                    // check if the arg is in aggregate_sort_descs items
                    // we can reuse the index
                    let column_binding = ColumnBindingBuilder::new(
                        name,
                        item.index,
                        Box::new(expr.data_type()?),
                        Visibility::Visible,
                    )
                    .build();

                    (true, BoundColumnRef {
                        span: expr.span(),
                        column: column_binding,
                    })
                } else {
                    let data_type = expr.data_type()?;
                    let index = self.metadata.write().add_derived_column(
                        name.clone(),
                        data_type.clone(),
                        Some(expr.clone()),
                    );

                    // Generate a ColumnBinding for each sort describe of aggregates
                    let column_binding = ColumnBindingBuilder::new(
                        name,
                        index,
                        Box::new(data_type),
                        Visibility::Visible,
                    )
                    .build();

                    aggregate_sort_descs.push(ScalarItem {
                        index,
                        scalar: expr.clone(),
                    });

                    (false, BoundColumnRef {
                        span: expr.span(),
                        column: column_binding.clone(),
                    })
                };
                Ok(AggregateFunctionScalarSortDesc {
                    expr: column.into(),
                    is_reuse_index,
                    nulls_first: desc.nulls_first,
                    asc: desc.asc,
                })
            })
            .collect()
    }

    fn replace_function_args(
        &mut self,
        args: &[ScalarExpr],
        func_name: &str,
    ) -> Result<Vec<ScalarExpr>> {
        let AggregateInfo {
            ref mut aggregate_arguments,
            ref group_items,
            ..
        } = self.bind_context.aggregate_info;

        args.iter()
            .enumerate()
            .map(|(i, arg)| {
                let name = format!("{}_arg_{}", func_name, i);
                let data_type = arg.data_type()?;
                if let ScalarExpr::BoundColumnRef(column_ref) = arg {
                    aggregate_arguments.push(ScalarItem {
                        index: column_ref.column.index,
                        scalar: arg.clone(),
                    });
                    return Ok(column_ref.clone());
                }

                if let Some(item) = group_items
                    .iter()
                    .chain(aggregate_arguments.iter())
                    .find(|x| &x.scalar == arg)
                {
                    // check if the arg is in group items
                    // we can reuse the index
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

                let index = self.metadata.write().add_derived_column(
                    name.clone(),
                    data_type.clone(),
                    Some(arg.clone()),
                );

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBindingBuilder::new(
                    name,
                    index,
                    Box::new(data_type),
                    Visibility::Visible,
                )
                .build();

                aggregate_arguments.push(ScalarItem {
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

    fn replace_grouping(&mut self, function: &FunctionCall) -> Result<FunctionCall> {
        let agg_info = &mut self.bind_context.aggregate_info;
        if agg_info.grouping_sets.is_none() {
            return Err(ErrorCode::SemanticError(
                "grouping can only be called in GROUP BY GROUPING SETS clauses",
            ));
        }
        let grouping_id_column = agg_info
            .grouping_sets
            .as_ref()
            .unwrap()
            .grouping_id_column
            .clone();
        // Rewrite the args to params.
        // The params are the index offset in `grouping_id`.
        // Here is an example:
        // If the query is `select grouping(b, a) from group by grouping sets ((a, b), (a));`
        // The group-by items are: [a, b].
        // The group ids will be (a: 0, b: 1):
        // ba -> 00 -> 0
        // _a -> 01 -> 1
        // grouping(b, a) will be rewritten to grouping<1, 0>(grouping_id).
        let mut replaced_params = Vec::with_capacity(function.arguments.len());
        for arg in &function.arguments {
            if let Some(index) = agg_info.group_items_map.get(arg) {
                replaced_params.push(Scalar::Number(NumberScalar::Int64(*index as _)));
            } else {
                return Err(ErrorCode::BadArguments(
                    "Arguments of grouping should be group by expressions",
                ));
            }
        }

        let replaced_func = FunctionCall {
            span: function.span,
            func_name: function.func_name.clone(),
            params: replaced_params,
            arguments: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: function.span,
                column: grouping_id_column,
            })],
        };

        Ok(replaced_func)
    }
}

impl<'a> VisitorMut<'a> for AggregateRewriter<'a> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        match expr {
            ScalarExpr::AggregateFunction(aggregate) => {
                *expr = self.replace_aggregate_function(aggregate)?;
                Ok(())
            }
            ScalarExpr::UDAFCall(udaf) => {
                *expr = self.replace_udaf_call(udaf)?;
                Ok(())
            }
            _ => walk_expr_mut(self, expr),
        }
    }

    fn visit_function_call(&mut self, func: &'a mut FunctionCall) -> Result<()> {
        if func.func_name.eq_ignore_ascii_case("grouping") {
            *func = self.replace_grouping(func)?;
            return Ok(());
        }

        for expr in &mut func.arguments {
            self.visit(expr)?;
        }

        Ok(())
    }

    fn visit_subquery_expr(&mut self, _subquery: &'a mut crate::plans::SubqueryExpr) -> Result<()> {
        // TODO(leiysky): should we recursively process subquery here?
        Ok(())
    }
}

impl Binder {
    /// Analyze aggregates in select clause, this will rewrite aggregate functions.
    /// See [`AggregateRewriter`] for more details.
    pub(crate) fn analyze_aggregate_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
        for item in select_list.items.iter_mut() {
            rewriter.visit(&mut item.scalar)?;
        }

        Ok(())
    }

    /// We have supported three kinds of `group by` items:
    ///
    ///   - Index, a integral literal, e.g. `GROUP BY 1`. It choose the 1st item in select as
    ///     group item.
    ///   - Alias, the aliased expressions specified in `SELECT` clause, e.g. column `b` in
    ///     `SELECT a as b, COUNT(a) FROM t GROUP BY b`.
    ///   - Scalar expressions that can be evaluated in current scope(doesn't contain aliases), e.g.
    ///     column `a` and expression `a+1` in `SELECT a as b, COUNT(a) FROM t GROUP BY a, a+1`.
    pub fn analyze_group_items(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'_>,
        group_by: &GroupBy,
    ) -> Result<()> {
        let mut available_aliases = vec![];

        // Extract available aliases from `SELECT` clause,
        for item in select_list.items.iter() {
            if let SelectTarget::AliasedExpr {
                alias: Some(alias), ..
            } = item.select_target
            {
                let column = normalize_identifier(alias, &self.name_resolution_ctx);
                available_aliases.push((column.name, item.scalar.clone()));
            }
        }

        let original_context = bind_context.expr_context.clone();
        bind_context.set_expr_context(ExprContext::GroupClaue);

        let group_by = Self::expand_group(group_by.clone())?;
        match &group_by {
            GroupBy::Normal(exprs) => self.resolve_group_items(
                bind_context,
                select_list,
                exprs,
                &available_aliases,
                false,
                &mut vec![],
            )?,
            GroupBy::All => {
                let groups = self.resolve_group_all(select_list)?;
                self.resolve_group_items(
                    bind_context,
                    select_list,
                    &groups,
                    &available_aliases,
                    false,
                    &mut vec![],
                )?;
            }
            GroupBy::GroupingSets(sets) => {
                self.resolve_grouping_sets(bind_context, select_list, sets, &available_aliases)?;
            }
            _ => unreachable!(),
        }
        bind_context.set_expr_context(original_context);
        Ok(())
    }

    pub fn expand_group(group_by: GroupBy) -> Result<GroupBy> {
        match group_by {
            GroupBy::Normal(_) | GroupBy::All | GroupBy::GroupingSets(_) => Ok(group_by),
            GroupBy::Cube(exprs) => {
                // Expand CUBE to GroupingSets
                let sets = Self::generate_cube_sets(exprs);
                Ok(GroupBy::GroupingSets(sets))
            }
            GroupBy::Rollup(exprs) => {
                // Expand ROLLUP to GroupingSets
                let sets = Self::generate_rollup_sets(exprs);
                Ok(GroupBy::GroupingSets(sets))
            }
            GroupBy::Combined(groups) => {
                // Flatten and expand all nested GroupBy variants
                let mut combined_sets = Vec::new();
                for group in groups {
                    match Self::expand_group(group)? {
                        GroupBy::Normal(exprs) => {
                            combined_sets = Self::cartesian_product(combined_sets, vec![exprs]);
                        }
                        GroupBy::GroupingSets(sets) => {
                            combined_sets = Self::cartesian_product(combined_sets, sets);
                        }
                        other => {
                            return Err(ErrorCode::SyntaxException(format!(
                                "COMBINED GROUP BY does not support {other:?}"
                            )));
                        }
                    }
                }
                Ok(GroupBy::GroupingSets(combined_sets))
            }
        }
    }

    /// Generate GroupingSets from CUBE (expr1, expr2, ...)
    fn generate_cube_sets(exprs: Vec<Expr>) -> Vec<Vec<Expr>> {
        (0..=exprs.len())
            .flat_map(|count| exprs.clone().into_iter().combinations(count))
            .collect::<Vec<_>>()
    }

    /// Generate GroupingSets from ROLLUP (expr1, expr2, ...)
    fn generate_rollup_sets(exprs: Vec<Expr>) -> Vec<Vec<Expr>> {
        let mut result = Vec::new();
        for i in (0..=exprs.len()).rev() {
            result.push(exprs[..i].to_vec());
        }
        result
    }

    /// Perform Cartesian product of two sets of grouping sets
    fn cartesian_product(set1: Vec<Vec<Expr>>, set2: Vec<Vec<Expr>>) -> Vec<Vec<Expr>> {
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

    pub fn bind_aggregate(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        // Enter in_grouping state
        bind_context.in_grouping = true;

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

        for item in agg_info.group_items.iter() {
            if let ScalarExpr::BoundColumnRef(col) = &item.scalar {
                if col.column.column_name.eq("_grouping_id") {
                    continue;
                }
            }
            scalar_items.push(item.clone());
        }

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            scalar_items.sort_by_key(|item| item.index);
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(new_expr));
        }

        let aggregate_plan = Aggregate {
            mode: AggregateMode::Initial,
            group_items: agg_info.group_items.clone(),
            aggregate_functions: agg_info.aggregate_functions.clone(),
            from_distinct: false,
            rank_limit: None,

            grouping_sets: agg_info.grouping_sets.as_ref().map(|g| GroupingSets {
                grouping_id_index: g.grouping_id_column.index,
                sets: g.sets.clone(),
                dup_group_items: g.dup_group_items.clone(),
            }),
        };
        new_expr = SExpr::create_unary(Arc::new(aggregate_plan.into()), Arc::new(new_expr));

        Ok(new_expr)
    }

    fn resolve_grouping_sets(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'_>,
        sets: &[Vec<Expr>],
        available_aliases: &[(String, ScalarExpr)],
    ) -> Result<()> {
        let mut grouping_sets = Vec::with_capacity(sets.len());
        for set in sets {
            self.resolve_group_items(
                bind_context,
                select_list,
                set,
                available_aliases,
                true,
                &mut grouping_sets,
            )?;
        }
        let agg_info = &mut bind_context.aggregate_info;
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
        let mut dup_group_items = Vec::with_capacity(agg_info.group_items.len());
        for (i, item) in agg_info.group_items.iter().enumerate() {
            // We just generate a new bound index.
            let dummy = self.create_derived_column_binding(
                format!("_dup_group_item_{i}"),
                item.scalar.data_type()?,
                Some(item.scalar.clone()),
            );
            dup_group_items.push((dummy.index, *dummy.data_type));
        }

        // Add a virtual column `_grouping_id` to group items.
        let grouping_id_column = self.create_derived_column_binding(
            "_grouping_id".to_string(),
            DataType::Number(NumberDataType::UInt32),
            None,
        );

        let bound_grouping_id_col = BoundColumnRef {
            span: None,
            column: grouping_id_column.clone(),
        };

        if !self.ctx.get_settings().get_grouping_sets_to_union()? {
            agg_info.group_items_map.insert(
                bound_grouping_id_col.clone().into(),
                agg_info.group_items.len(),
            );
            agg_info.group_items.push(ScalarItem {
                index: grouping_id_column.index,
                scalar: bound_grouping_id_col.into(),
            });
        }

        let grouping_sets_info = GroupingSetsInfo {
            grouping_id_column,
            sets: grouping_sets,
            dup_group_items,
        };

        agg_info.grouping_sets = Some(grouping_sets_info);

        Ok(())
    }

    fn resolve_group_all(&mut self, select_list: &SelectList<'_>) -> Result<Vec<Expr>> {
        // Resolve group items with `FROM` context. Since the alias item can not be resolved
        // from the context, we can detect the failure and fallback to resolving with `available_aliases`.

        let f = |scalar: &ScalarExpr| matches!(scalar, ScalarExpr::AggregateFunction(_));
        let mut groups = Vec::new();
        for (idx, select_item) in select_list.items.iter().enumerate() {
            let mut finder = Finder::new(&f);
            finder.visit(&select_item.scalar)?;
            if finder.scalars().is_empty() {
                groups.push(Expr::Literal {
                    span: None,
                    value: Literal::UInt64(idx as u64 + 1),
                });
            }
        }
        Ok(groups)
    }

    fn resolve_group_items(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'_>,
        group_by: &[Expr],
        available_aliases: &[(String, ScalarExpr)],
        collect_grouping_sets: bool,
        grouping_sets: &mut Vec<Vec<ScalarExpr>>,
    ) -> Result<()> {
        if collect_grouping_sets {
            grouping_sets.push(Vec::with_capacity(group_by.len()));
        }
        // Resolve group items with `FROM` context. Since the alias item can not be resolved
        // from the context, we can detect the failure and fallback to resolving with `available_aliases`.
        for expr in group_by.iter() {
            // If expr is a number literal, then this is a index group item.
            if let Expr::Literal {
                value: Literal::UInt64(index),
                ..
            } = expr
            {
                let (scalar, alias) = Self::resolve_index_item(expr, *index, select_list)?;
                if let Entry::Vacant(entry) = bind_context
                    .aggregate_info
                    .group_items_map
                    .entry(scalar.clone())
                {
                    // Add group item if it's not duplicated
                    let column_binding = if let ScalarExpr::BoundColumnRef(ref column_ref) = scalar
                    {
                        column_ref.column.clone()
                    } else {
                        self.create_derived_column_binding(
                            alias,
                            scalar.data_type()?,
                            Some(scalar.clone()),
                        )
                    };
                    bind_context.aggregate_info.group_items.push(ScalarItem {
                        scalar: scalar.clone(),
                        index: column_binding.index,
                    });
                    entry.insert(bind_context.aggregate_info.group_items.len() - 1);
                }
                if collect_grouping_sets && !grouping_sets.last().unwrap().contains(&scalar) {
                    grouping_sets.last_mut().unwrap().push(scalar);
                }
                continue;
            }

            // Resolve scalar item and alias item
            let mut scalar_binder = ScalarBinder::new(
                bind_context,
                self.ctx.clone(),
                &self.name_resolution_ctx,
                self.metadata.clone(),
                &[],
            );

            let (mut scalar_expr, _) = scalar_binder
                .bind(expr)
                .or_else(|e| self.resolve_alias_item(bind_context, expr, available_aliases, e))?;

            let mut analyzer = SetReturningAnalyzer::new(bind_context, self.metadata.clone());
            analyzer.visit(&mut scalar_expr)?;
            if collect_grouping_sets && !grouping_sets.last().unwrap().contains(&scalar_expr) {
                grouping_sets.last_mut().unwrap().push(scalar_expr.clone());
            }

            // Was add by previous
            // The group key is duplicated
            if bind_context
                .aggregate_info
                .group_items_map
                .contains_key(&scalar_expr)
            {
                continue;
            }

            let group_item_name = format!("{:#}", expr);
            let index = if let ScalarExpr::BoundColumnRef(BoundColumnRef {
                column: ColumnBinding { index, .. },
                ..
            }) = &scalar_expr
            {
                *index
            } else {
                self.metadata.write().add_derived_column(
                    group_item_name.clone(),
                    scalar_expr.data_type()?,
                    Some(scalar_expr.clone()),
                )
            };

            bind_context.aggregate_info.group_items.push(ScalarItem {
                scalar: scalar_expr.clone(),
                index,
            });
            bind_context.aggregate_info.group_items_map.insert(
                scalar_expr,
                bind_context.aggregate_info.group_items.len() - 1,
            );
        }

        // Check group by contains aggregate functions or not
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::AggregateFunction(_) | ScalarExpr::WindowFunction(_)
            )
        };

        for item in bind_context.aggregate_info.group_items.iter() {
            let mut finder = Finder::new(&f);
            finder.visit(&item.scalar)?;
            if !finder.scalars().is_empty() {
                let scalar = finder.scalars().first().unwrap();
                return Err(ErrorCode::SemanticError(format!(
                    "GROUP BY items can't contain aggregate functions or window functions: {:?}",
                    scalar
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
        for item in bind_context.aggregate_info.group_items.iter() {
            let columns: HashSet<ScalarExpr> = bind_context
                .aggregate_info
                .group_items
                .iter()
                .filter(|p| p.scalar != item.scalar)
                .map(|p| p.scalar.clone())
                .collect();

            if prune_by_children(&item.scalar, &columns) {
                continue;
            }
            results.push(item.clone());
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

    fn resolve_index_item(
        expr: &Expr,
        index: u64,
        select_list: &SelectList,
    ) -> Result<(ScalarExpr, String)> {
        // Convert to zero-based index
        if index < 1 {
            return Err(ErrorCode::SemanticError(format!(
                "GROUP BY position {} is illegal",
                index
            ))
            .set_span(expr.span()));
        }
        let index = index as usize - 1;
        if index >= select_list.items.len() {
            return Err(ErrorCode::SemanticError(format!(
                "GROUP BY position {} is not in select list",
                index + 1
            ))
            .set_span(expr.span()));
        }
        let item = select_list
            .items
            .get(index)
            .ok_or_else(|| ErrorCode::Internal("Should not fail"))?;

        let scalar = item.scalar.clone();
        let alias = item.alias.clone();

        Ok((scalar, alias))
    }

    fn resolve_alias_item(
        &mut self,
        bind_context: &mut BindContext,
        expr: &Expr,
        available_aliases: &[(String, ScalarExpr)],
        original_error: ErrorCode,
    ) -> Result<(ScalarExpr, DataType)> {
        let mut result: Vec<usize> = vec![];
        // If cannot resolve group item, then try to find an available alias
        for (i, (alias, _)) in available_aliases.iter().enumerate() {
            // Alias of the select item
            if let Expr::ColumnRef {
                column:
                    ColumnRef {
                        database: None,
                        table: None,
                        column,
                    },
                ..
            } = expr
            {
                if alias.eq_ignore_ascii_case(column.name()) {
                    result.push(i);
                }
            }
        }

        if result.is_empty() {
            Err(original_error)
        } else if result.len() > 1 {
            Err(
                ErrorCode::SemanticError(format!("GROUP BY \"{}\" is ambiguous", expr))
                    .set_span(expr.span()),
            )
        } else {
            let (alias, scalar) = available_aliases[result[0]].clone();

            // check scalar first, avoid duplicate create column.
            let mut scalar_column_index = None;
            let column_binding = if let Some(column_index) =
                bind_context.aggregate_info.group_items_map.get(&scalar)
            {
                scalar_column_index = Some(*column_index);

                let group_item = &bind_context.aggregate_info.group_items[*column_index];
                ColumnBindingBuilder::new(
                    alias.clone(),
                    group_item.index,
                    Box::new(group_item.scalar.data_type()?),
                    Visibility::Visible,
                )
                .build()
            } else if let ScalarExpr::BoundColumnRef(column_ref) = &scalar {
                let mut column = column_ref.column.clone();
                column.column_name = alias.clone();
                column
            } else {
                self.create_derived_column_binding(
                    alias.clone(),
                    scalar.data_type()?,
                    Some(scalar.clone()),
                )
            };

            if scalar_column_index.is_none() {
                let index = column_binding.index;
                bind_context.aggregate_info.group_items.push(ScalarItem {
                    scalar: scalar.clone(),
                    index,
                });
                bind_context.aggregate_info.group_items_map.insert(
                    scalar.clone(),
                    bind_context.aggregate_info.group_items.len() - 1,
                );
                scalar_column_index = Some(bind_context.aggregate_info.group_items.len() - 1);
            }

            let scalar_column_index = scalar_column_index.unwrap();

            let column_ref: ScalarExpr = BoundColumnRef {
                span: scalar.span(),
                column: column_binding.clone(),
            }
            .into();
            let has_column = bind_context
                .aggregate_info
                .group_items_map
                .contains_key(&column_ref);
            if !has_column {
                // We will add the alias to BindContext, so we can reference it
                // in `HAVING` and `ORDER BY` clause.
                bind_context.add_column_binding(column_binding.clone());

                // Add a mapping (alias -> scalar), so we can resolve the alias later
                bind_context
                    .aggregate_info
                    .group_items_map
                    .insert(column_ref, scalar_column_index);
            }

            Ok((scalar.clone(), scalar.data_type()?))
        }
    }
}

/// Replace [`AggregateFunction`] with a [`ColumnBinding`] if the function is already replaced.
pub fn find_replaced_aggregate_function(
    agg_info: &AggregateInfo,
    display_name: &str,
    return_type: &DataType,
    new_name: &str,
) -> Option<ColumnBinding> {
    agg_info
        .get_aggregate_function(display_name)
        .map(|scalar_item| {
            // This expression is already replaced.
            debug_assert_eq!(&scalar_item.scalar.data_type().unwrap(), return_type);
            ColumnBindingBuilder::new(
                new_name.to_string(),
                scalar_item.index,
                Box::new(return_type.clone()),
                Visibility::Visible,
            )
            .build()
        })
}
