// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::SelectTarget;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;

use super::prune_by_children;
use crate::binder::scalar::ScalarBinder;
use crate::binder::select::SelectList;
use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Unnest;
use crate::BindContext;
use crate::MetadataRef;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct AggregateInfo {
    /// Aggregation functions
    pub aggregate_functions: Vec<ScalarItem>,

    /// Arguments of aggregation functions
    pub aggregate_arguments: Vec<ScalarItem>,

    /// Group items of aggregation
    pub group_items: Vec<ScalarItem>,

    /// Output columns of aggregation, including group items and aggregate functions.
    pub output_columns: Vec<ColumnBinding>,

    /// Mapping: (aggregate function display name) -> (index of agg func in `aggregate_functions`)
    /// This is used to find a aggregate function in current context.
    pub aggregate_functions_map: HashMap<String, usize>,

    /// Mapping: (group item display name) -> (index of group item in `group_items`)
    /// This is used to check if a scalar expression is a group item.
    /// For example, `SELECT count(*) FROM t GROUP BY a+1 HAVING a+1+1`.
    /// The group item `a+1` is involved in `a+1+1`, so it's a valid `HAVING`.
    /// We will check the validity by lookup this map with display name.
    ///
    /// TODO(leiysky): so far we are using `Debug` string of `Scalar` as identifier,
    /// maybe a more reasonable way is needed
    pub group_items_map: HashMap<String, usize>,
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

    pub fn visit(&mut self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => Ok(scalar.clone()),
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::AndExpr(scalar) => Ok(AndExpr {
                left: Box::new(self.visit(&scalar.left)?),
                right: Box::new(self.visit(&scalar.right)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::OrExpr(scalar) => Ok(OrExpr {
                left: Box::new(self.visit(&scalar.left)?),
                right: Box::new(self.visit(&scalar.right)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::NotExpr(scalar) => Ok(NotExpr {
                argument: Box::new(self.visit(&scalar.argument)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::ComparisonExpr(scalar) => Ok(ComparisonExpr {
                op: scalar.op.clone(),
                left: Box::new(self.visit(&scalar.left)?),
                right: Box::new(self.visit(&scalar.right)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::FunctionCall(func) => {
                let new_args = func
                    .arguments
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FunctionCall {
                    params: func.params.clone(),
                    arguments: new_args,
                    func_name: func.func_name.clone(),
                    return_type: func.return_type.clone(),
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                is_try: cast.is_try,
                argument: Box::new(self.visit(&cast.argument)?),
                from_type: cast.from_type.clone(),
                target_type: cast.target_type.clone(),
            }
            .into()),
            ScalarExpr::Unnest(unnest) => Ok(Unnest {
                argument: Box::new(self.visit(&unnest.argument)?),
                return_type: unnest.return_type.clone(),
            }
            .into()),

            // TODO(leiysky): should we recursively process subquery here?
            ScalarExpr::SubqueryExpr(_) => Ok(scalar.clone()),

            ScalarExpr::AggregateFunction(agg_func) => self.replace_aggregate_function(agg_func),
        }
    }

    /// Replace the arguments of aggregate function with a BoundColumnRef, and
    /// add the replaced aggregate function and the arguments into `AggregateInfo`.
    fn replace_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<ScalarExpr> {
        let agg_info = &mut self.bind_context.aggregate_info;
        let mut replaced_args: Vec<ScalarExpr> = Vec::with_capacity(aggregate.args.len());

        for (i, arg) in aggregate.args.iter().enumerate() {
            let name = format!("{}_arg_{}", &aggregate.func_name, i);
            if let ScalarExpr::BoundColumnRef(column_ref) = arg {
                replaced_args.push(column_ref.clone().into());
                agg_info.aggregate_arguments.push(ScalarItem {
                    index: column_ref.column.index,
                    scalar: arg.clone(),
                });
            } else {
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(name.clone(), arg.data_type());

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,

                    // TODO(leiysky): use a more reasonable name, since aggregate arguments
                    // can not be referenced, the name is only for debug
                    column_name: name,
                    index,
                    data_type: Box::new(arg.data_type()),
                    visibility: Visibility::Visible,
                };
                replaced_args.push(
                    BoundColumnRef {
                        column: column_binding.clone(),
                    }
                    .into(),
                );
                agg_info.aggregate_arguments.push(ScalarItem {
                    index,
                    scalar: arg.clone(),
                });
            }
        }

        let index = self.metadata.write().add_derived_column(
            aggregate.display_name.clone(),
            *aggregate.return_type.clone(),
        );

        let replaced_agg = AggregateFunction {
            display_name: aggregate.display_name.clone(),
            func_name: aggregate.func_name.clone(),
            distinct: aggregate.distinct,
            params: aggregate.params.clone(),
            args: replaced_args,
            return_type: aggregate.return_type.clone(),
        };

        agg_info.aggregate_functions.push(ScalarItem {
            scalar: replaced_agg.clone().into(),
            index,
        });
        agg_info.aggregate_functions_map.insert(
            replaced_agg.display_name.clone(),
            agg_info.aggregate_functions.len() - 1,
        );

        Ok(replaced_agg.into())
    }
}

impl Binder {
    /// Analyze aggregates in select clause, this will rewrite aggregate functions.
    /// See `AggregateRewriter` for more details.
    pub(crate) fn analyze_aggregate_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
            let new_scalar = rewriter.visit(&item.scalar)?;
            item.scalar = new_scalar;
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
    pub async fn analyze_group_items<'a>(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'a>,
        group_by: &[Expr],
    ) -> Result<()> {
        let mut available_aliases = vec![];

        // Extract available aliases from `SELECT` clause,
        for item in select_list.items.iter() {
            if let SelectTarget::AliasedExpr { alias: Some(_), .. } = item.select_target {
                let column = if let ScalarExpr::BoundColumnRef(column_ref) = &item.scalar {
                    let mut column = column_ref.column.clone();
                    column.column_name = item.alias.clone();
                    column
                } else {
                    self.create_column_binding(
                        None,
                        None,
                        item.alias.clone(),
                        item.scalar.data_type(),
                    )
                };
                available_aliases.push((column, item.scalar.clone()));
            }
        }

        self.resolve_group_items(bind_context, select_list, group_by, &available_aliases)
            .await
    }

    pub(super) async fn bind_aggregate(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        // Enter in_grouping state
        bind_context.in_grouping = true;

        // Build a ProjectPlan, which will produce aggregate arguments and group items
        let agg_info = &bind_context.aggregate_info;
        let mut scalar_items: Vec<ScalarItem> =
            Vec::with_capacity(agg_info.aggregate_arguments.len() + agg_info.group_items.len());
        for arg in agg_info.aggregate_arguments.iter() {
            scalar_items.push(arg.clone());
        }
        for item in agg_info.group_items.iter() {
            scalar_items.push(item.clone());
        }

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(eval_scalar.into(), new_expr);
        }

        let aggregate_plan = Aggregate {
            mode: AggregateMode::Initial,
            group_items: bind_context.aggregate_info.group_items.clone(),
            aggregate_functions: bind_context.aggregate_info.aggregate_functions.clone(),
            from_distinct: false,
            limit: None,
        };
        new_expr = SExpr::create_unary(aggregate_plan.into(), new_expr);

        Ok(new_expr)
    }

    async fn resolve_group_items(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &SelectList<'_>,
        group_by: &[Expr],
        available_aliases: &[(ColumnBinding, ScalarExpr)],
    ) -> Result<()> {
        // Resolve group items with `FROM` context. Since the alias item can not be resolved
        // from the context, we can detect the failure and fallback to resolving with `available_aliases`.
        for expr in group_by.iter() {
            // If expr is a number literal, then this is a index group item.
            if let Expr::Literal {
                lit: Literal::Integer(index),
                ..
            } = expr
            {
                let (scalar, alias) = Self::resolve_index_item(expr, *index, select_list)?;
                let key = format!("{:?}", &scalar);
                if let Entry::Vacant(entry) = bind_context.aggregate_info.group_items_map.entry(key)
                {
                    // Add group item if it's not duplicated
                    let column_binding = if let ScalarExpr::BoundColumnRef(ref column_ref) = scalar
                    {
                        column_ref.column.clone()
                    } else {
                        self.create_column_binding(None, None, alias, scalar.data_type())
                    };
                    bind_context.aggregate_info.group_items.push(ScalarItem {
                        scalar,
                        index: column_binding.index,
                    });
                    entry.insert(bind_context.aggregate_info.group_items.len() - 1);
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
            let (scalar_expr, data_type) = scalar_binder
                .bind(expr)
                .await
                .or_else(|e| Self::resolve_alias_item(bind_context, expr, available_aliases, e))?;

            if bind_context
                .aggregate_info
                .group_items_map
                .get(&format!("{:?}", &scalar_expr))
                .is_some()
            {
                // The group key is duplicated
                continue;
            }

            let group_item_name = format!("{:#}", expr);
            let index = if let ScalarExpr::BoundColumnRef(BoundColumnRef {
                column: ColumnBinding { index, .. },
            }) = &scalar_expr
            {
                *index
            } else {
                self.metadata
                    .write()
                    .add_derived_column(group_item_name.clone(), data_type.clone())
            };

            bind_context.aggregate_info.group_items.push(ScalarItem {
                scalar: scalar_expr.clone(),
                index,
            });
            bind_context.aggregate_info.group_items_map.insert(
                format!("{:?}", &scalar_expr),
                bind_context.aggregate_info.group_items.len() - 1,
            );
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
                .insert(format!("{:?}", &item.scalar), i);
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
        bind_context: &mut BindContext,
        expr: &Expr,
        available_aliases: &[(ColumnBinding, ScalarExpr)],
        original_error: ErrorCode,
    ) -> Result<(ScalarExpr, DataType)> {
        let mut result: Vec<usize> = vec![];
        // If cannot resolve group item, then try to find an available alias
        for (i, (column_binding, _)) in available_aliases.iter().enumerate() {
            // Alias of the select item
            let col_name = column_binding.column_name.as_str();
            if let Expr::ColumnRef {
                column,
                database: None,
                table: None,
                ..
            } = expr
            {
                if col_name.eq_ignore_ascii_case(column.name.as_str()) {
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
            let (column_binding, scalar) = available_aliases[result[0]].clone();
            // We will add the alias to BindContext, so we can reference it
            // in `HAVING` and `ORDER BY` clause.
            bind_context.columns.push(column_binding.clone());

            let index = column_binding.index;
            bind_context.aggregate_info.group_items.push(ScalarItem {
                scalar: scalar.clone(),
                index,
            });
            bind_context.aggregate_info.group_items_map.insert(
                format!("{:?}", &scalar),
                bind_context.aggregate_info.group_items.len() - 1,
            );

            // Add a mapping (alias -> scalar), so we can resolve the alias later
            let column_ref: ScalarExpr = BoundColumnRef {
                column: column_binding,
            }
            .into();
            bind_context.aggregate_info.group_items_map.insert(
                format!("{:?}", &column_ref),
                bind_context.aggregate_info.group_items.len() - 1,
            );

            Ok((scalar.clone(), scalar.data_type()))
        }
    }
}
