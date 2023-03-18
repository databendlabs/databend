// Copyright 2023 Datafuse Labs.
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

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::OrderByExpr;
use common_ast::ast::SelectTarget;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::binder::select::SelectList;
use crate::binder::sort::OrderItem;
use crate::binder::sort::OrderItems;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;
use crate::plans::Sort;
use crate::plans::SortItem;
use crate::plans::Window;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;

impl Binder {
    pub(super) async fn fetch_window_order_by_expr(
        &mut self,
        select_list: &[SelectTarget],
    ) -> Vec<Vec<OrderByExpr>> {
        let mut window_order_bys = vec![];
        for select_target in select_list {
            match select_target {
                SelectTarget::QualifiedName { .. } => continue,
                SelectTarget::AliasedExpr { expr, .. } => match expr.as_ref() {
                    Expr::FunctionCall { window, .. } => {
                        if let Some(window) = window {
                            window_order_bys.push(window.order_by.clone());
                        }
                    }
                    _ => continue,
                },
            }
        }
        window_order_bys
    }

    pub(super) async fn fetch_window_order_items(
        &mut self,
        from_context: &BindContext,
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        projections: &[ColumnBinding],
        window_order_by: &[OrderByExpr],
    ) -> Result<OrderItems> {
        let mut order_items = Vec::with_capacity(window_order_by.len());
        for order in window_order_by {
            match &order.expr {
                Expr::ColumnRef {
                    database: ref database_name,
                    table: ref table_name,
                    column: ref ident,
                    ..
                } => {
                    // We first search the identifier in select list
                    let mut found = false;
                    let database = database_name
                        .as_ref()
                        .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name);
                    let table = table_name
                        .as_ref()
                        .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name);
                    let column = normalize_identifier(ident, &self.name_resolution_ctx).name;

                    for item in projections.iter() {
                        if BindContext::match_column_binding(
                            database.as_deref(),
                            table.as_deref(),
                            column.as_str(),
                            item,
                        ) {
                            order_items.push(OrderItem {
                                expr: order.clone(),
                                index: item.index,
                                name: item.column_name.clone(),
                                need_eval_scalar: scalar_items.get(&item.index).map_or(
                                    false,
                                    |scalar_item| {
                                        !matches!(
                                            &scalar_item.scalar,
                                            ScalarExpr::BoundColumnRef(_)
                                        )
                                    },
                                ),
                            });
                            found = true;
                            break;
                        }
                    }

                    if found {
                        continue;
                    }

                    return Err(ErrorCode::SemanticError(
                        "for WINDOW FUNCTION, ORDER BY expressions must appear in select list"
                            .to_string(),
                    )
                    .set_span(order.expr.span()));
                }
                Expr::Literal {
                    lit: Literal::UInt64(index),
                    ..
                } => {
                    let index = *index as usize - 1;
                    if index >= projections.len() {
                        return Err(ErrorCode::SemanticError(format!(
                            "ORDER BY position {} is not in select list",
                            index + 1
                        ))
                        .set_span(order.expr.span()));
                    }

                    order_items.push(OrderItem {
                        expr: order.clone(),
                        name: projections[index].column_name.clone(),
                        index: projections[index].index,
                        need_eval_scalar: scalar_items.get(&projections[index].index).map_or(
                            false,
                            |scalar_item| {
                                !matches!(&scalar_item.scalar, ScalarExpr::BoundColumnRef(_))
                            },
                        ),
                    });
                }
                _ => {
                    let mut bind_context = from_context.clone();
                    for column_binding in projections.iter() {
                        if bind_context.columns.contains(column_binding) {
                            continue;
                        }
                        bind_context.columns.push(column_binding.clone());
                    }
                    let mut scalar_binder = ScalarBinder::new(
                        &bind_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &[],
                    );
                    let (bound_expr, _) = scalar_binder.bind(&order.expr).await?;
                    let rewrite_scalar = self
                        .rewrite_scalar_with_replacement(&bound_expr, &|nest_scalar| {
                            if let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) =
                                nest_scalar
                            {
                                if let Some(scalar_item) = scalar_items.get(&column.index) {
                                    return Ok(Some(scalar_item.scalar.clone()));
                                }
                            }
                            Ok(None)
                        })
                        .map_err(|e| ErrorCode::SemanticError(e.message()))?;
                    let column_binding = self.create_column_binding(
                        None,
                        None,
                        format!("{:#}", order.expr),
                        rewrite_scalar.data_type()?,
                    );
                    order_items.push(OrderItem {
                        expr: order.clone(),
                        name: column_binding.column_name.clone(),
                        index: column_binding.index,
                        need_eval_scalar: true,
                    });
                    scalar_items.insert(column_binding.index, ScalarItem {
                        scalar: rewrite_scalar,
                        index: column_binding.index,
                    });
                }
            }
        }
        Ok(OrderItems { items: order_items })
    }

    pub(crate) async fn bind_window_order_by(
        &mut self,
        _from_context: &BindContext,
        order_by: OrderItems,
        _select_list: &SelectList<'_>,
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut order_by_items = Vec::with_capacity(order_by.items.len());
        let mut scalars = vec![];

        for order in order_by.items {
            if let Expr::ColumnRef {
                database: ref database_name,
                table: ref table_name,
                ..
            } = order.expr.expr
            {
                if let (Some(table_name), Some(database_name)) = (table_name, database_name) {
                    let catalog_name = self.ctx.get_current_catalog();
                    let catalog = self.ctx.get_catalog(catalog_name.as_str())?;
                    catalog
                        .get_table(
                            &self.ctx.get_tenant(),
                            &database_name.name,
                            &table_name.name,
                        )
                        .await?;
                }
            }
            if order.need_eval_scalar {
                if let Entry::Occupied(entry) = scalar_items.entry(order.index) {
                    let (index, item) = entry.remove_entry();
                    scalars.push(ScalarItem {
                        scalar: item.scalar,
                        index,
                    });
                }
            }

            // null is the largest value in databend, smallest in hive
            // todo: rewrite after https://github.com/jorgecarleitao/arrow2/pull/1286 is merged
            let default_nulls_first = !self
                .ctx
                .get_settings()
                .get_sql_dialect()
                .unwrap()
                .is_null_biggest();
            let order_by_item = SortItem {
                index: order.index,
                asc: order.expr.asc.unwrap_or(true),
                nulls_first: order.expr.nulls_first.unwrap_or(default_nulls_first),
            };

            order_by_items.push(order_by_item);
        }

        let mut new_expr = if !scalars.is_empty() {
            let eval_scalar = EvalScalar { items: scalars };
            SExpr::create_unary(eval_scalar.into(), child)
        } else {
            child
        };

        let sort_plan = Sort {
            items: order_by_items,
            limit: None,
        };
        new_expr = SExpr::create_unary(sort_plan.into(), new_expr);
        Ok(new_expr)
    }

    pub(super) async fn bind_window_function(
        &mut self,
        window_info: &WindowInfo,
        child: SExpr,
    ) -> Result<SExpr> {
        // Build a ProjectPlan, which will produce aggregate arguments and window partitions
        let mut scalar_items: Vec<ScalarItem> = Vec::with_capacity(
            window_info.aggregate_arguments.len() + window_info.partition_by_items.len(),
        );
        for arg in window_info.aggregate_arguments.iter() {
            scalar_items.push(arg.clone());
        }
        for part in window_info.partition_by_items.iter() {
            scalar_items.push(part.clone());
        }

        let mut new_expr = child;
        if !scalar_items.is_empty() {
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(eval_scalar.into(), new_expr);
        }

        let window_plan = Window {
            aggregate_function: window_info.aggregate_function.clone(),
            partition_by: window_info.partition_by_items.clone(),
            frame: window_info.frame.clone(),
        };
        new_expr = SExpr::create_unary(window_plan.into(), new_expr);

        Ok(new_expr)
    }

    /// Analyze window functions in select clause, this will rewrite window functions.
    pub(crate) fn analyze_window_select(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            if let ScalarExpr::WindowFunction(window_func) = &item.scalar {
                let new_scalar =
                    self.replace_window_function(bind_context, self.metadata.clone(), window_func)?;
                item.scalar = new_scalar;
            }
        }

        Ok(())
    }

    fn replace_window_function(
        &mut self,
        bind_context: &mut BindContext,
        metadata: MetadataRef,
        window: &WindowFunc,
    ) -> Result<ScalarExpr> {
        let window_infos = &mut bind_context.windows;
        let mut replaced_args: Vec<ScalarExpr> = Vec::with_capacity(window.agg_func.args.len());
        let mut replaced_partition_items: Vec<ScalarExpr> =
            Vec::with_capacity(window.partition_by.len());

        // resolve aggregate function args in window function.
        let mut agg_args = vec![];
        for (i, arg) in window.agg_func.args.iter().enumerate() {
            let name = format!("{}_arg_{}", &window.agg_func.func_name, i);
            if let ScalarExpr::BoundColumnRef(column_ref) = arg {
                replaced_args.push(column_ref.clone().into());
                agg_args.push(ScalarItem {
                    index: column_ref.column.index,
                    scalar: arg.clone(),
                });
            } else {
                let index = metadata
                    .write()
                    .add_derived_column(name.clone(), arg.data_type()?);

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    column_name: name,
                    index,
                    data_type: Box::new(arg.data_type()?),
                    visibility: Visibility::Visible,
                };
                replaced_args.push(
                    BoundColumnRef {
                        span: arg.span(),
                        column: column_binding.clone(),
                    }
                    .into(),
                );
                agg_args.push(ScalarItem {
                    index,
                    scalar: arg.clone(),
                });
            }
        }

        // resolve partition by
        let mut partition_by_items = vec![];
        for (i, part) in window.partition_by.iter().enumerate() {
            let name = format!("{}_part_{}", &window.agg_func.func_name, i);
            if let ScalarExpr::BoundColumnRef(column_ref) = part {
                replaced_partition_items.push(column_ref.clone().into());
                partition_by_items.push(ScalarItem {
                    index: column_ref.column.index,
                    scalar: part.clone(),
                });
            } else {
                let index = metadata
                    .write()
                    .add_derived_column(name.clone(), part.data_type()?);

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    column_name: name,
                    index,
                    data_type: Box::new(part.data_type()?),
                    visibility: Visibility::Visible,
                };
                replaced_partition_items.push(
                    BoundColumnRef {
                        span: part.span(),
                        column: column_binding.clone(),
                    }
                    .into(),
                );
                partition_by_items.push(ScalarItem {
                    index,
                    scalar: part.clone(),
                });
            }
        }

        let index = metadata
            .write()
            .add_derived_column(window.display_name(), *window.agg_func.return_type.clone());

        let replaced_agg = AggregateFunction {
            display_name: window.agg_func.display_name.clone(),
            func_name: window.agg_func.func_name.clone(),
            distinct: window.agg_func.distinct,
            params: window.agg_func.params.clone(),
            args: replaced_args,
            return_type: window.agg_func.return_type.clone(),
        };

        // create window info
        let window_info = WindowInfo {
            aggregate_function: ScalarItem {
                scalar: replaced_agg.clone().into(),
                index,
            },
            aggregate_arguments: agg_args,
            partition_by_items,
            frame: window.frame.clone(),
        };

        // push window info to BindContext
        window_infos.push(window_info);

        let replaced_window = WindowFunc {
            agg_func: replaced_agg,
            partition_by: replaced_partition_items,
            frame: window.frame.clone(),
        };

        Ok(replaced_window.into())
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WindowInfo {
    pub aggregate_function: ScalarItem,
    pub aggregate_arguments: Vec<ScalarItem>,
    pub partition_by_items: Vec<ScalarItem>,
    pub frame: WindowFuncFrame,
}
