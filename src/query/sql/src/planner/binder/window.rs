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

use std::collections::HashMap;

use common_exception::Result;

use crate::binder::select::SelectList;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;
use crate::plans::Window;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowOrderBy;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBinding;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

impl Binder {
    pub(super) async fn bind_window_function(
        &mut self,
        window_info: &WindowFunctionInto,
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
            order_by: window_info.order_by_items.clone(),
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
        let mut replaced_order_by_items: Vec<WindowOrderBy> =
            Vec::with_capacity(window.order_by.len());

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

        // resolve order by
        let mut order_by_items = vec![];
        for (i, order) in window.order_by.iter().enumerate() {
            let name = format!("{}_order_{}", &window.agg_func.func_name, i);
            if let ScalarExpr::BoundColumnRef(column_ref) = &order.expr {
                replaced_order_by_items.push(WindowOrderBy {
                    expr: column_ref.clone().into(),
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                });
                order_by_items.push(WindowOrderByInfo {
                    order_by_item: ScalarItem {
                        index: column_ref.column.index,
                        scalar: order.expr.clone(),
                    },
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                })
            } else {
                let index = metadata
                    .write()
                    .add_derived_column(name.clone(), order.expr.data_type()?);

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    column_name: name,
                    index,
                    data_type: Box::new(order.expr.data_type()?),
                    visibility: Visibility::Visible,
                };
                replaced_order_by_items.push(WindowOrderBy {
                    expr: BoundColumnRef {
                        span: order.expr.span(),
                        column: column_binding,
                    }
                    .into(),
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                });
                order_by_items.push(WindowOrderByInfo {
                    order_by_item: ScalarItem {
                        index,
                        scalar: order.expr.clone(),
                    },
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                })
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
        let window_info = WindowFunctionInto {
            aggregate_function: ScalarItem {
                scalar: replaced_agg.clone().into(),
                index,
            },
            aggregate_arguments: agg_args,
            partition_by_items,
            order_by_items,
            frame: window.frame.clone(),
        };

        // push window info to BindContext
        window_infos.window_functions.push(window_info);
        window_infos.window_functions_map.insert(
            replaced_agg.display_name.clone(),
            window_infos.window_functions.len() - 1,
        );

        let replaced_window = WindowFunc {
            agg_func: replaced_agg,
            partition_by: replaced_partition_items,
            order_by: replaced_order_by_items,
            frame: window.frame.clone(),
        };

        Ok(replaced_window.into())
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct WindowInfo {
    pub window_functions: Vec<WindowFunctionInto>,
    pub window_functions_map: HashMap<String, usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WindowFunctionInto {
    pub aggregate_function: ScalarItem,
    pub aggregate_arguments: Vec<ScalarItem>,
    pub partition_by_items: Vec<ScalarItem>,
    pub order_by_items: Vec<WindowOrderByInfo>,
    pub frame: WindowFuncFrame,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct WindowOrderByInfo {
    pub order_by_item: ScalarItem,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
}
