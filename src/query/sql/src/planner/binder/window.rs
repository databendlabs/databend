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

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;

use super::select::SelectList;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::LagLeadFunction;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Window;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

impl Binder {
    #[async_backtrace::framed]
    pub(super) async fn bind_window_function(
        &mut self,
        window_info: &WindowFunctionInfo,
        child: SExpr,
    ) -> Result<SExpr> {
        let window_plan = Window {
            span: window_info.span,
            index: window_info.index,
            function: window_info.func.clone(),
            arguments: window_info.arguments.clone(),
            partition_by: window_info.partition_by_items.clone(),
            order_by: window_info.order_by_items.clone(),
            frame: window_info.frame.clone(),
        };

        Ok(SExpr::create_unary(window_plan.into(), child))
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct WindowInfo {
    pub window_functions: Vec<WindowFunctionInfo>,
    pub window_functions_map: HashMap<String, usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WindowFunctionInfo {
    pub span: Span,
    pub index: IndexType,
    pub func: WindowFuncType,
    pub arguments: Vec<ScalarItem>,
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

pub(super) struct WindowRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
    // While analyzing in-window aggregate function, we can replace it with a BoundColumnRef
    in_window: bool,
}

impl<'a> WindowRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
            in_window: false,
        }
    }

    pub fn visit(&mut self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => Ok(scalar.clone()),
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::FunctionCall(func) => {
                let new_args = func
                    .arguments
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FunctionCall {
                    span: func.span,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments: new_args,
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.visit(&cast.argument)?),
                target_type: cast.target_type.clone(),
            }
            .into()),

            // TODO(leiysky): should we recursively process subquery here?
            ScalarExpr::SubqueryExpr(_) => Ok(scalar.clone()),

            ScalarExpr::AggregateFunction(agg_func) => {
                if self.in_window {
                    if let Some(index) = self
                        .bind_context
                        .aggregate_info
                        .aggregate_functions_map
                        .get(&agg_func.display_name)
                    {
                        let agg = &self.bind_context.aggregate_info.aggregate_functions[*index];
                        let column_binding = ColumnBinding {
                            database_name: None,
                            table_name: None,
                            table_index: None,
                            column_name: agg_func.display_name.clone(),
                            index: agg.index,
                            data_type: agg_func.return_type.clone(),
                            visibility: Visibility::Visible,
                        };
                        Ok(BoundColumnRef {
                            span: None,
                            column: column_binding,
                        }
                        .into())
                    } else {
                        Err(ErrorCode::BadArguments("Invalid window function argument"))
                    }
                } else {
                    let new_args = agg_func
                        .args
                        .iter()
                        .map(|arg| self.visit(arg))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(AggregateFunction {
                        func_name: agg_func.func_name.clone(),
                        distinct: agg_func.distinct,
                        params: agg_func.params.clone(),
                        args: new_args,
                        return_type: agg_func.return_type.clone(),
                        display_name: agg_func.display_name.clone(),
                    }
                    .into())
                }
            }

            ScalarExpr::WindowFunction(window) => {
                self.in_window = true;
                let scalar = self.replace_window_function(window)?;
                self.in_window = false;
                Ok(scalar)
            }
        }
    }

    fn replace_window_function(&mut self, window: &WindowFunc) -> Result<ScalarExpr> {
        let mut replaced_partition_items: Vec<ScalarExpr> =
            Vec::with_capacity(window.partition_by.len());
        let mut replaced_order_by_items: Vec<WindowOrderBy> =
            Vec::with_capacity(window.order_by.len());
        let mut agg_args = vec![];

        let window_func_name = window.func.func_name();
        let func = match &window.func {
            WindowFuncType::Aggregate(agg) => {
                // resolve aggregate function args in window function.
                let mut replaced_args: Vec<ScalarExpr> = Vec::with_capacity(agg.args.len());
                for (i, arg) in agg.args.iter().enumerate() {
                    let arg = self.visit(arg)?;
                    let name = format!("{}_arg_{}", &window_func_name, i);
                    if let ScalarExpr::BoundColumnRef(column_ref) = &arg {
                        replaced_args.push(column_ref.clone().into());
                        agg_args.push(ScalarItem {
                            index: column_ref.column.index,
                            scalar: arg.clone(),
                        });
                    } else {
                        let index = self
                            .metadata
                            .write()
                            .add_derived_column(name.clone(), arg.data_type()?);

                        // Generate a ColumnBinding for each argument of aggregates
                        let column_binding = ColumnBinding {
                            database_name: None,
                            table_name: None,
                            table_index: None,
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
                WindowFuncType::Aggregate(AggregateFunction {
                    display_name: agg.display_name.clone(),
                    func_name: agg.func_name.clone(),
                    distinct: agg.distinct,
                    params: agg.params.clone(),
                    args: replaced_args,
                    return_type: agg.return_type.clone(),
                })
            }
            WindowFuncType::Lag(lag) => {
                let (new_arg, new_default) =
                    self.replace_lag_lead_args(&mut agg_args, &window_func_name, lag)?;

                WindowFuncType::Lag(LagLeadFunction {
                    arg: Box::new(new_arg),
                    offset: lag.offset,
                    default: new_default,
                    return_type: lag.return_type.clone(),
                })
            }
            WindowFuncType::Lead(lead) => {
                let (new_arg, new_default) =
                    self.replace_lag_lead_args(&mut agg_args, &window_func_name, lead)?;

                WindowFuncType::Lead(LagLeadFunction {
                    arg: Box::new(new_arg),
                    offset: lead.offset,
                    default: new_default,
                    return_type: lead.return_type.clone(),
                })
            }
            func => func.clone(),
        };

        // resolve partition by
        let mut partition_by_items = vec![];
        for (i, part) in window.partition_by.iter().enumerate() {
            let part = self.visit(part)?;
            let name = format!("{}_part_{}", &window_func_name, i);
            if let ScalarExpr::BoundColumnRef(column_ref) = &part {
                replaced_partition_items.push(column_ref.clone().into());
                partition_by_items.push(ScalarItem {
                    index: column_ref.column.index,
                    scalar: part.clone(),
                });
            } else {
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(name.clone(), part.data_type()?);

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    table_index: None,
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
            let order_expr = self.visit(&order.expr)?;
            let name = format!("{}_order_{}", &window_func_name, i);
            if let ScalarExpr::BoundColumnRef(column_ref) = &order_expr {
                replaced_order_by_items.push(WindowOrderBy {
                    expr: column_ref.clone().into(),
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                });
                order_by_items.push(WindowOrderByInfo {
                    order_by_item: ScalarItem {
                        index: column_ref.column.index,
                        scalar: order_expr.clone(),
                    },
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                })
            } else {
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(name.clone(), order_expr.data_type()?);

                // Generate a ColumnBinding for each argument of aggregates
                let column_binding = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    table_index: None,
                    column_name: name,
                    index,
                    data_type: Box::new(order_expr.data_type()?),
                    visibility: Visibility::Visible,
                };
                replaced_order_by_items.push(WindowOrderBy {
                    expr: BoundColumnRef {
                        span: order_expr.span(),
                        column: column_binding,
                    }
                    .into(),
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                });
                order_by_items.push(WindowOrderByInfo {
                    order_by_item: ScalarItem {
                        index,
                        scalar: order_expr,
                    },
                    asc: order.asc,
                    nulls_first: order.nulls_first,
                })
            }
        }

        let index = self
            .metadata
            .write()
            .add_derived_column(window.display_name.clone(), window.func.return_type());

        // create window info
        let window_info = WindowFunctionInfo {
            span: window.span,
            index,
            func: func.clone(),
            arguments: agg_args,
            partition_by_items,
            order_by_items,
            frame: window.frame.clone(),
        };

        let window_infos = &mut self.bind_context.windows;
        // push window info to BindContext
        window_infos.window_functions.push(window_info);
        window_infos.window_functions_map.insert(
            window.display_name.clone(),
            window_infos.window_functions.len() - 1,
        );

        let replaced_window = WindowFunc {
            span: window.span,
            display_name: window.display_name.clone(),
            func,
            partition_by: replaced_partition_items,
            order_by: replaced_order_by_items,
            frame: window.frame.clone(),
        };

        Ok(replaced_window.into())
    }

    fn replace_lag_lead_args(
        &mut self,
        agg_args: &mut Vec<ScalarItem>,
        window_func_name: &String,
        f: &LagLeadFunction,
    ) -> Result<(ScalarExpr, Option<Box<ScalarExpr>>)> {
        let new_arg = self.visit(&f.arg)?;
        let new_default = match &f.default {
            None => None,
            Some(d) => {
                let d = self.visit(d)?;
                let replaced_default = if let ScalarExpr::BoundColumnRef(column_ref) = &d {
                    agg_args.push(ScalarItem {
                        index: column_ref.column.index,
                        scalar: d.clone(),
                    });
                    column_ref.clone().into()
                } else {
                    let name = format!("{}_default_value", &window_func_name);
                    let index = self
                        .metadata
                        .write()
                        .add_derived_column(name.clone(), d.data_type()?);

                    let column_binding = ColumnBinding {
                        database_name: None,
                        table_name: None,
                        table_index: None,
                        column_name: name,
                        index,
                        data_type: Box::new(d.data_type()?),
                        visibility: Visibility::Visible,
                    };
                    agg_args.push(ScalarItem {
                        index,
                        scalar: d.clone(),
                    });

                    BoundColumnRef {
                        span: d.span(),
                        column: column_binding,
                    }
                    .into()
                };
                Some(Box::new(replaced_default))
            }
        };
        Ok((new_arg, new_default))
    }
}

impl Binder {
    /// Analyze =windows in select clause, this will rewrite window functions.
    /// See [`WindowRewriter`] for more details.
    pub(crate) fn analyze_window(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = WindowRewriter::new(bind_context, self.metadata.clone());
            let new_scalar = rewriter.visit(&item.scalar)?;
            item.scalar = new_scalar;
        }

        Ok(())
    }
}
