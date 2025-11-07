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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;

use crate::binder::bind_window_function_info;
use crate::binder::window::WindowRewriter;
use crate::binder::JoinPredicate;
use crate::binder::Visibility;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SExpr;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::LagLeadFunction;
use crate::plans::ScalarExpr;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::BindContext;
use crate::ColumnBindingBuilder;

impl Binder {
    pub(super) fn rewrite_asof(&self, mut join: Join, left: SExpr, right: SExpr) -> Result<SExpr> {
        let left_prop = left.derive_relational_prop()?;
        let right_prop = right.derive_relational_prop()?;

        let mut range_condition = None;
        for condition in join.non_equi_conditions.iter() {
            if check_range_condition(condition, &left_prop, &right_prop) {
                if range_condition.is_none() {
                    range_condition = Some(condition);
                } else {
                    return Err(ErrorCode::Internal("Multiple inequalities condition!"));
                }
            }
        }
        let range_condition = match range_condition {
            Some(range_condition) => range_condition,
            None => {
                return Err(ErrorCode::Internal("Missing inequality condition!"));
            }
        };

        let (window, right_column, left_type) =
            self.bind_window_func(&join, &left, &right, range_condition)?;

        let mut bind_ctx = BindContext::new();
        let mut rewriter = WindowRewriter::new(&mut bind_ctx, self.metadata.clone());

        let window_func = rewriter.replace_window_function(&window)?;
        let window_info = bind_ctx
            .windows
            .get_window_info(&window_func.display_name)
            .unwrap();

        let folded_args = [
            right_column.clone(),
            BoundColumnRef {
                span: right_column.span(),
                column: ColumnBindingBuilder::new(
                    window_func.display_name.clone(),
                    window_info.index,
                    Box::new(left_type),
                    Visibility::Visible,
                )
                .build(),
            }
            .into(),
        ];

        for condition in join.equi_conditions.iter_mut() {
            std::mem::swap(&mut condition.left, &mut condition.right)
        }

        if let ScalarExpr::FunctionCall(func) = range_condition.clone() {
            let func_name =
                match ComparisonOp::try_from_func_name(func.func_name.as_str()).unwrap() {
                    ComparisonOp::GTE => "lt",
                    ComparisonOp::GT => "lte",
                    ComparisonOp::LT => "gte",
                    ComparisonOp::LTE => "gt",
                    _ => unreachable!("must be range condition!"),
                }
                .to_string();
            join.non_equi_conditions.push(
                FunctionCall {
                    span: range_condition.span(),
                    params: vec![],
                    arguments: folded_args.to_vec(),
                    func_name,
                }
                .into(),
            );
        } else {
            let [left, right] = folded_args;
            join.equi_conditions.push(JoinEquiCondition {
                left,
                right,
                is_null_equal: false,
            });
        };

        let window_plan = bind_window_function_info(&self.ctx, window_info, right)?;
        Ok(SExpr::create_binary(
            Arc::new(join.into()),
            Arc::new(window_plan),
            Arc::new(left),
        ))
    }

    fn bind_window_func(
        &self,
        join: &Join,
        left: &SExpr,
        right: &SExpr,
        range_condition: &ScalarExpr,
    ) -> Result<(WindowFunc, ScalarExpr, DataType)> {
        let right_prop = RelExpr::with_s_expr(left).derive_relational_prop()?;
        let left_prop = RelExpr::with_s_expr(right).derive_relational_prop()?;

        let mut right_column = range_condition.clone();
        let mut left_column = range_condition.clone();
        let mut order_items: Vec<WindowOrderBy> = Vec::with_capacity(1);
        let mut constant_default = ConstantExpr {
            span: right_column.span(),
            value: Scalar::Null,
        };
        if let ScalarExpr::FunctionCall(func) = range_condition
            && func.arguments.len() == 2
        {
            for arg in func.arguments.iter() {
                if let ScalarExpr::BoundColumnRef(_) = arg {
                    let asc =
                        match ComparisonOp::try_from_func_name(func.func_name.as_str()).unwrap() {
                            ComparisonOp::GT | ComparisonOp::GTE => Ok(Some(true)),
                            ComparisonOp::LT | ComparisonOp::LTE => Ok(Some(false)),
                            _ => Err(ErrorCode::Internal("must be range condition!")),
                        }?;
                    if arg.used_columns().is_subset(&left_prop.output_columns) {
                        left_column = arg.clone();
                        constant_default.span = left_column.span();
                        constant_default.value = left_column
                            .data_type()?
                            .remove_nullable()
                            .infinity()
                            .unwrap();
                        if asc == Some(false) {
                            constant_default.value = left_column
                                .data_type()?
                                .remove_nullable()
                                .ninfinity()
                                .unwrap();
                        }
                        order_items.push(WindowOrderBy {
                            expr: arg.clone(),
                            asc,
                            nulls_first: Some(true),
                        });
                    }
                    if arg.used_columns().is_subset(&right_prop.output_columns) {
                        right_column = arg.clone();
                    }
                } else {
                    return Err(ErrorCode::Internal(
                        "Cannot downcast Scalar to BoundColumnRef",
                    ));
                }
            }
        }

        let mut partition_items: Vec<ScalarExpr> = Vec::with_capacity(join.equi_conditions.len());
        for condition in join.equi_conditions.iter() {
            if matches!(condition.right, ScalarExpr::BoundColumnRef(_))
                && matches!(condition.left, ScalarExpr::BoundColumnRef(_))
            {
                partition_items.push(condition.right.clone());
            } else {
                return Err(ErrorCode::Internal(
                    "Cannot downcast Scalar to BoundColumnRef",
                ));
            }
        }
        let func_type = WindowFuncType::LagLead(LagLeadFunction {
            is_lag: false,
            arg: Box::new(left_column.clone()),
            offset: 1,
            default: Some(Box::new(constant_default.into())),
            return_type: Box::new(left_column.data_type()?.clone()),
        });

        let window_func = WindowFunc {
            span: range_condition.span(),
            display_name: func_type.func_name(),
            partition_by: partition_items,
            func: func_type,
            order_by: order_items,
            frame: WindowFuncFrame {
                units: WindowFuncFrameUnits::Rows,
                start_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                    NumberScalar::UInt64(1),
                ))),
                end_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                    NumberScalar::UInt64(1),
                ))),
            },
        };
        Ok((window_func, right_column, left_column.data_type()?.clone()))
    }
}

fn check_range_condition(
    expr: &ScalarExpr,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> bool {
    let ScalarExpr::FunctionCall(FunctionCall {
        func_name,
        arguments,
        ..
    }) = expr
    else {
        return false;
    };
    if !matches!(func_name.as_str(), "gt" | "lt" | "gte" | "lte") {
        return false;
    }
    let [left, right] = arguments.as_slice() else {
        unreachable!()
    };

    matches!(
        (
            JoinPredicate::new(left, left_prop, right_prop),
            JoinPredicate::new(right, left_prop, right_prop),
        ),
        (JoinPredicate::Left(_), JoinPredicate::Right(_))
    )
}
