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
use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;

use crate::BindContext;
use crate::ColumnBindingBuilder;
use crate::binder::JoinPredicate;
use crate::binder::Visibility;
use crate::binder::bind_window_function_info;
use crate::binder::window::WindowRewriter;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SExpr;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::LagLeadFunction;
use crate::plans::ScalarExpr;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;

const GT: &str = "gt";
const GTE: &str = "gte";
const LT: &str = "lt";
const LTE: &str = "lte";

impl Binder {
    pub(super) fn rewrite_asof(
        &self,
        mut join: Join,
        left: SExpr,
        (right, right_context): (SExpr, &mut BindContext),
    ) -> Result<SExpr> {
        let left_prop = left.derive_relational_prop()?;
        let right_prop = right.derive_relational_prop()?;

        let mut range_condition = None;
        for condition in join.non_equi_conditions.iter() {
            if let Some(func) = is_range_join_condition(condition, &left_prop, &right_prop) {
                if range_condition.is_some() {
                    return Err(ErrorCode::Internal("Multiple inequalities condition!"));
                }
                range_condition = Some(func);
            }
        }
        let Some(range_func) = range_condition else {
            return Err(ErrorCode::Internal("Missing inequality condition!"));
        };

        let (window_func, right_column) =
            self.create_window_func(&join, &left_prop, &right_prop, range_func)?;

        let mut rewriter = WindowRewriter::new(right_context, self.metadata.clone());
        let window_func = rewriter.replace_window_function(&window_func)?;

        let window_info = right_context
            .windows
            .get_window_info(&window_func.display_name)
            .unwrap();

        for condition in join.equi_conditions.iter_mut() {
            std::mem::swap(&mut condition.left, &mut condition.right)
        }

        let func_name = match range_func.func_name.as_str() {
            GTE => LT,
            GT => LTE,
            LT => GTE,
            LTE => GT,
            _ => unreachable!(),
        };
        let span = right_column.span();
        let lead_column = BoundColumnRef {
            span,
            column: ColumnBindingBuilder::new(
                window_func.display_name.clone(),
                window_info.index,
                Box::new(window_func.func.return_type()),
                Visibility::Visible,
            )
            .build(),
        }
        .into();
        join.non_equi_conditions
            .push(make_asof_interval_end_condition(
                range_func.span,
                right_column,
                lead_column,
                func_name,
            ));

        let window_plan = bind_window_function_info(&self.ctx, window_info, right)?;
        Ok(SExpr::create_binary(
            Arc::new(join.into()),
            Arc::new(window_plan),
            Arc::new(left),
        ))
    }

    fn create_window_func(
        &self,
        join: &Join,
        left_prop: &RelationalProperty,
        right_prop: &RelationalProperty,
        range_func: &FunctionCall,
    ) -> Result<(WindowFunc, ScalarExpr)> {
        let (left_column, right_column) = {
            match range_func.arguments.as_slice() {
                [right, left]
                    if right.used_columns().is_subset(&right_prop.output_columns)
                        && left.used_columns().is_subset(&left_prop.output_columns) =>
                {
                    (right.clone(), left.clone())
                }
                [left, right]
                    if right.used_columns().is_subset(&right_prop.output_columns)
                        && left.used_columns().is_subset(&left_prop.output_columns) =>
                {
                    (right.clone(), left.clone())
                }
                _ => unreachable!(),
            }
        };

        let asc = match range_func.func_name.as_str() {
            GT | GTE => true,
            LT | LTE => false,
            _ => unreachable!(),
        };

        let order_items = vec![WindowOrderBy {
            expr: left_column.clone(),
            asc: Some(asc),
            nulls_first: Some(true),
        }];

        let mut partition_items: Vec<_> = Vec::with_capacity(join.equi_conditions.len());
        for condition in join.equi_conditions.iter() {
            partition_items.push(condition.right.clone());
        }

        let return_type = asof_window_result_type(&left_column.data_type()?);
        let func_type = WindowFuncType::LagLead(LagLeadFunction {
            is_lag: false,
            return_type: Box::new(return_type),
            arg: Box::new(left_column),
            offset: 1,
            default: None,
        });

        let window_func = WindowFunc {
            span: range_func.span,
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
        Ok((window_func, right_column))
    }
}

fn asof_window_result_type(
    data_type: &databend_common_expression::types::DataType,
) -> databend_common_expression::types::DataType {
    data_type.wrap_nullable()
}

fn make_asof_interval_end_condition(
    span: databend_common_ast::Span,
    probe_key: ScalarExpr,
    lead_key: ScalarExpr,
    func_name: &str,
) -> ScalarExpr {
    let compare = ScalarExpr::FunctionCall(FunctionCall {
        span,
        func_name: func_name.to_string(),
        params: vec![],
        arguments: vec![probe_key, lead_key.clone()],
    });

    ScalarExpr::FunctionCall(FunctionCall {
        span,
        func_name: "if".to_string(),
        params: vec![],
        arguments: vec![
            ScalarExpr::FunctionCall(FunctionCall {
                span,
                func_name: "is_not_null".to_string(),
                params: vec![],
                arguments: vec![lead_key],
            }),
            compare,
            ScalarExpr::ConstantExpr(ConstantExpr {
                span,
                value: Scalar::Boolean(true),
            }),
        ],
    })
}

pub fn is_range_join_condition<'a>(
    expr: &'a ScalarExpr,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> Option<&'a FunctionCall> {
    let ScalarExpr::FunctionCall(func) = expr else {
        return None;
    };
    if !matches!(func.func_name.as_str(), GT | LT | GTE | LTE) {
        return None;
    }
    let [a, b] = func.arguments.as_slice() else {
        unreachable!()
    };

    match JoinPredicate::new(a, left_prop, right_prop) {
        JoinPredicate::Left(_)
            if matches!(
                JoinPredicate::new(b, left_prop, right_prop),
                JoinPredicate::Right(_)
            ) =>
        {
            Some(func)
        }
        JoinPredicate::Right(_)
            if matches!(
                JoinPredicate::new(b, left_prop, right_prop),
                JoinPredicate::Left(_)
            ) =>
        {
            Some(func)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::Symbol;

    fn test_column(name: &str, index: usize, data_type: DataType) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                name.to_string(),
                Symbol::from_field_index(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    #[test]
    fn test_asof_interval_end_condition_guards_open_tail_with_null_lead() {
        let probe = test_column("probe", 0, DataType::Number(NumberDataType::UInt8));
        let lead = test_column(
            "lead",
            1,
            DataType::Number(NumberDataType::UInt8).wrap_nullable(),
        );

        let expr = make_asof_interval_end_condition(None, probe.clone(), lead.clone(), LT);
        let ScalarExpr::FunctionCall(func) = expr else {
            panic!("expected function call");
        };

        assert_eq!(func.func_name, "if");
        assert_eq!(func.arguments.len(), 3);

        let ScalarExpr::FunctionCall(not_null) = &func.arguments[0] else {
            panic!("expected is_not_null guard");
        };
        assert_eq!(not_null.func_name, "is_not_null");
        assert_eq!(not_null.arguments, vec![lead.clone()]);

        let ScalarExpr::FunctionCall(compare) = &func.arguments[1] else {
            panic!("expected comparison branch");
        };
        assert_eq!(compare.func_name, LT);
        assert_eq!(compare.arguments, vec![probe, lead]);

        let ScalarExpr::ConstantExpr(constant) = &func.arguments[2] else {
            panic!("expected constant true branch");
        };
        assert_eq!(constant.value, Scalar::Boolean(true));
    }

    #[test]
    fn test_asof_window_result_type_is_nullable() {
        let data_type = DataType::Number(NumberDataType::UInt8);
        assert_eq!(
            asof_window_result_type(&data_type),
            data_type.wrap_nullable()
        );
    }
}
