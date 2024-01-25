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

use databend_common_constraint::mir::MirBinaryOperator;
use databend_common_constraint::mir::MirConstant;
use databend_common_constraint::mir::MirDataType;
use databend_common_constraint::mir::MirExpr;
use databend_common_constraint::mir::MirUnaryOperator;
use databend_common_constraint::problem::variable_must_not_null;
use databend_common_expression::cast_scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::plans::FunctionCall;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Debug)]
pub struct ConstraintSet {
    constraints: Vec<(ScalarExpr, MirExpr)>,
    #[allow(dead_code)]
    unsupported_constraints: Vec<ScalarExpr>,
}

impl ConstraintSet {
    /// Build a `ConstraintSet` with conjunctions
    pub fn new(constraints: &[ScalarExpr]) -> Self {
        let mut supported_constraints = Vec::new();
        let mut unsupported_constraints = Vec::new();

        for constraint in constraints {
            let mir_expr = as_mir(constraint);
            if let Some(mir_expr) = mir_expr {
                supported_constraints.push((constraint.clone(), mir_expr));
            } else {
                unsupported_constraints.push(constraint.clone());
            }
        }

        Self {
            constraints: supported_constraints,
            unsupported_constraints,
        }
    }

    /// Check if the given variable is null-rejected with current constraints.
    /// For example, with a constraint `a > 1`, the variable `a` must not be null.
    ///
    /// NOTICE: this check is false-positive, which means it may return `false` even
    /// if the variable is null-rejected. But it can ensure not returning `true` for
    /// the variable is not null-rejected.
    pub fn is_null_reject(&self, variable: &IndexType) -> bool {
        if !self
            .constraints
            .iter()
            .any(|(scalar, _)| scalar.used_columns().contains(variable))
        {
            // The variable isn't used by any constraint, therefore it's unconstrained.
            return false;
        }

        let conjunctions = self
            .constraints
            .iter()
            .map(|(_, mir)| mir.clone())
            .reduce(|left, right| MirExpr::BinaryOperator {
                op: MirBinaryOperator::And,
                left: Box::new(left),
                right: Box::new(right),
            })
            .unwrap();

        variable_must_not_null(&conjunctions, &variable.to_string())
    }
}

/// Transform a logical expression into a MIR expression.
pub fn as_mir(scalar: &ScalarExpr) -> Option<MirExpr> {
    match scalar {
        ScalarExpr::FunctionCall(func) => {
            if let Some(unary_op) = match func.func_name.as_str() {
                "minus" => Some(MirUnaryOperator::Minus),
                "not" => Some(MirUnaryOperator::Not),
                "is_null" => Some(MirUnaryOperator::IsNull),
                "is_not_null" => {
                    return Some(MirExpr::UnaryOperator {
                        op: MirUnaryOperator::Not,
                        arg: Box::new(as_mir(&ScalarExpr::FunctionCall(FunctionCall {
                            func_name: "is_null".to_string(),
                            ..func.clone()
                        }))?),
                    });
                }
                _ => None,
            } {
                let arg = as_mir(&func.arguments[0])?;
                return Some(MirExpr::UnaryOperator {
                    op: unary_op,
                    arg: Box::new(arg),
                });
            }

            if let Some(binary_op) = match func.func_name.as_str() {
                "plus" => Some(MirBinaryOperator::Plus),
                "minus" => Some(MirBinaryOperator::Minus),
                "multiply" => Some(MirBinaryOperator::Multiply),
                "and" => Some(MirBinaryOperator::And),
                "or" => Some(MirBinaryOperator::Or),
                "lt" => Some(MirBinaryOperator::Lt),
                "lte" => Some(MirBinaryOperator::Lte),
                "gt" => Some(MirBinaryOperator::Gt),
                "gte" => Some(MirBinaryOperator::Gte),
                "eq" => Some(MirBinaryOperator::Eq),
                "noteq" => {
                    return Some(MirExpr::UnaryOperator {
                        op: MirUnaryOperator::Not,
                        arg: Box::new(as_mir(&ScalarExpr::FunctionCall(FunctionCall {
                            func_name: "eq".to_string(),
                            ..func.clone()
                        }))?),
                    });
                }
                _ => None,
            } {
                let left = as_mir(&func.arguments[0])?;
                let right = as_mir(&func.arguments[1])?;
                return Some(MirExpr::BinaryOperator {
                    op: binary_op,
                    left: Box::new(left),
                    right: Box::new(right),
                });
            }

            None
        }
        ScalarExpr::ConstantExpr(constant) => {
            let value = match &constant.value {
                Scalar::Number(scalar) if scalar.data_type().is_integer() => {
                    MirConstant::Int(parse_int_literal(*scalar)?)
                }
                Scalar::Boolean(value) => MirConstant::Bool(*value),
                Scalar::Null => MirConstant::Null,
                Scalar::Timestamp(value) => MirConstant::Int(*value),
                _ => return None,
            };
            Some(MirExpr::Constant(value))
        }
        ScalarExpr::BoundColumnRef(column_ref) => {
            let name = column_ref.column.index.to_string();
            let data_type = match column_ref.column.data_type.remove_nullable() {
                DataType::Boolean => MirDataType::Bool,
                DataType::Number(num_ty) if num_ty.is_integer() => MirDataType::Int,
                DataType::Timestamp => MirDataType::Int,
                _ => return None,
            };
            Some(MirExpr::Variable { name, data_type })
        }
        _ => None,
    }
}

/// Parse a scalar value into a i64 if possible.
/// This is used to parse a constant expression into z3 ast.
fn parse_int_literal(lit: NumberScalar) -> Option<i64> {
    Some(
        cast_scalar(
            None,
            Scalar::Number(lit),
            DataType::Number(NumberDataType::Int64),
            &BUILTIN_FUNCTIONS,
        )
        .ok()?
        .into_number()
        .unwrap()
        .into_int64()
        .unwrap(),
    )
}
