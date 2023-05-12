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

use common_constraint::prelude::*;
use common_expression::eval_function;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use ordered_float::OrderedFloat;
use z3::ast::Bool;
use z3::ast::Dynamic;
use z3::ast::Int;
use z3::Config;
use z3::Context;
use z3::SatResult;
use z3::Solver;

use crate::binder::Recursion;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::IndexType;
use crate::ScalarExpr;
use crate::ScalarVisitor;

#[derive(Debug)]
pub struct ConstraintSet {
    constraints: Vec<ScalarExpr>,
}

impl ConstraintSet {
    /// Build a `ConstraintSet` with conjunctions
    pub fn new(constraints: &mut [ScalarExpr]) -> Option<Self> {
        let context = Context::new(&Config::new());

        // Check if all constraints are supported.
        for constraint in constraints.iter_mut() {
            as_z3_ast(&context, constraint)?;
        }

        Some(Self {
            constraints: constraints.to_vec(),
        })
    }

    /// NOTICE: this check is false-positive, which means it may return `false` even
    /// if the variable is null-rejected. But it can ensure not returning `true` for
    /// the variable is not null-rejected.
    ///
    /// Check if the given variable is null-rejected with current constraints.
    /// For example, with a constraint `a > 1`, the variable `a` cannot be null.
    pub fn is_null_reject(&mut self, variable: &IndexType) -> bool {
        if !self
            .constraints
            .iter()
            .any(|scalar| scalar.used_columns().contains(variable))
        {
            // If the variable isn't used by any constraint, then it's unconstrained.
            return false;
        }

        let context = Context::new(&Config::new());
        let variable = Int::new_const(&context, variable.to_string().as_str());

        let z3_asts = self
            .constraints
            .iter_mut()
            .map(|c| as_z3_ast(&context, c))
            .collect::<Option<Vec<Dynamic>>>();

        if let Some(z3_asts) = z3_asts {
            let conjunctions = z3_asts
                .iter()
                .map(|a| is_true(&context, a))
                .collect::<Vec<_>>();
            let proposition = Bool::and(&context, &conjunctions.iter().collect::<Vec<_>>());

            let collector = VariableCollector::new(&context);
            let variables = self
                .constraints
                .iter()
                .fold(collector, |c, s| s.accept(c).unwrap())
                .into_result();

            let solver = Solver::new(&context);
            let result =
                assert_int_is_not_null(&context, &solver, &variables, &variable, &proposition);

            matches!(result, SatResult::Sat)
        } else {
            false
        }
    }
}

/// Transform a logical expression into a z3 ast.
pub fn as_z3_ast<'ctx>(ctx: &'ctx Context, scalar: &mut ScalarExpr) -> Option<Dynamic<'ctx>> {
    transform_logical_expr(ctx, scalar)
}

struct VariableCollector<'ctx> {
    pub context: &'ctx Context,
    pub variables: Vec<Int<'ctx>>,
}

impl<'ctx> VariableCollector<'ctx> {
    pub fn new(context: &'ctx Context) -> Self {
        Self {
            context,
            variables: vec![],
        }
    }

    pub fn into_result(self) -> Vec<Int<'ctx>> {
        self.variables
    }
}

impl<'ctx> ScalarVisitor for VariableCollector<'ctx> {
    fn pre_visit(mut self, scalar: &ScalarExpr) -> common_exception::Result<Recursion<Self>> {
        let result = match scalar {
            ScalarExpr::BoundColumnRef(column) => {
                self.variables.push(Int::new_const(
                    self.context,
                    column.column.index.to_string(),
                ));
                Recursion::Continue(self)
            }
            _ => Recursion::Continue(self),
        };

        Ok(result)
    }
}

/// Transform a logical expression into a z3 ast.
/// Will return a Nullable Boolean ast.
fn transform_logical_expr<'ctx>(
    ctx: &'ctx Context,
    scalar: &mut ScalarExpr,
) -> Option<Dynamic<'ctx>> {
    let result = match scalar {
        ScalarExpr::FunctionCall(func) if func.func_name == "and" => {
            let left = transform_logical_expr(ctx, &mut func.arguments[0])?;
            let right = transform_logical_expr(ctx, &mut func.arguments[1])?;

            and_nullable_bool(ctx, &left, &right)
        }
        ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
            let left = transform_logical_expr(ctx, &mut func.arguments[0])?;
            let right = transform_logical_expr(ctx, &mut func.arguments[1])?;

            or_nullable_bool(ctx, &left, &right)
        }
        ScalarExpr::FunctionCall(func) if func.func_name == "not" => {
            let arg = transform_logical_expr(ctx, &mut func.arguments[0])?;
            not_nullable_bool(ctx, &arg)
        }
        _ => transform_predicate_expr(ctx, scalar)?,
    };

    Some(result)
}

fn transform_predicate_expr<'ctx>(
    ctx: &'ctx Context,
    scalar: &mut ScalarExpr,
) -> Option<Dynamic<'ctx>> {
    tracing::info!("Transforming: {:?}", scalar);
    match scalar {
        ScalarExpr::FunctionCall(func) => {
            if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                (func.arguments[0], func.arguments[1]) =
                    remove_trivial_type_cast(func.arguments[0].clone(), func.arguments[1].clone());
                let left = &func.arguments[0];
                let right = &func.arguments[1];
                match (left, right) {
                    (
                        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }),
                        ScalarExpr::ConstantExpr(ConstantExpr { value, .. }),
                    ) => match value {
                        Scalar::Number(value)
                            if column.data_type.remove_nullable().is_numeric() =>
                        {
                            let int_value =
                                Int::from_i64(ctx, parse_int_literal(&Scalar::Number(*value))?);
                            let left = Int::new_const(ctx, column.index.to_string().as_str());

                            match op {
                                ComparisonOp::Equal => Some(eq_int(ctx, &left, &int_value)),
                                ComparisonOp::NotEqual => Some(ne_int(ctx, &left, &int_value)),
                                ComparisonOp::GT => Some(gt_int(ctx, &left, &int_value)),
                                ComparisonOp::GTE => Some(ge_int(ctx, &left, &int_value)),
                                ComparisonOp::LT => Some(lt_int(ctx, &left, &int_value)),
                                ComparisonOp::LTE => Some(le_int(ctx, &left, &int_value)),
                            }
                        }
                        _ => None,
                    },
                    (
                        ScalarExpr::ConstantExpr(ConstantExpr { value, .. }),
                        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }),
                    ) => match value {
                        Scalar::Number(value) if column.data_type.is_numeric() => {
                            let int_value =
                                Int::from_i64(ctx, parse_int_literal(&Scalar::Number(*value))?);
                            let right = Int::new_const(ctx, column.index.to_string().as_str());

                            match op {
                                ComparisonOp::Equal => Some(eq_int(ctx, &right, &int_value)),
                                ComparisonOp::NotEqual => Some(ne_int(ctx, &right, &int_value)),
                                ComparisonOp::GT => Some(lt_int(ctx, &right, &int_value)),
                                ComparisonOp::GTE => Some(le_int(ctx, &right, &int_value)),
                                ComparisonOp::LT => Some(gt_int(ctx, &right, &int_value)),
                                ComparisonOp::LTE => Some(ge_int(ctx, &right, &int_value)),
                            }
                        }
                        _ => None,
                    },
                    _ => None,
                }
            } else if func.arguments.len() == 1
                && func.arguments[0]
                    .data_type()
                    .ok()?
                    .remove_nullable()
                    .is_numeric()
            {
                if let ScalarExpr::BoundColumnRef(column) = &func.arguments[0] {
                    if func.func_name == "is_null" {
                        Some(from_bool(
                            ctx,
                            &is_null_int(
                                ctx,
                                &Int::new_const(ctx, column.column.index.to_string().as_str()),
                            ),
                        ))
                    } else if func.func_name == "is_not_null" {
                        Some(from_bool(
                            ctx,
                            &is_not_null_int(
                                ctx,
                                &Int::new_const(ctx, column.column.index.to_string().as_str()),
                            ),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Parse a scalar value into a i64 if possible.
/// This is used to parse a constant expression into z3 ast.
fn parse_int_literal(lit: &Scalar) -> Option<i64> {
    let (v, _) = eval_function(
        None,
        "to_int64",
        [(Value::Scalar(lit.clone()), lit.as_ref().infer_data_type())],
        &FunctionContext::default(),
        1,
        &BUILTIN_FUNCTIONS,
    )
    .ok()?;

    v.as_scalar()
        .and_then(|s| s.as_number())
        .and_then(|number| number.as_int64())
        .copied()
}

fn check_trivial_uint_cast(max: u64, value: &Scalar) -> (bool, u64) {
    let value = match *value {
        Scalar::Number(NumberScalar::UInt8(value)) => value as u64,
        Scalar::Number(NumberScalar::UInt16(value)) => value as u64,
        Scalar::Number(NumberScalar::UInt32(value)) => value as u64,
        Scalar::Number(NumberScalar::UInt64(value)) => value,
        _ => return (false, 0),
    };
    (value <= max, value)
}

fn check_trivial_int_cast(min: i64, max: i64, value: &Scalar) -> (bool, i64) {
    let value = match *value {
        Scalar::Number(NumberScalar::Int8(value)) => value as i64,
        Scalar::Number(NumberScalar::Int16(value)) => value as i64,
        Scalar::Number(NumberScalar::Int32(value)) => value as i64,
        Scalar::Number(NumberScalar::Int64(value)) => value,
        _ => return (false, 0),
    };
    (value >= min && value <= max, value)
}

fn check_trivial_float_cast(min: f64, max: f64, value: &Scalar) -> (bool, f64) {
    let value = match *value {
        Scalar::Number(NumberScalar::Float32(value)) => value.into_inner() as f64,
        Scalar::Number(NumberScalar::Float64(value)) => value.into_inner(),
        _ => return (false, 0.0),
    };
    (value >= min && value <= max, value)
}

fn remove_trivial_type_cast(left: ScalarExpr, right: ScalarExpr) -> (ScalarExpr, ScalarExpr) {
    match (&left, &right) {
        (
            ScalarExpr::CastExpr(CastExpr { argument, .. }),
            ScalarExpr::ConstantExpr(ConstantExpr { span, value }),
        )
        | (
            ScalarExpr::ConstantExpr(ConstantExpr { span, value }),
            ScalarExpr::CastExpr(CastExpr { argument, .. }),
        ) => {
            if let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = &(**argument) {
                match *column.data_type {
                    DataType::Number(NumberDataType::UInt8)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt8)) => {
                        let (is_trivial, v) = check_trivial_uint_cast(u8::MAX as u64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt8(v as u8)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::UInt16)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt16)) => {
                        let (is_trivial, v) = check_trivial_uint_cast(u16::MAX as u64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt16(v as u16)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::UInt32)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt32)) => {
                        let (is_trivial, v) = check_trivial_uint_cast(u32::MAX as u64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt32(v as u32)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::UInt64)
                    | DataType::Nullable(box DataType::Number(NumberDataType::UInt64)) => {
                        let (is_trivial, v) = check_trivial_uint_cast(u64::MAX, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::UInt64(v)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int8)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int8)) => {
                        let (is_trivial, v) =
                            check_trivial_int_cast(i8::MIN as i64, i8::MAX as i64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int8(v as i8)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int16)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int16)) => {
                        let (is_trivial, v) =
                            check_trivial_int_cast(i16::MIN as i64, i16::MAX as i64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int16(v as i16)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int32)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int32)) => {
                        let (is_trivial, v) =
                            check_trivial_int_cast(i32::MIN as i64, i32::MAX as i64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int32(v as i32)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Int64)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Int64)) => {
                        let (is_trivial, v) = check_trivial_int_cast(i64::MIN, i64::MAX, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Int64(v)),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Float32)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Float32)) => {
                        let (is_trivial, v) =
                            check_trivial_float_cast(f32::MIN as f64, f32::MAX as f64, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Float32(OrderedFloat(
                                        v as f32,
                                    ))),
                                }),
                            );
                        }
                    }
                    DataType::Number(NumberDataType::Float64)
                    | DataType::Nullable(box DataType::Number(NumberDataType::Float64)) => {
                        let (is_trivial, v) = check_trivial_float_cast(f64::MIN, f64::MAX, value);
                        if is_trivial {
                            return (
                                (**argument).clone(),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: *span,
                                    value: Scalar::Number(NumberScalar::Float64(OrderedFloat(v))),
                                }),
                            );
                        }
                    }
                    _ => (),
                };
                (left, right)
            } else {
                (left, right)
            }
        }
        _ => (left, right),
    }
}
