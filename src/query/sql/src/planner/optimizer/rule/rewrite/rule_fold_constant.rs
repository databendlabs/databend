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
use std::sync::Arc;

use common_exception::Result;
use common_expression::types::decimal::DecimalScalar;
use common_expression::types::decimal::DecimalSize;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberScalar;
use common_expression::ConstantFolder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_functions::BUILTIN_FUNCTIONS;
use ethnum::i256;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Exchange;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::TypeCheck;

pub struct RuleFoldConstant {
    id: RuleID,
    patterns: Vec<SExpr>,
    func_ctx: FunctionContext,
}

impl RuleFoldConstant {
    pub fn new(func_ctx: FunctionContext) -> Self {
        Self {
            id: RuleID::FoldConstant,
            patterns: vec![
                // Filter
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Filter,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // EvalScalar
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::EvalScalar,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // ProjectSet
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::ProjectSet,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // Exchange
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Exchange,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // Join
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Join,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // Window
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Window,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
            ],
            func_ctx,
        }
    }

    fn fold_constant(&self, scalar: &mut ScalarExpr) -> Result<()> {
        if scalar.used_columns().is_empty() && !scalar.as_constant_expr().is_some() {
            let expr = scalar.resolve_and_check(&HashMap::new())?;
            let (new_expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            if let Expr::Constant {
                span,
                scalar: value,
                ..
            } = new_expr
            {
                *scalar = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
            };
        }

        match scalar {
            ScalarExpr::FunctionCall(func) => {
                for arg in func.arguments.iter_mut() {
                    self.fold_constant(arg)?
                }
            }
            ScalarExpr::CastExpr(cast) => self.fold_constant(&mut cast.argument)?,
            ScalarExpr::ConstantExpr(constant) => {
                constant.value = Self::shrink_constant(constant.value.clone())
            }
            _ => (),
        }

        Ok(())
    }

    fn shrink_constant(scalar: Scalar) -> Scalar {
        match scalar {
            Scalar::Number(NumberScalar::UInt8(n)) => Self::shrink_u64(n as u64),
            Scalar::Number(NumberScalar::UInt16(n)) => Self::shrink_u64(n as u64),
            Scalar::Number(NumberScalar::UInt32(n)) => Self::shrink_u64(n as u64),
            Scalar::Number(NumberScalar::UInt64(n)) => Self::shrink_u64(n),
            Scalar::Number(NumberScalar::Int8(n)) => Self::shrink_i64(n as i64),
            Scalar::Number(NumberScalar::Int16(n)) => Self::shrink_i64(n as i64),
            Scalar::Number(NumberScalar::Int32(n)) => Self::shrink_i64(n as i64),
            Scalar::Number(NumberScalar::Int64(n)) => Self::shrink_i64(n),
            Scalar::Decimal(DecimalScalar::Decimal128(d, size)) => {
                Self::shrink_d256(d.into(), size)
            }
            Scalar::Decimal(DecimalScalar::Decimal256(d, size)) => Self::shrink_d256(d, size),
            Scalar::Tuple(mut fields) => {
                for field in fields.iter_mut() {
                    *field = Self::shrink_constant(field.clone());
                }
                Scalar::Tuple(fields)
            }
            _ => scalar,
        }
    }

    fn shrink_u64(num: u64) -> Scalar {
        if num < u8::MAX as u64 {
            Scalar::Number(NumberScalar::UInt8(num as u8))
        } else if num < u16::MAX as u64 {
            Scalar::Number(NumberScalar::UInt16(num as u16))
        } else if num < u32::MAX as u64 {
            Scalar::Number(NumberScalar::UInt32(num as u32))
        } else {
            Scalar::Number(NumberScalar::UInt64(num))
        }
    }

    fn shrink_i64(num: i64) -> Scalar {
        if num > 0 {
            return Self::shrink_u64(num as u64);
        }

        if num < i8::MAX as i64 && num > i8::MIN as i64 {
            Scalar::Number(NumberScalar::Int8(num as i8))
        } else if num < i16::MAX as i64 && num > i16::MIN as i64 {
            Scalar::Number(NumberScalar::Int16(num as i16))
        } else if num < i32::MAX as i64 && num > i32::MIN as i64 {
            Scalar::Number(NumberScalar::Int32(num as i32))
        } else {
            Scalar::Number(NumberScalar::Int64(num))
        }
    }

    fn shrink_d256(decimal: i256, size: DecimalSize) -> Scalar {
        if size.scale == 0 {
            if decimal.is_positive() && decimal <= i256::from(u64::MAX) {
                return Self::shrink_u64(decimal.try_into().unwrap());
            } else if decimal <= i256::from(i64::MAX) && decimal >= i256::from(i64::MIN) {
                return Self::shrink_i64(decimal.try_into().unwrap());
            }
        }

        let valid_bits = 256 - decimal.leading_zeros();
        let log10_2 = std::f64::consts::LOG10_2;
        let precision_f64 = (valid_bits as f64) * log10_2;
        let precision = precision_f64.ceil() as u8;
        let size = DecimalSize { precision, ..size };
        let decimal_ty = DecimalDataType::from_size(size).unwrap();

        match decimal_ty {
            DecimalDataType::Decimal128(size) => {
                Scalar::Decimal(DecimalScalar::Decimal128(decimal.try_into().unwrap(), size))
            }
            DecimalDataType::Decimal256(size) => {
                Scalar::Decimal(DecimalScalar::Decimal256(decimal, size))
            }
        }
    }
}

impl Rule for RuleFoldConstant {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let mut new_plan = s_expr.plan().clone();
        match &mut new_plan {
            RelOperator::EvalScalar(eval_scalar) => {
                for scalar in eval_scalar.items.iter_mut() {
                    self.fold_constant(&mut scalar.scalar)?;
                }
            }
            RelOperator::Filter(filter) => {
                for predicate in filter.predicates.iter_mut() {
                    self.fold_constant(predicate)?;
                }
            }
            RelOperator::ProjectSet(project_set) => {
                for srf in project_set.srfs.iter_mut() {
                    self.fold_constant(&mut srf.scalar)?;
                }
            }
            RelOperator::Exchange(Exchange::Hash(scalars)) => {
                for scalar in scalars.iter_mut() {
                    self.fold_constant(scalar)?;
                }
            }
            RelOperator::Join(join) => {
                for cond in join.left_conditions.iter_mut() {
                    self.fold_constant(cond)?;
                }
                for cond in join.right_conditions.iter_mut() {
                    self.fold_constant(cond)?;
                }
                for cond in join.non_equi_conditions.iter_mut() {
                    self.fold_constant(cond)?;
                }
            }
            RelOperator::Window(window) => {
                for arg in window.arguments.iter_mut() {
                    self.fold_constant(&mut arg.scalar)?;
                }
                for part in window.partition_by.iter_mut() {
                    self.fold_constant(&mut part.scalar)?;
                }
                for o in window.order_by.iter_mut() {
                    self.fold_constant(&mut o.order_by_item.scalar)?;
                }
            }
            _ => unreachable!(),
        }
        if &new_plan != s_expr.plan() {
            state.add_result(s_expr.replace_plan(Arc::new(new_plan)));
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
