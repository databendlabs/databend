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
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::Scalar;
use ordered_float::OrderedFloat;

use crate::optimizer::constraint::check_float_range;
use crate::optimizer::constraint::check_int_range;
use crate::optimizer::constraint::check_uint_range;
use crate::optimizer::constraint::remove_trivial_type_cast;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

// The rule tries to apply the inverse OR distributive law to the predicate.
// ((A AND B) OR (A AND C))  =>  (A AND (B OR C))
// It'll find all OR expressions and extract the common terms.
pub struct RuleInferFilter {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleInferFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::InferFilter,
            // Filter
            //  \
            //   *
            patterns: vec![SExpr::create_unary(
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
            )],
        }
    }
}

pub struct Predicate {
    op: ComparisonOp,
    constant: ConstantExpr,
}

pub struct PredicateSet {
    predicates: HashMap<ScalarExpr, Vec<Predicate>>,
    is_merged: bool,
    is_falsy: bool,
}

enum MergeResult {
    All,
    Left,
    Right,
    None,
}

impl PredicateSet {
    fn into_predicates(self) -> Vec<ScalarExpr> {
        let mut result = vec![];
        for (scalar, predicates) in self.predicates.into_iter() {
            for predicate in predicates.into_iter() {
                result.push(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: String::from(predicate.op.to_func_name()),
                    params: vec![],
                    arguments: vec![scalar.clone(), ScalarExpr::ConstantExpr(predicate.constant)],
                }));
            }
        }
        result
    }

    fn merge_predicate(&mut self, left: &ScalarExpr, right: Predicate) {
        match self.predicates.get_mut(left) {
            Some(predicates) => {
                for predicate in predicates.iter_mut() {
                    match Self::merge(predicate, &right) {
                        MergeResult::None => {
                            self.is_falsy = true;
                            self.is_merged = true;
                            return;
                        }
                        MergeResult::Left => {
                            self.is_merged = true;
                            return;
                        }
                        MergeResult::Right => {
                            *predicate = right;
                            self.is_merged = true;
                            return;
                        }
                        MergeResult::All => (),
                    }
                }
                predicates.push(right);
            }
            None => {
                self.predicates.insert(left.clone(), vec![right]);
            }
        };
    }

    fn merge(left: &Predicate, right: &Predicate) -> MergeResult {
        match left.op {
            ComparisonOp::Equal => match right.op {
                ComparisonOp::Equal => match left.constant == right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::None,
                },
                ComparisonOp::NotEqual => match left.constant == right.constant {
                    true => MergeResult::None,
                    false => MergeResult::Left,
                },
                ComparisonOp::LT => match left.constant < right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::None,
                },
                ComparisonOp::LTE => match left.constant <= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::None,
                },
                ComparisonOp::GT => match left.constant > right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::None,
                },
                ComparisonOp::GTE => match left.constant >= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::None,
                },
            },
            ComparisonOp::NotEqual => match right.op {
                ComparisonOp::Equal => match left.constant == right.constant {
                    true => MergeResult::None,
                    false => MergeResult::Right,
                },
                ComparisonOp::NotEqual => match left.constant == right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::All,
                },
                ComparisonOp::LT => match left.constant >= right.constant {
                    true => MergeResult::Right,
                    false => MergeResult::All,
                },
                ComparisonOp::LTE => match left.constant > right.constant {
                    true => MergeResult::Right,
                    false => MergeResult::All,
                },
                ComparisonOp::GT => match left.constant <= right.constant {
                    true => MergeResult::Right,
                    false => MergeResult::All,
                },
                ComparisonOp::GTE => match left.constant < right.constant {
                    true => MergeResult::Right,
                    false => MergeResult::All,
                },
            },
            ComparisonOp::LT => match right.op {
                ComparisonOp::Equal => match left.constant <= right.constant {
                    true => MergeResult::None,
                    false => MergeResult::Right,
                },
                ComparisonOp::NotEqual => match left.constant <= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::All,
                },
                ComparisonOp::LT | ComparisonOp::LTE => match left.constant <= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::Right,
                },
                ComparisonOp::GT | ComparisonOp::GTE => match left.constant <= right.constant {
                    true => MergeResult::None,
                    false => MergeResult::All,
                },
            },
            ComparisonOp::LTE => match right.op {
                ComparisonOp::Equal => match left.constant < right.constant {
                    true => MergeResult::None,
                    false => MergeResult::Right,
                },
                ComparisonOp::NotEqual => match left.constant < right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::All,
                },
                ComparisonOp::LT => match left.constant < right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::Right,
                },
                ComparisonOp::LTE => match left.constant <= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::Right,
                },
                ComparisonOp::GT => match left.constant <= right.constant {
                    true => MergeResult::None,
                    false => MergeResult::All,
                },
                ComparisonOp::GTE => match left.constant < right.constant {
                    true => MergeResult::None,
                    false => MergeResult::All,
                },
            },
            ComparisonOp::GT => match right.op {
                ComparisonOp::Equal => match left.constant >= right.constant {
                    true => MergeResult::None,
                    false => MergeResult::Right,
                },
                ComparisonOp::NotEqual => match left.constant >= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::All,
                },
                ComparisonOp::LT | ComparisonOp::LTE => match left.constant >= right.constant {
                    true => MergeResult::None,
                    false => MergeResult::All,
                },
                ComparisonOp::GT | ComparisonOp::GTE => match left.constant >= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::Right,
                },
            },
            ComparisonOp::GTE => match right.op {
                ComparisonOp::Equal => match left.constant > right.constant {
                    true => MergeResult::None,
                    false => MergeResult::Right,
                },
                ComparisonOp::NotEqual => match left.constant > right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::All,
                },
                ComparisonOp::LT => match left.constant >= right.constant {
                    true => MergeResult::None,
                    false => MergeResult::All,
                },
                ComparisonOp::LTE => match left.constant > right.constant {
                    true => MergeResult::None,
                    false => MergeResult::All,
                },
                ComparisonOp::GT => match left.constant > right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::Right,
                },
                ComparisonOp::GTE => match left.constant >= right.constant {
                    true => MergeResult::Left,
                    false => MergeResult::Right,
                },
            },
        }
    }
}

pub fn adjust_scalar(scalar: Scalar, data_type: DataType) -> (bool, ConstantExpr) {
    match data_type {
        DataType::Number(NumberDataType::UInt8)
        | DataType::Nullable(box DataType::Number(NumberDataType::UInt8)) => {
            let (ok, v) = check_uint_range(u8::MAX as u64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt8(v as u8)),
                });
            }
        }
        DataType::Number(NumberDataType::UInt16)
        | DataType::Nullable(box DataType::Number(NumberDataType::UInt16)) => {
            let (ok, v) = check_uint_range(u16::MAX as u64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt16(v as u16)),
                });
            }
        }
        DataType::Number(NumberDataType::UInt32)
        | DataType::Nullable(box DataType::Number(NumberDataType::UInt32)) => {
            let (ok, v) = check_uint_range(u32::MAX as u64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt32(v as u32)),
                });
            }
        }
        DataType::Number(NumberDataType::UInt64)
        | DataType::Nullable(box DataType::Number(NumberDataType::UInt64)) => {
            let (ok, v) = check_uint_range(u64::MAX, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt64(v)),
                });
            }
        }
        DataType::Number(NumberDataType::Int8)
        | DataType::Nullable(box DataType::Number(NumberDataType::Int8)) => {
            let (ok, v) = check_int_range(i8::MIN as i64, i8::MAX as i64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int8(v as i8)),
                });
            }
        }
        DataType::Number(NumberDataType::Int16)
        | DataType::Nullable(box DataType::Number(NumberDataType::Int16)) => {
            let (ok, v) = check_int_range(i16::MIN as i64, i16::MAX as i64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int16(v as i16)),
                });
            }
        }
        DataType::Number(NumberDataType::Int32)
        | DataType::Nullable(box DataType::Number(NumberDataType::Int32)) => {
            let (ok, v) = check_int_range(i32::MIN as i64, i32::MAX as i64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int32(v as i32)),
                });
            }
        }
        DataType::Number(NumberDataType::Int64)
        | DataType::Nullable(box DataType::Number(NumberDataType::Int64)) => {
            let (ok, v) = check_int_range(i64::MIN, i64::MAX, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int64(v)),
                });
            }
        }
        DataType::Number(NumberDataType::Float32)
        | DataType::Nullable(box DataType::Number(NumberDataType::Float32)) => {
            let (ok, v) =
                check_float_range(f32::MIN as f64, f32::MAX as f64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Float32(OrderedFloat(v as f32))),
                });
            }
        }
        DataType::Number(NumberDataType::Float64)
        | DataType::Nullable(box DataType::Number(NumberDataType::Float64)) => {
            let (ok, v) = check_float_range(f64::MIN, f64::MAX, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Float64(OrderedFloat(v))),
                });
            }
        }
        _ => (),
    }
    (false, ConstantExpr {
        span: None,
        value: scalar,
    })
}

impl Rule for RuleInferFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut predicates = filter.predicates;
        let mut new_predicates = vec![];
        let mut is_rewritten = false;
        let mut predicate_set = PredicateSet {
            predicates: HashMap::new(),
            is_merged: false,
            is_falsy: false,
        };
        for predicate in predicates.iter_mut() {
            if let ScalarExpr::FunctionCall(func) = predicate {
                if ComparisonOp::try_from_func_name(&func.func_name).is_some() {
                    let (left, right) = remove_trivial_type_cast(
                        func.arguments[0].clone(),
                        func.arguments[1].clone(),
                    );
                    if left != func.arguments[0] {
                        is_rewritten = true;
                        func.arguments[0] = left;
                    }
                    if right != func.arguments[1] {
                        is_rewritten = true;
                        func.arguments[1] = right;
                    }
                }
            }
        }
        for predicate in predicates.into_iter() {
            if let ScalarExpr::FunctionCall(func) = &predicate {
                if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    match (
                        func.arguments[0].is_column_ref(),
                        func.arguments[1].is_column_ref(),
                    ) {
                        (true, true) => {
                            // TODO(Dousir9): infer column ref equal predicate.
                            new_predicates.push(predicate);
                        }
                        (true, false) => {
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[1] {
                                let (is_adjusted, constant) = adjust_scalar(
                                    constant.value.clone(),
                                    func.arguments[0].data_type()?,
                                );
                                if is_adjusted {
                                    predicate_set.merge_predicate(&func.arguments[0], Predicate {
                                        op,
                                        constant,
                                    });
                                } else {
                                    new_predicates.push(predicate);
                                }
                            }
                        }
                        (false, true) => {
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[0] {
                                let (is_adjusted, constant) = adjust_scalar(
                                    constant.value.clone(),
                                    func.arguments[1].data_type()?,
                                );
                                if is_adjusted {
                                    predicate_set.merge_predicate(&func.arguments[1], Predicate {
                                        op: op.reverse(),
                                        constant,
                                    });
                                } else {
                                    new_predicates.push(predicate);
                                }
                            }
                        }
                        (false, false) => {
                            new_predicates.push(predicate);
                        }
                    }
                } else {
                    new_predicates.push(predicate);
                }
            } else {
                new_predicates.push(predicate);
            }
        }
        is_rewritten |= predicate_set.is_merged;
        if predicate_set.is_falsy {
            new_predicates = vec![
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false),
                }
                .into(),
            ];
        } else {
            let infer_predicates = predicate_set.into_predicates();
            new_predicates.extend(infer_predicates);
        }
        if is_rewritten {
            state.add_result(SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: new_predicates,
                        is_having: filter.is_having,
                    }
                    .into(),
                ),
                Arc::new(s_expr.child(0)?.clone()),
            ));
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
