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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use ordered_float::OrderedFloat;

use crate::optimizer::rule::constant::check_float_range;
use crate::optimizer::rule::constant::check_int_range;
use crate::optimizer::rule::constant::check_uint_range;
use crate::optimizer::rule::constant::remove_trivial_type_cast;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug)]
struct Predicate {
    op: ComparisonOp,
    constant: ConstantExpr,
}

enum MergeResult {
    All,
    Left,
    Right,
    None,
}

pub struct InferFilterOptimizer {
    exprs: Vec<ScalarExpr>,
    expr_index: HashMap<ScalarExpr, usize>,
    expr_equal_to: Vec<Vec<ScalarExpr>>,
    predicates: Vec<Vec<Predicate>>,
    is_falsy: bool,
}

impl InferFilterOptimizer {
    pub fn new() -> Self {
        Self {
            exprs: vec![],
            expr_index: HashMap::new(),
            expr_equal_to: vec![],
            predicates: vec![],
            is_falsy: false,
        }
    }

    pub fn run(mut self, mut predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
        for predicate in predicates.iter_mut() {
            if let ScalarExpr::FunctionCall(func) = predicate {
                if ComparisonOp::try_from_func_name(&func.func_name).is_some() {
                    let (left, right) = remove_trivial_type_cast(
                        func.arguments[0].clone(),
                        func.arguments[1].clone(),
                    );
                    if left != func.arguments[0] {
                        func.arguments[0] = left;
                    }
                    if right != func.arguments[1] {
                        func.arguments[1] = right;
                    }
                }
            }
        }

        let mut new_predicates = vec![];
        for predicate in predicates.into_iter() {
            if let ScalarExpr::FunctionCall(func) = &predicate {
                if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    match (
                        func.arguments[0].is_column_ref(),
                        func.arguments[1].is_column_ref(),
                    ) {
                        (true, true) => {
                            if op == ComparisonOp::Equal {
                                self.add_equal(&func.arguments[0], &func.arguments[1]);
                            }
                            new_predicates.push(predicate);
                        }
                        (true, false) => {
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[1] {
                                let (is_adjusted, constant) = adjust_scalar(
                                    constant.value.clone(),
                                    func.arguments[0].data_type()?,
                                );
                                if is_adjusted {
                                    self.add_predicate(&func.arguments[0], Predicate {
                                        op,
                                        constant,
                                    });
                                } else {
                                    new_predicates.push(predicate);
                                }
                            } else {
                                new_predicates.push(predicate);
                            }
                        }
                        (false, true) => {
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[0] {
                                let (is_adjusted, constant) = adjust_scalar(
                                    constant.value.clone(),
                                    func.arguments[1].data_type()?,
                                );
                                if is_adjusted {
                                    self.add_predicate(&func.arguments[1], Predicate {
                                        op: op.reverse(),
                                        constant,
                                    });
                                } else {
                                    new_predicates.push(predicate);
                                }
                            } else {
                                new_predicates.push(predicate);
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
        if !self.is_falsy {
            // `derive_predicates` may change is_falsy to true.
            let derived_predicates = self.derive_predicates();
            new_predicates.extend(derived_predicates);
        }
        if self.is_falsy {
            new_predicates = vec![
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false),
                }
                .into(),
            ];
        }
        Ok(new_predicates)
    }

    fn add_expr(
        &mut self,
        expr: &ScalarExpr,
        predicates: Vec<Predicate>,
        expr_equal_to: Vec<ScalarExpr>,
    ) {
        self.expr_index.insert(expr.clone(), self.exprs.len());
        self.exprs.push(expr.clone());
        self.predicates.push(predicates);
        self.expr_equal_to.push(expr_equal_to);
    }

    fn add_equal(&mut self, left: &ScalarExpr, right: &ScalarExpr) {
        match self.expr_index.get(left) {
            Some(idx) => {
                let expr_equal_to = &mut self.expr_equal_to[*idx];
                expr_equal_to.push(right.clone());
            }
            None => self.add_expr(left, vec![], vec![right.clone()]),
        };
        if self.expr_index.get(right).is_none() {
            self.add_expr(right, vec![], vec![]);
        }
    }

    fn add_predicate(&mut self, left: &ScalarExpr, right: Predicate) {
        match self.expr_index.get(left) {
            Some(idx) => {
                let predicates = &mut self.predicates[*idx];
                for predicate in predicates.iter_mut() {
                    match Self::merge(predicate, &right) {
                        MergeResult::None => {
                            self.is_falsy = true;
                            return;
                        }
                        MergeResult::Left => {
                            return;
                        }
                        MergeResult::Right => {
                            *predicate = right;
                            return;
                        }
                        MergeResult::All => (),
                    }
                }
                predicates.push(right);
            }
            None => self.add_expr(left, vec![right], vec![]),
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

    fn find(parent: &mut [usize], x: usize) -> usize {
        if parent[x] != x {
            parent[x] = Self::find(parent, parent[x]);
        }
        parent[x]
    }

    fn union(parent: &mut [usize], x: usize, y: usize) {
        let parent_x = Self::find(parent, x);
        let parent_y = Self::find(parent, y);
        if parent_x != parent_y {
            parent[parent_y] = parent_x;
        }
    }

    fn derive_predicates(&mut self) -> Vec<ScalarExpr> {
        let mut result = vec![];
        let num_exprs = self.exprs.len();
        let mut parents = vec![0; num_exprs];
        for (i, parent) in parents.iter_mut().enumerate().take(num_exprs) {
            *parent = i;
        }
        for (left_idx, expr_equal_to) in self.expr_equal_to.iter().enumerate() {
            for expr in expr_equal_to.iter() {
                let right_idx = self.expr_index.get(expr).unwrap();
                Self::union(&mut parents, left_idx, *right_idx);
            }
        }
        let mut old_predicates_set = self.predicates.clone();
        for predicates in old_predicates_set.iter_mut() {
            predicates.sort();
        }
        for idx in 0..num_exprs {
            let parent_idx = Self::find(&mut parents, idx);
            if idx != parent_idx {
                let expr = self.exprs[parent_idx].clone();
                let predicates = self.predicates[idx].clone();
                for predicate in predicates {
                    self.add_predicate(&expr, predicate);
                }
            }
        }
        for predicates in self.predicates.iter_mut() {
            predicates.sort();
        }
        for (scalar, idx) in self.expr_index.iter() {
            let parent_idx = Self::find(&mut parents, *idx);
            let parent_predicates = &self.predicates[parent_idx];
            for predicate in parent_predicates.iter() {
                result.push(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: String::from(predicate.op.to_func_name()),
                    params: vec![],
                    arguments: vec![
                        scalar.clone(),
                        ScalarExpr::ConstantExpr(predicate.constant.clone()),
                    ],
                }));
            }
        }
        result
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
            let (ok, v) = check_float_range(f32::MIN as f64, f32::MAX as f64, &scalar);
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
