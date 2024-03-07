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
use std::collections::HashSet;

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
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::Visitor;
use crate::plans::VisitorMut;
use crate::ColumnBinding;

// The InferFilterOptimizer tries to infer new predicates from existing predicates, for example:
// 1. [A > 1 and A > 5] => [A > 5], [A > 1 and A <= 1 => false], [A = 1 and A < 10] => [A = 1]
// 2. [A = 10 and A = B] => [B = 10]
// 3. [A = B and A = C] => [B = C]
pub struct InferFilterOptimizer {
    columns: Vec<ColumnBinding>,
    column_index: HashMap<ColumnBinding, usize>,
    column_equal_to: Vec<Vec<ColumnBinding>>,
    predicates: Vec<Vec<Predicate>>,
    is_falsy: bool,
}

impl InferFilterOptimizer {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            column_index: HashMap::new(),
            column_equal_to: vec![],
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

        let mut remaining_predicates = vec![];
        for predicate in predicates.into_iter() {
            if let ScalarExpr::FunctionCall(func) = &predicate {
                if let Some(mut op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    match (&func.arguments[0], &func.arguments[1]) {
                        (ScalarExpr::BoundColumnRef(left), ScalarExpr::BoundColumnRef(right)) => {
                            if op == ComparisonOp::Equal {
                                self.add_equal_column(&left.column, &right.column);
                            } else {
                                remaining_predicates.push(predicate);
                            }
                        }
                        (
                            ScalarExpr::BoundColumnRef(column_ref),
                            ScalarExpr::ConstantExpr(constant),
                        )
                        | (
                            ScalarExpr::ConstantExpr(constant),
                            ScalarExpr::BoundColumnRef(column_ref),
                        ) => {
                            if let ScalarExpr::BoundColumnRef(_) = &func.arguments[1] {
                                op = op.reverse();
                            }
                            let (is_adjusted, constant) = adjust_scalar(
                                constant.value.clone(),
                                *column_ref.column.data_type.clone(),
                            );
                            if is_adjusted {
                                self.add_predicate(&column_ref.column, Predicate { op, constant });
                            } else {
                                remaining_predicates.push(predicate);
                            }
                        }
                        _ => remaining_predicates.push(predicate),
                    }
                } else {
                    remaining_predicates.push(predicate);
                }
            } else {
                remaining_predicates.push(predicate);
            }
        }

        let mut new_predicates = vec![];
        if !self.is_falsy {
            // `derive_predicates` may change is_falsy to true.
            new_predicates = self.derive_predicates();
        }

        if self.is_falsy {
            new_predicates = vec![
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false),
                }
                .into(),
            ];
        } else {
            new_predicates.extend(self.derive_remaining_predicates(remaining_predicates));
        }

        Ok(new_predicates)
    }

    fn add_column(
        &mut self,
        column: &ColumnBinding,
        predicates: Vec<Predicate>,
        column_equal_to: Vec<ColumnBinding>,
    ) {
        self.column_index.insert(column.clone(), self.columns.len());
        self.columns.push(column.clone());
        self.predicates.push(predicates);
        self.column_equal_to.push(column_equal_to);
    }

    pub fn add_equal_column(&mut self, left: &ColumnBinding, right: &ColumnBinding) {
        match self.column_index.get(left) {
            Some(index) => self.column_equal_to[*index].push(right.clone()),
            None => self.add_column(left, vec![], vec![right.clone()]),
        };

        match self.column_index.get(right) {
            Some(index) => self.column_equal_to[*index].push(left.clone()),
            None => self.add_column(right, vec![], vec![left.clone()]),
        };
    }

    fn add_predicate(&mut self, column: &ColumnBinding, new_predicate: Predicate) {
        match self.column_index.get(column) {
            Some(index) => {
                let predicates = &mut self.predicates[*index];
                for predicate in predicates.iter_mut() {
                    match Self::merge_predicate(predicate, &new_predicate) {
                        MergeResult::None => {
                            self.is_falsy = true;
                            return;
                        }
                        MergeResult::Left => {
                            return;
                        }
                        MergeResult::Right => {
                            *predicate = new_predicate;
                            return;
                        }
                        MergeResult::All => (),
                    }
                }
                predicates.push(new_predicate);
            }
            None => {
                self.add_column(column, vec![new_predicate], vec![]);
            }
        };
    }

    fn merge_predicate(left: &Predicate, right: &Predicate) -> MergeResult {
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
        let num_columns = self.columns.len();

        let mut parents = vec![0; num_columns];
        for (i, parent) in parents.iter_mut().enumerate().take(num_columns) {
            *parent = i;
        }
        for (left_index, column_equal_to) in self.column_equal_to.iter().enumerate() {
            for column in column_equal_to.iter() {
                let right_index = self.column_index.get(column).unwrap();
                Self::union(&mut parents, left_index, *right_index);
            }
        }

        let mut equal_index_sets: HashMap<usize, HashSet<usize>> = HashMap::new();
        for index in 0..num_columns {
            let parent_index = Self::find(&mut parents, index);
            match equal_index_sets.get_mut(&parent_index) {
                Some(equal_index_set) => {
                    equal_index_set.insert(index);
                }
                None => {
                    equal_index_sets.insert(parent_index, HashSet::from([index]));
                }
            }
            if index != parent_index {
                let expr = self.columns[parent_index].clone();
                let predicates = self.predicates[index].clone();
                for predicate in predicates {
                    self.add_predicate(&expr, predicate);
                }
            }
        }

        for column in self.columns.iter() {
            let index = self.column_index.get(column).unwrap();
            let parent_index = Self::find(&mut parents, *index);
            let parent_predicates = &self.predicates[parent_index];
            for predicate in parent_predicates.iter() {
                result.push(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: String::from(predicate.op.to_func_name()),
                    params: vec![],
                    arguments: vec![
                        ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: column.clone(),
                        }),
                        ScalarExpr::ConstantExpr(predicate.constant.clone()),
                    ],
                }));
            }
        }

        for index in 0..num_columns {
            let parent_index = Self::find(&mut parents, index);
            if index == parent_index {
                if let Some(equal_index_set) = equal_index_sets.get(&parent_index) {
                    let mut equal_indexes = equal_index_set.iter().copied().collect::<Vec<_>>();
                    equal_indexes.sort();
                    let equal_indexes_len = equal_indexes.len();
                    for i in 0..equal_indexes_len {
                        for j in i + 1..equal_indexes_len {
                            result.push(ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: String::from(ComparisonOp::Equal.to_func_name()),
                                params: vec![],
                                arguments: vec![
                                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                                        span: None,
                                        column: self.columns[equal_indexes[i]].clone(),
                                    }),
                                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                                        span: None,
                                        column: self.columns[equal_indexes[j]].clone(),
                                    }),
                                ],
                            }));
                        }
                    }
                }
            }
        }

        result
    }

    fn derive_remaining_predicates(&self, predicates: Vec<ScalarExpr>) -> Vec<ScalarExpr> {
        struct ColumnBindingsCollector {
            columns: HashSet<ColumnBinding>,
            can_derive: bool,
        }

        impl<'a> Visitor<'a> for ColumnBindingsCollector {
            fn visit(&mut self, expr: &'a ScalarExpr) -> Result<()> {
                match expr {
                    ScalarExpr::BoundColumnRef(expr) => self.visit_bound_column_ref(expr),
                    ScalarExpr::FunctionCall(expr) => self.visit_function_call(expr),
                    ScalarExpr::CastExpr(expr) => self.visit_cast(expr),
                    ScalarExpr::ConstantExpr(_) => Ok(()),
                    ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::LambdaFunction(_)
                    | ScalarExpr::SubqueryExpr(_)
                    | ScalarExpr::UDFCall(_)
                    | ScalarExpr::UDFLambdaCall(_) => {
                        self.can_derive = false;
                        Ok(())
                    }
                }
            }

            fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                self.columns.insert(col.column.clone());
                Ok(())
            }
        }
        struct ReplaceColumnBindings<'a> {
            equal_to: &'a ColumnBinding,
        }

        impl<'a> VisitorMut<'_> for ReplaceColumnBindings<'a> {
            fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
                if let ScalarExpr::BoundColumnRef(column_ref) = expr {
                    column_ref.column = self.equal_to.clone()
                }
                walk_expr_mut(self, expr)
            }
        }

        let mut result_predicates = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let mut collector = ColumnBindingsCollector {
                columns: HashSet::new(),
                can_derive: true,
            };
            collector.visit(&predicate).unwrap();
            if collector.can_derive && collector.columns.len() == 1 {
                let original_column = collector.columns.iter().next().unwrap();
                if let Some(index) = self.column_index.get(original_column) {
                    let equal_to = &self.column_equal_to[*index];
                    for column in equal_to.iter() {
                        let mut replace = ReplaceColumnBindings { equal_to: column };
                        let mut new_predicate = predicate.clone();
                        replace.visit(&mut new_predicate).unwrap();
                        result_predicates.push(new_predicate);
                    }
                }
            }
            result_predicates.push(predicate);
        }

        result_predicates
    }
}

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
