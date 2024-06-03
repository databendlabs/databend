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
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use ordered_float::OrderedFloat;

use crate::optimizer::rule::constant::check_float_range;
use crate::optimizer::rule::constant::check_int_range;
use crate::optimizer::rule::constant::check_uint_range;
use crate::optimizer::rule::constant::remove_trivial_type_cast;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;
use crate::ColumnSet;

// The InferFilterOptimizer tries to infer new predicates from existing predicates, for example:
// 1. [A > 1 and A > 5] => [A > 5], [A > 1 and A <= 1 => false], [A = 1 and A < 10] => [A = 1]
// 2. [A = 10 and A = B] => [B = 10]
// 3. [A = B and A = C] => [B = C]
pub struct InferFilterOptimizer<'a> {
    // All ScalarExprs.
    exprs: Vec<ScalarExpr>,
    // The index of ScalarExpr in `exprs`.
    expr_index: HashMap<ScalarExpr, usize>,
    // The equal ScalarExprs of each ScalarExpr.
    expr_equal_to: Vec<Vec<ScalarExpr>>,
    // The predicates of each ScalarExpr.
    expr_predicates: Vec<Vec<Predicate>>,
    // If the whole predicates is false.
    is_falsy: bool,
    // The `join_prop` is used for filter push down join.
    join_prop: Option<JoinProperty<'a>>,
}

impl<'a> InferFilterOptimizer<'a> {
    pub fn new(join_prop: Option<JoinProperty<'a>>) -> Self {
        Self {
            exprs: vec![],
            expr_index: HashMap::new(),
            expr_equal_to: vec![],
            expr_predicates: vec![],
            is_falsy: false,
            join_prop,
        }
    }

    pub fn run(mut self, mut predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
        dbg!("predicate = {:?}", &predicates);
        // Remove trivial type cast.
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

        // Process each predicate, add it to the optimizer if it can be used to infer new predicates,
        // otherwise, add it to the remaining predicates.
        let mut remaining_predicates = vec![];
        for predicate in predicates.into_iter() {
            if let ScalarExpr::FunctionCall(func) = &predicate {
                if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    match (
                        func.arguments[0].has_one_column_ref(),
                        func.arguments[1].has_one_column_ref(),
                    ) {
                        (true, true) => {
                            if op == ComparisonOp::Equal {
                                self.add_equal_expr(&func.arguments[0], &func.arguments[1]);
                            } else {
                                remaining_predicates.push(predicate);
                            }
                        }
                        (true, false)
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[1] =>
                        {
                            let (is_adjusted, constant) = adjust_scalar(
                                constant.value.clone(),
                                func.arguments[0].data_type()?,
                            );
                            if is_adjusted {
                                self.add_expr_predicate(&func.arguments[0], Predicate {
                                    op,
                                    constant,
                                })?;
                            } else {
                                remaining_predicates.push(predicate);
                            }
                        }
                        (false, true)
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[0] =>
                        {
                            let (is_adjusted, constant) = adjust_scalar(
                                constant.value.clone(),
                                func.arguments[1].data_type()?,
                            );
                            if is_adjusted {
                                self.add_expr_predicate(&func.arguments[1], Predicate {
                                    op: op.reverse(),
                                    constant,
                                })?;
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
            // Derive new predicates from existing predicates, `derive_predicates` may change is_falsy to true.
            new_predicates = self.derive_predicates()?;
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
            // Derive new predicates from remaining predicates.
            new_predicates.extend(self.derive_remaining_predicates(remaining_predicates));
        }

        Ok(new_predicates)
    }

    fn add_expr(
        &mut self,
        expr: &ScalarExpr,
        expr_predicates: Vec<Predicate>,
        expr_equal_to: Vec<ScalarExpr>,
    ) {
        self.expr_index.insert(expr.clone(), self.exprs.len());
        self.exprs.push(expr.clone());
        self.expr_predicates.push(expr_predicates);
        self.expr_equal_to.push(expr_equal_to);
    }

    pub fn add_equal_expr(&mut self, left: &ScalarExpr, right: &ScalarExpr) {
        match self.expr_index.get(left) {
            Some(index) => self.expr_equal_to[*index].push(right.clone()),
            None => self.add_expr(left, vec![], vec![right.clone()]),
        };

        match self.expr_index.get(right) {
            Some(index) => self.expr_equal_to[*index].push(left.clone()),
            None => self.add_expr(right, vec![], vec![left.clone()]),
        };
    }

    fn add_expr_predicate(&mut self, expr: &ScalarExpr, new_predicate: Predicate) -> Result<()> {
        match self.expr_index.get(expr) {
            Some(index) => {
                let predicates = &mut self.expr_predicates[*index];
                for predicate in predicates.iter_mut() {
                    match Self::merge_predicate(predicate.clone(), new_predicate.clone())? {
                        MergeResult::None => {
                            self.is_falsy = true;
                            return Ok(());
                        }
                        MergeResult::Left => {
                            return Ok(());
                        }
                        MergeResult::Right => {
                            *predicate = new_predicate;
                            return Ok(());
                        }
                        MergeResult::All => (),
                    }
                }
                predicates.push(new_predicate);
            }
            None => {
                self.add_expr(expr, vec![new_predicate], vec![]);
            }
        };
        Ok(())
    }

    fn merge_predicate(mut left: Predicate, mut right: Predicate) -> Result<MergeResult> {
        let left_data_type = ScalarExpr::ConstantExpr(left.constant.clone()).data_type()?;
        let right_data_type = ScalarExpr::ConstantExpr(right.constant.clone()).data_type()?;
        if left_data_type != right_data_type {
            let common_data_type = common_super_type(
                left_data_type,
                right_data_type,
                &BUILTIN_FUNCTIONS.default_cast_rules,
            );
            if let Some(data_type) = common_data_type {
                let (left_is_adjusted, left_constant) =
                    adjust_scalar(left.constant.value.clone(), data_type.clone());
                let (right_is_adjusted, right_constant) =
                    adjust_scalar(right.constant.value.clone(), data_type.clone());
                if left_is_adjusted && right_is_adjusted {
                    left.constant = left_constant;
                    right.constant = right_constant;
                }
            } else {
                return Ok(MergeResult::All);
            }
        }
        let merge_result = match left.op {
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
        };
        Ok(merge_result)
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

    fn derive_predicates(&mut self) -> Result<Vec<ScalarExpr>> {
        let mut result = vec![];
        let num_exprs = self.exprs.len();

        // Using the Union-Find algorithm to construct the equal ScalarExpr index sets.
        let mut equal_index_sets: HashMap<usize, HashSet<usize>> = HashMap::new();
        let mut parents = vec![0; num_exprs];
        for (i, parent) in parents.iter_mut().enumerate().take(num_exprs) {
            *parent = i;
        }
        for (left_index, expr_equal_to) in self.expr_equal_to.iter().enumerate() {
            for expr in expr_equal_to.iter() {
                let right_index = self.expr_index.get(expr).unwrap();
                Self::union(&mut parents, left_index, *right_index);
            }
        }
        for index in 0..num_exprs {
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
                // Add the predicates to the parent ScalarExpr.
                let expr = self.exprs[parent_index].clone();
                let predicates = self.expr_predicates[index].clone();
                for predicate in predicates {
                    self.add_expr_predicate(&expr, predicate)?;
                }
            }
        }

        // Construct predicates for each ScalarExpr.
        for expr in self.exprs.iter() {
            let index = self.expr_index.get(expr).unwrap();
            let parent_index = Self::find(&mut parents, *index);
            let parent_predicates = &self.expr_predicates[parent_index];
            for predicate in parent_predicates.iter() {
                result.push(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: String::from(predicate.op.to_func_name()),
                    params: vec![],
                    arguments: vec![
                        expr.clone(),
                        ScalarExpr::ConstantExpr(predicate.constant.clone()),
                    ],
                }));
            }
        }

        // Construct equal condition predicates for each equal ScalarExpr index set.
        for index in 0..num_exprs {
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
                                    self.exprs[equal_indexes[i]].clone(),
                                    self.exprs[equal_indexes[j]].clone(),
                                ],
                            }));
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    fn derive_remaining_predicates(&self, predicates: Vec<ScalarExpr>) -> Vec<ScalarExpr> {
        // The ReplaceScalarExpr is used to replace the ScalarExpr of a predicate.
        struct ReplaceScalarExpr<'a> {
            // The index of ScalarExpr in `exprs`.
            expr_index: &'a HashMap<ScalarExpr, usize>,
            // The equal ScalarExprs of each ScalarExpr.
            expr_equal_to: &'a Vec<Vec<ScalarExpr>>,
            // The columns used by the predicate.
            column_set: HashSet<usize>,
            // If the predicate can be replaced to generate a new predicate.
            can_replace: bool,
        }

        impl<'a> ReplaceScalarExpr<'a> {
            fn reset(&mut self) {
                self.column_set.clear();
                self.can_replace = true;
            }
        }

        impl<'a> VisitorMut<'_> for ReplaceScalarExpr<'a> {
            fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
                if let Some(index) = self.expr_index.get(expr) {
                    let equal_to = &self.expr_equal_to[*index];
                    if !equal_to.is_empty() {
                        let used_columns = expr.used_columns();
                        for column in used_columns {
                            self.column_set.insert(column);
                        }
                        *expr = equal_to[0].clone();
                        return Ok(());
                    }
                }
                match expr {
                    ScalarExpr::FunctionCall(expr) => self.visit_function_call(expr),
                    ScalarExpr::CastExpr(expr) => self.visit_cast_expr(expr),
                    ScalarExpr::ConstantExpr(_) => Ok(()),
                    ScalarExpr::BoundColumnRef(_)
                    | ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::LambdaFunction(_)
                    | ScalarExpr::SubqueryExpr(_)
                    | ScalarExpr::UDFCall(_)
                    | ScalarExpr::UDFLambdaCall(_)
                    | ScalarExpr::AsyncFunctionCall(_) => {
                        // Can not replace `BoundColumnRef` or can not replace unsupported ScalarExpr.
                        self.can_replace = false;
                        Ok(())
                    }
                }
            }
        }

        let mut replace = ReplaceScalarExpr {
            expr_index: &self.expr_index,
            expr_equal_to: &self.expr_equal_to,
            column_set: HashSet::new(),
            can_replace: true,
        };

        let mut result_predicates = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            replace.reset();
            let mut new_predicate = predicate.clone();
            replace.visit(&mut new_predicate).unwrap();
            if !replace.can_replace {
                result_predicates.push(predicate);
                continue;
            }

            let mut can_replace = false;
            if let Some(join_prop) = &self.join_prop {
                let mut has_left = false;
                let mut has_right = false;
                for column in replace.column_set.iter() {
                    if join_prop.left_columns.contains(column) {
                        has_left = true;
                    } else if join_prop.right_columns.contains(column) {
                        has_right = true;
                    }
                }
                // We only derive new predicates when the predicate contains columns only from one side of the join.
                if has_left && !has_right || !has_left && has_right {
                    can_replace = true;
                }
            } else if replace.column_set.len() == 1 {
                can_replace = true;
            }

            if !can_replace {
                result_predicates.push(predicate);
                continue;
            }

            if new_predicate != predicate {
                result_predicates.push(new_predicate);
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

pub struct JoinProperty<'a> {
    left_columns: &'a ColumnSet,
    right_columns: &'a ColumnSet,
}

impl<'a> JoinProperty<'a> {
    pub fn new(left_columns: &'a ColumnSet, right_columns: &'a ColumnSet) -> Self {
        Self {
            left_columns,
            right_columns,
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
