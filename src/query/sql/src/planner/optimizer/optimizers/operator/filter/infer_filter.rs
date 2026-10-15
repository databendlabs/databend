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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_base::base::OrderedFloat;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::conversion::common_super_type_with_conversion;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::i256;

use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::optimizers::operator::filter::check_float_range;
use crate::optimizer::optimizers::operator::filter::check_int_range;
use crate::optimizer::optimizers::operator::filter::check_uint_range;
use crate::optimizer::optimizers::operator::filter::remove_trivial_type_cast;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

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
    // The normalized constraints of each ScalarExpr.
    expr_predicates: Vec<ExprConstraints>,
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

    pub fn optimize(&mut self, predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
        let mut predicates = predicates;
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
                    *func.return_type = ScalarExpr::passthrough_nullable_type(DataType::Boolean, [
                        &func.arguments[0],
                        &func.arguments[1],
                    ]);
                }
            }
        }

        // Borrow the inputs until inference succeeds. An unexpected domain mismatch
        // must keep every original condition, not become a contradiction or silently
        // drop a predicate. Record remaining indexes so the normal path moves, rather
        // than clones, their scalar expressions below.
        let mut remaining_indexes = vec![];
        for (index, predicate) in predicates.iter().enumerate() {
            if let ScalarExpr::FunctionCall(func) = predicate {
                if let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) {
                    match (
                        func.arguments[0].has_one_column_ref(),
                        func.arguments[1].has_one_column_ref(),
                    ) {
                        (true, true) => {
                            if op == ComparisonOp::Equal {
                                if !self.add_equal_expr(&func.arguments[0], &func.arguments[1]) {
                                    remaining_indexes.push(index);
                                }
                            } else {
                                remaining_indexes.push(index);
                            }
                        }
                        (true, false)
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[1] =>
                        {
                            if matches!(constant.value, Scalar::Null) {
                                self.is_falsy = true;
                                continue;
                            }
                            let (is_adjusted, constant) = adjust_scalar(
                                constant.value.clone(),
                                func.arguments[0].data_type().as_ref(),
                            );
                            if is_adjusted {
                                if !self.add_expr_predicate(&func.arguments[0], Predicate {
                                    op,
                                    constant,
                                }) {
                                    return Ok(predicates);
                                }
                            } else {
                                remaining_indexes.push(index);
                            }
                        }
                        (false, true)
                            if let ScalarExpr::ConstantExpr(constant) = &func.arguments[0] =>
                        {
                            if matches!(constant.value, Scalar::Null) {
                                self.is_falsy = true;
                                continue;
                            }
                            let (is_adjusted, constant) = adjust_scalar(
                                constant.value.clone(),
                                func.arguments[1].data_type().as_ref(),
                            );
                            if is_adjusted {
                                if !self.add_expr_predicate(&func.arguments[1], Predicate {
                                    op: op.reverse(),
                                    constant,
                                }) {
                                    return Ok(predicates);
                                }
                            } else {
                                remaining_indexes.push(index);
                            }
                        }
                        _ => remaining_indexes.push(index),
                    }
                } else {
                    remaining_indexes.push(index);
                }
            } else {
                remaining_indexes.push(index);
            }
        }

        let mut new_predicates = vec![];
        if !self.is_falsy {
            // Derive new predicates from existing predicates, `derive_predicates` may change is_falsy to true.
            let Some(derived) = self.derive_predicates() else {
                return Ok(predicates);
            };
            new_predicates = derived;
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
            // Move only the unabsorbed inputs into the remaining-predicate pass.
            let mut remaining_indexes = remaining_indexes.into_iter().peekable();
            let remaining_predicates = predicates
                .into_iter()
                .enumerate()
                .filter_map(|(index, predicate)| {
                    if remaining_indexes.peek() == Some(&index) {
                        remaining_indexes.next();
                        Some(predicate)
                    } else {
                        None
                    }
                })
                .collect();
            new_predicates.extend(self.derive_remaining_predicates(remaining_predicates));
        }

        Ok(new_predicates)
    }

    #[recursive::recursive]
    fn add_expr(
        &mut self,
        expr: &ScalarExpr,
        expr_predicates: ExprConstraints,
        expr_equal_to: Vec<ScalarExpr>,
    ) {
        self.expr_index.insert(expr.clone(), self.exprs.len());
        self.exprs.push(expr.clone());
        self.expr_predicates.push(expr_predicates);
        self.expr_equal_to.push(expr_equal_to);
    }

    pub fn add_equal_expr(&mut self, left: &ScalarExpr, right: &ScalarExpr) -> bool {
        if left == right {
            // Returning true tells the caller that this predicate has been absorbed and
            // can be removed. However, a reflexive equality only forms a singleton
            // equivalence class, so derive_predicates() will not reconstruct `expr = expr`.
            // Removing it is valid for a non-nullable operand, but for a nullable operand
            // the predicate is still needed to reject NULL rows. Return false so the caller
            // keeps the original predicate.
            return !left.data_type().is_nullable_or_null();
        }

        let left_ty = left.data_type();
        let right_ty = right.data_type();
        // NULL-typed expressions cannot make equality true. Do not let the bottom
        // type bridge otherwise incomparable numeric domains in an equivalence class.
        if left_ty.remove_nullable() == DataType::Null
            || right_ty.remove_nullable() == DataType::Null
        {
            return false;
        }
        if !common_super_type_with_conversion(left_ty.as_ref(), right_ty.as_ref())
            .is_some_and(|conversion| conversion.is_safe_for_equality_inference())
        {
            return false;
        }
        match self.expr_index.get(left) {
            Some(index) => self.expr_equal_to[*index].push(right.clone()),
            None => self.add_expr(left, ExprConstraints::default(), vec![right.clone()]),
        };

        match self.expr_index.get(right) {
            Some(index) => self.expr_equal_to[*index].push(left.clone()),
            None => self.add_expr(right, ExprConstraints::default(), vec![left.clone()]),
        };

        true
    }

    // Whether the predicate was absorbed. A domain mismatch is not a contradiction.
    fn add_expr_predicate(&mut self, expr: &ScalarExpr, predicate: Predicate) -> bool {
        let domain = NumericDomain::of_type(expr.data_type().as_ref());
        if domain.is_none()
            || domain
                != NumericDomain::of_type(&predicate.constant.value.as_ref().infer_data_type())
        {
            return false;
        }
        let index = match self.expr_index.get(expr) {
            Some(index) => *index,
            None => {
                let index = self.exprs.len();
                self.add_expr(expr, ExprConstraints::default(), vec![]);
                index
            }
        };
        let Some(satisfiable) = self.expr_predicates[index].try_add(predicate) else {
            return false;
        };
        self.is_falsy |= !satisfiable;
        true
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

    // None disables this inference pass; the caller retains all original inputs.
    fn derive_predicates(&mut self) -> Option<Vec<ScalarExpr>> {
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
        // Check the entire class before merging any bounds. In particular, a Float
        // member without predicates can bridge two Exact members: merging their
        // conflicting constants first could falsely conclude the class is empty.
        let mut class_domains = vec![None; num_exprs];
        for index in 0..num_exprs {
            let parent_index = Self::find(&mut parents, index);
            for domain in [
                NumericDomain::of_type(self.exprs[index].data_type().as_ref()),
                self.expr_predicates[index].domain(),
            ]
            .into_iter()
            .flatten()
            {
                if class_domains[parent_index].is_some_and(|existing| existing != domain) {
                    return None;
                }
                class_domains[parent_index] = Some(domain);
            }
            match equal_index_sets.get_mut(&parent_index) {
                Some(equal_index_set) => {
                    equal_index_set.insert(index);
                }
                None => {
                    equal_index_sets.insert(parent_index, HashSet::from([index]));
                }
            }
        }
        for index in 0..num_exprs {
            let parent_index = Self::find(&mut parents, index);
            if index != parent_index {
                // Merge each member's normalized constraints into its equivalence class.
                let constraints = std::mem::take(&mut self.expr_predicates[index]);
                for predicate in constraints.into_predicates() {
                    if !self.expr_predicates[parent_index].try_add(predicate)? {
                        self.is_falsy = true;
                        return Some(vec![]);
                    }
                }
            }
        }

        // Discard redundant exclusions once per class, not once per derived expression.
        for (index, constraints) in self.expr_predicates.iter_mut().enumerate() {
            if index == Self::find(&mut parents, index) {
                constraints.prune_exclusions();
            }
        }

        // Construct predicates for each ScalarExpr. `exprs` is indexed by the same
        // positions recorded in `expr_index`, so the position is the expression index.
        for (index, expr) in self.exprs.iter().enumerate() {
            let parent_index = Self::find(&mut parents, index);
            let parent_predicates = &self.expr_predicates[parent_index];
            for predicate in parent_predicates.predicates() {
                // A transitive class can connect types whose pairwise common type
                // would round or overflow. Keep the constraint on safely comparable
                // members instead of introducing a lossy comparison on every member.
                if !common_super_type_with_conversion(
                    expr.data_type().as_ref(),
                    predicate.constant.value.as_ref().infer_data_type(),
                )
                .is_some_and(|conversion| conversion.is_safe_for_equality_inference())
                {
                    continue;
                }
                let return_type = ScalarExpr::passthrough_nullable_type(DataType::Boolean, [expr]);
                result.push(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: String::from(predicate.op.to_func_name()),
                    params: vec![],
                    arguments: vec![
                        expr.clone(),
                        ScalarExpr::ConstantExpr(predicate.constant.clone()),
                    ],
                    return_type: Box::new(return_type),
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
                            if !common_super_type_with_conversion(
                                self.exprs[equal_indexes[i]].data_type().as_ref(),
                                self.exprs[equal_indexes[j]].data_type().as_ref(),
                            )
                            .is_some_and(|conversion| conversion.is_safe_for_equality_inference())
                            {
                                continue;
                            }
                            let return_type =
                                ScalarExpr::passthrough_nullable_type(DataType::Boolean, [
                                    &self.exprs[equal_indexes[i]],
                                    &self.exprs[equal_indexes[j]],
                                ]);
                            result.push(ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: String::from(ComparisonOp::Equal.to_func_name()),
                                params: vec![],
                                arguments: vec![
                                    self.exprs[equal_indexes[i]].clone(),
                                    self.exprs[equal_indexes[j]].clone(),
                                ],
                                return_type: Box::new(return_type),
                            }));
                        }
                    }
                }
            }
        }

        Some(result)
    }

    fn derive_remaining_predicates(&self, predicates: Vec<ScalarExpr>) -> Vec<ScalarExpr> {
        // The ReplaceScalarExpr is used to replace the ScalarExpr of a predicate.
        struct ReplaceScalarExpr<'a> {
            // The index of ScalarExpr in `exprs`.
            expr_index: &'a HashMap<ScalarExpr, usize>,
            // The equal ScalarExprs of each ScalarExpr.
            expr_equal_to: &'a Vec<Vec<ScalarExpr>>,
            // The columns used by the predicate.
            column_set: HashSet<Symbol>,
            // If the predicate can be replaced to generate a new predicate.
            can_replace: bool,
        }

        impl ReplaceScalarExpr<'_> {
            fn reset(&mut self) {
                self.column_set.clear();
                self.can_replace = true;
            }
        }

        impl VisitorMut<'_> for ReplaceScalarExpr<'_> {
            #[recursive::recursive]
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
                    ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => Ok(()),
                    ScalarExpr::BoundColumnRef(_)
                    | ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::LambdaFunction(_)
                    | ScalarExpr::SubqueryExpr(_)
                    | ScalarExpr::UDFCall(_)
                    | ScalarExpr::UDFLambdaCall(_)
                    | ScalarExpr::UDAFCall(_)
                    | ScalarExpr::AsyncFunctionCall(_) => {
                        // Can not replace `BoundColumnRef` or can not replace unsupported ScalarExpr.
                        self.can_replace = false;
                        Ok(())
                    }
                }
            }

            fn visit_function_call(&mut self, function: &mut FunctionCall) -> Result<()> {
                for argument in &mut function.arguments {
                    self.visit(argument)?;
                }
                function.refresh_return_type()
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
            if replace.visit(&mut new_predicate).is_err() || !replace.can_replace {
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

// Exact comparison keys, independent of integer width or Decimal precision. Never use
// Scalar::cmp here: incomparable Scalar variants compare as Equal. Float keys are
// deliberately kept separate from exact numeric keys to avoid lossy conversions.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum PredicateValue {
    Exact(ExactValue),
    Float(OrderedFloat<f64>),
}

#[derive(Clone, Debug)]
struct ExactValue {
    value: i256,
    scale: u8,
}

impl Ord for ExactValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.scale.cmp(&other.scale) {
            Ordering::Equal => self.value.cmp(&other.value),
            Ordering::Less => {
                // If rescaling overflows i256, its magnitude exceeds any value on
                // the other side. This also avoids rounding at Decimal256 boundaries.
                match self.value.checked_mul(i256::e(other.scale - self.scale)) {
                    Some(value) => value.cmp(&other.value),
                    None if self.value > i256::zero() => Ordering::Greater,
                    None => Ordering::Less,
                }
            }
            Ordering::Greater => other.cmp(self).reverse(),
        }
    }
}

impl PartialOrd for ExactValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ExactValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ExactValue {}

impl PredicateValue {
    fn new(value: &Scalar) -> Self {
        match value {
            Scalar::Number(number) => {
                if let Some(value) = number.integer_to_i128() {
                    Self::Exact(ExactValue {
                        value: i256::from(value),
                        scale: 0,
                    })
                } else {
                    Self::Float(number.to_f64())
                }
            }
            Scalar::Decimal(decimal) => Self::Exact(ExactValue {
                value: decimal.as_decimal(),
                scale: decimal.scale(),
            }),
            _ => unreachable!("only adjusted numeric constants can become constraints"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NumericDomain {
    Exact,
    Float,
}

impl NumericDomain {
    fn of_type(data_type: &DataType) -> Option<Self> {
        let mut data_type = data_type;
        while let DataType::Nullable(inner) = data_type {
            data_type = inner;
        }
        match data_type {
            DataType::Number(NumberDataType::Float32 | NumberDataType::Float64) => {
                Some(Self::Float)
            }
            DataType::Number(_) | DataType::Decimal(_) => Some(Self::Exact),
            _ => None,
        }
    }
}

#[derive(Default)]
enum ExprConstraints {
    #[default]
    Empty,
    Exact(Box<PredicateConstraints>),
    Float(Box<PredicateConstraints>),
}

impl ExprConstraints {
    // None means not absorbed, not false. Preserve the existing state on mismatch.
    fn try_add(&mut self, predicate: Predicate) -> Option<bool> {
        if !matches!(
            predicate.constant.value,
            Scalar::Number(_) | Scalar::Decimal(_)
        ) {
            return None;
        }
        let value = PredicateValue::new(&predicate.constant.value);
        if matches!(self, Self::Empty) {
            *self = match &value {
                PredicateValue::Exact(_) => Self::Exact(Box::default()),
                PredicateValue::Float(_) => Self::Float(Box::default()),
            };
        }
        // Constant adjustment and lossless equality inference keep every expression
        // and its equivalence class in one numeric domain.
        let constraints = match (self, &value) {
            (Self::Exact(constraints), PredicateValue::Exact(_))
            | (Self::Float(constraints), PredicateValue::Float(_)) => constraints,
            _ => return None,
        };
        Some(constraints.add(value, predicate))
    }

    fn domain(&self) -> Option<NumericDomain> {
        match self {
            Self::Empty => None,
            Self::Exact(_) => Some(NumericDomain::Exact),
            Self::Float(_) => Some(NumericDomain::Float),
        }
    }

    fn prune_exclusions(&mut self) {
        if let Self::Exact(constraints) | Self::Float(constraints) = self {
            constraints.prune_exclusions();
        }
    }

    // Call prune_exclusions after the last insertion before borrowing predicates.
    fn predicates(&self) -> impl Iterator<Item = &Predicate> {
        let constraints = match self {
            Self::Empty => None,
            Self::Exact(constraints) | Self::Float(constraints) => Some(constraints.as_ref()),
        };
        constraints
            .into_iter()
            .flat_map(PredicateConstraints::predicates)
    }

    fn into_predicates(self) -> impl Iterator<Item = Predicate> {
        let constraints = match self {
            Self::Empty => None,
            Self::Exact(constraints) | Self::Float(constraints) => Some(*constraints),
        };
        constraints
            .into_iter()
            .flat_map(PredicateConstraints::into_predicates)
    }
}

#[derive(Default)]
struct PredicateConstraints {
    equality: Option<(PredicateValue, Predicate)>,
    lower: Option<(PredicateValue, Predicate)>,
    upper: Option<(PredicateValue, Predicate)>,
    excluded: BTreeMap<PredicateValue, Predicate>,
    is_falsy: bool,
}

impl PredicateConstraints {
    // Each insertion compares only the equality and the two tightest bounds, with
    // O(log n) exclusion lookups. Do not rescan all exclusions when a bound changes.
    fn add(&mut self, value: PredicateValue, predicate: Predicate) -> bool {
        if self.is_falsy {
            return false;
        }
        if let Some((equal_value, _)) = &self.equality {
            self.is_falsy = !Self::satisfies(equal_value, &value, predicate.op);
            return !self.is_falsy;
        }

        match predicate.op {
            ComparisonOp::Equal => self.equality = Some((value, predicate)),
            ComparisonOp::NotEqual => {
                self.excluded.entry(value).or_insert(predicate);
            }
            ComparisonOp::GT | ComparisonOp::GTE => {
                if self.lower.as_ref().is_none_or(|(old_value, old)| {
                    value > *old_value
                        || value == *old_value
                            && predicate.op == ComparisonOp::GT
                            && old.op == ComparisonOp::GTE
                }) {
                    self.lower = Some((value, predicate));
                }
            }
            ComparisonOp::LT | ComparisonOp::LTE => {
                if self.upper.as_ref().is_none_or(|(old_value, old)| {
                    value < *old_value
                        || value == *old_value
                            && predicate.op == ComparisonOp::LT
                            && old.op == ComparisonOp::LTE
                }) {
                    self.upper = Some((value, predicate));
                }
            }
        }

        // Excluding a closed endpoint makes it open, regardless of insertion order.
        if let Some((value, predicate)) = &mut self.lower {
            if predicate.op == ComparisonOp::GTE && self.excluded.contains_key(value) {
                predicate.op = ComparisonOp::GT;
            }
        }
        if let Some((value, predicate)) = &mut self.upper {
            if predicate.op == ComparisonOp::LTE && self.excluded.contains_key(value) {
                predicate.op = ComparisonOp::LT;
            }
        }
        if let (Some((lower, lower_predicate)), Some((upper, upper_predicate))) =
            (&self.lower, &self.upper)
        {
            match lower.cmp(upper) {
                Ordering::Greater => self.is_falsy = true,
                Ordering::Equal => {
                    if lower_predicate.op == ComparisonOp::GT
                        || upper_predicate.op == ComparisonOp::LT
                    {
                        self.is_falsy = true;
                    } else if self.equality.is_none() {
                        let mut predicate = lower_predicate.clone();
                        predicate.op = ComparisonOp::Equal;
                        self.equality = Some((lower.clone(), predicate));
                    }
                }
                Ordering::Less => (),
            }
        }
        if let Some((value, _)) = &self.equality {
            self.is_falsy |= !self.within_bounds(value) || self.excluded.contains_key(value);
        }
        !self.is_falsy
    }

    fn satisfies(left: &PredicateValue, right: &PredicateValue, op: ComparisonOp) -> bool {
        match op {
            ComparisonOp::Equal => left == right,
            ComparisonOp::NotEqual => left != right,
            ComparisonOp::GT => left > right,
            ComparisonOp::GTE => left >= right,
            ComparisonOp::LT => left < right,
            ComparisonOp::LTE => left <= right,
        }
    }

    fn within_bounds(&self, value: &PredicateValue) -> bool {
        self.lower
            .as_ref()
            .is_none_or(|(lower, predicate)| Self::satisfies(value, lower, predicate.op))
            && self
                .upper
                .as_ref()
                .is_none_or(|(upper, predicate)| Self::satisfies(value, upper, predicate.op))
    }

    fn prune_exclusions(&mut self) {
        if self.equality.is_some() {
            self.excluded.clear();
        } else {
            let lower = &self.lower;
            let upper = &self.upper;
            self.excluded.retain(|value, _| {
                lower
                    .as_ref()
                    .is_none_or(|(bound, predicate)| Self::satisfies(value, bound, predicate.op))
                    && upper.as_ref().is_none_or(|(bound, predicate)| {
                        Self::satisfies(value, bound, predicate.op)
                    })
            });
        }
    }

    fn into_predicates(mut self) -> impl Iterator<Item = Predicate> {
        // Normalize before moving the bounds, which are needed to prune exclusions.
        self.prune_exclusions();
        let has_equality = self.equality.is_some();
        self.equality
            .into_iter()
            .chain(
                self.lower
                    .into_iter()
                    .chain(self.upper)
                    .filter(move |_| !has_equality),
            )
            .map(|(_, predicate)| predicate)
            .chain(self.excluded.into_values())
    }

    fn predicates(&self) -> impl Iterator<Item = &Predicate> {
        debug_assert!(
            self.excluded
                .keys()
                .all(|value| self.equality.is_none() && self.within_bounds(value)),
            "prune_exclusions must be called before predicates"
        );
        self.equality
            .iter()
            .map(|(_, predicate)| predicate)
            .chain(
                self.lower
                    .iter()
                    .chain(self.upper.iter())
                    .filter(|_| self.equality.is_none())
                    .map(|(_, predicate)| predicate),
            )
            .chain(self.excluded.values())
    }
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

pub fn adjust_scalar(scalar: Scalar, data_type: &DataType) -> (bool, ConstantExpr) {
    match data_type {
        DataType::Number(NumberDataType::UInt8)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::UInt8))) => {
            let (ok, v) = check_uint_range(u8::MAX as u64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt8(v as u8)),
                });
            }
        }
        DataType::Number(NumberDataType::UInt16)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::UInt16))) => {
            let (ok, v) = check_uint_range(u16::MAX as u64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt16(v as u16)),
                });
            }
        }
        DataType::Number(NumberDataType::UInt32)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::UInt32))) => {
            let (ok, v) = check_uint_range(u32::MAX as u64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt32(v as u32)),
                });
            }
        }
        DataType::Number(NumberDataType::UInt64)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::UInt64))) => {
            let (ok, v) = check_uint_range(u64::MAX, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt64(v)),
                });
            }
        }
        DataType::Number(NumberDataType::Int8)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::Int8))) => {
            let (ok, v) = check_int_range(i8::MIN as i64, i8::MAX as i64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int8(v as i8)),
                });
            }
        }
        DataType::Number(NumberDataType::Int16)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::Int16))) => {
            let (ok, v) = check_int_range(i16::MIN as i64, i16::MAX as i64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int16(v as i16)),
                });
            }
        }
        DataType::Number(NumberDataType::Int32)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::Int32))) => {
            let (ok, v) = check_int_range(i32::MIN as i64, i32::MAX as i64, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int32(v as i32)),
                });
            }
        }
        DataType::Number(NumberDataType::Int64)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::Int64))) => {
            let (ok, v) = check_int_range(i64::MIN, i64::MAX, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Int64(v)),
                });
            }
        }
        DataType::Number(NumberDataType::Float32)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::Float32))) => {
            let (ok, v) = check_float_range(f32::MIN as f64, f32::MAX as f64, &scalar);
            if ok && (v as f32) as f64 == v {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Float32(OrderedFloat(v as f32))),
                });
            }
        }
        DataType::Number(NumberDataType::Float64)
        | DataType::Nullable(deref!(DataType::Number(NumberDataType::Float64))) => {
            let (ok, v) = check_float_range(f64::MIN, f64::MAX, &scalar);
            if ok {
                return (true, ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::Float64(OrderedFloat(v))),
                });
            }
        }
        DataType::Decimal(_) | DataType::Nullable(deref!(DataType::Decimal(_)))
            if matches!(scalar, Scalar::Decimal(_) | Scalar::Number(_))
                && common_super_type_with_conversion(
                    data_type,
                    scalar.as_ref().infer_data_type(),
                )
                .is_some_and(|conversion| conversion.is_safe_for_equality_inference()) =>
        {
            // Do not round or rescale constants to the expression's Decimal type.
            return (true, ConstantExpr {
                span: None,
                value: scalar,
            });
        }
        _ => (),
    }
    (false, ConstantExpr {
        span: None,
        value: scalar,
    })
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use itertools::Itertools;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;

    fn predicate(op: ComparisonOp, value: Scalar) -> Predicate {
        Predicate {
            op,
            constant: ConstantExpr { span: None, value },
        }
    }

    impl ExprConstraints {
        // Tests of normalization use comparable inputs unless they explicitly test
        // the non-absorbed path through try_add.
        fn add(&mut self, predicate: Predicate) -> bool {
            self.try_add(predicate).expect("comparable test predicates")
        }
    }

    fn int(value: i64) -> Scalar {
        Scalar::Number(NumberScalar::Int64(value))
    }

    fn column(index: usize, data_type: DataType) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::from_field_index(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    fn comparison(left: ScalarExpr, op: ComparisonOp, right: ScalarExpr) -> ScalarExpr {
        let return_type = ScalarExpr::passthrough_nullable_type(DataType::Boolean, [&left, &right]);
        FunctionCall {
            span: None,
            func_name: op.to_func_name().to_string(),
            params: vec![],
            arguments: vec![left, right],
            return_type: Box::new(return_type),
        }
        .into()
    }

    fn filter(column: &ScalarExpr, op: ComparisonOp, value: Scalar) -> ScalarExpr {
        comparison(
            column.clone(),
            op,
            ConstantExpr { span: None, value }.into(),
        )
    }

    #[test]
    fn test_constraint_domains() {
        for predicate in [
            predicate(ComparisonOp::Equal, int(5)),
            predicate(
                ComparisonOp::Equal,
                Scalar::Number(NumberScalar::Float64(1.5.into())),
            ),
        ] {
            let mut constraints = ExprConstraints::default();
            assert!(matches!(constraints, ExprConstraints::Empty));
            assert!(constraints.add(predicate.clone()));
            match predicate.constant.value {
                Scalar::Number(NumberScalar::Int64(_)) => {
                    assert!(matches!(constraints, ExprConstraints::Exact(_)));
                }
                _ => assert!(matches!(constraints, ExprConstraints::Float(_))),
            }
            constraints.prune_exclusions();
            assert_eq!(constraints.predicates().cloned().collect::<Vec<_>>(), vec![
                predicate.clone()
            ]);
            assert_eq!(constraints.into_predicates().collect::<Vec<_>>(), vec![
                predicate
            ]);
        }
    }

    #[test]
    fn test_mixed_constraint_domains_are_not_absorbed() {
        let exact = predicate(ComparisonOp::Equal, int(5));
        let float = predicate(
            ComparisonOp::Equal,
            Scalar::Number(NumberScalar::Float64(1.5.into())),
        );
        for (initial, incompatible) in [(exact.clone(), float.clone()), (float, exact)] {
            let mut constraints = ExprConstraints::default();
            assert_eq!(constraints.try_add(initial.clone()), Some(true));
            assert_eq!(constraints.try_add(incompatible), None);
            constraints.prune_exclusions();
            assert_eq!(constraints.predicates().cloned().collect::<Vec<_>>(), vec![
                initial.clone()
            ]);
            assert_eq!(constraints.into_predicates().collect::<Vec<_>>(), vec![
                initial
            ]);
        }
    }

    #[test]
    fn test_wrong_adjusted_domain_is_not_absorbed() {
        let mut optimizer = InferFilterOptimizer::new(None);
        let col = column(0, DataType::Number(NumberDataType::Float64));
        assert!(!optimizer.add_expr_predicate(&col, predicate(ComparisonOp::Equal, int(1))));
        assert!(!optimizer.is_falsy);
        assert!(optimizer.exprs.is_empty());
        let mut constraints = ExprConstraints::default();
        assert_eq!(
            constraints.try_add(predicate(ComparisonOp::Equal, Scalar::Boolean(true))),
            None
        );
        assert!(matches!(constraints, ExprConstraints::Empty));
    }

    #[test]
    fn test_domain_mismatch_keeps_all_original_filters() -> Result<()> {
        let col = column(0, DataType::Number(NumberDataType::Int64));
        let other = column(1, DataType::Number(NumberDataType::Int64));
        let original = vec![
            comparison(col.clone(), ComparisonOp::NotEqual, other.clone()),
            filter(&other, ComparisonOp::GT, int(0)),
            filter(&col, ComparisonOp::LT, int(10)),
            filter(&other, ComparisonOp::LT, int(100)),
        ];
        let mut incompatible = ExprConstraints::default();
        assert!(incompatible.add(predicate(
            ComparisonOp::Equal,
            Scalar::Number(NumberScalar::Float64(1.5.into()))
        )));
        let mut optimizer = InferFilterOptimizer::new(None);
        // Fault injection: a future adjustment change admitted the wrong domain.
        optimizer.add_expr(&col, incompatible, vec![]);
        assert_eq!(optimizer.optimize(original.clone())?, original);
        assert!(!optimizer.is_falsy);
        Ok(())
    }

    #[test]
    fn test_mixed_class_falls_back_before_merging_exact_constants() -> Result<()> {
        let a = column(0, DataType::Number(NumberDataType::Int64));
        let b = column(1, DataType::Number(NumberDataType::Int64));
        let f = column(2, DataType::Number(NumberDataType::Float64));
        let cast = |expr| {
            ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(expr),
                target_type: Box::new(DataType::Number(NumberDataType::Float64)),
            })
        };
        let original = vec![
            comparison(cast(a.clone()), ComparisonOp::Equal, f.clone()),
            comparison(cast(b.clone()), ComparisonOp::Equal, f.clone()),
            filter(&a, ComparisonOp::Equal, int(9_007_199_254_740_993)),
            filter(&b, ComparisonOp::Equal, int(9_007_199_254_740_992)),
        ];
        let mut optimizer = InferFilterOptimizer::new(None);
        // Simulate a future equality-inference edge connecting integers through an
        // unconstrained Float member. Do not relax conversion.rs just for this test.
        optimizer.add_expr(&a, ExprConstraints::default(), vec![f.clone()]);
        optimizer.add_expr(&b, ExprConstraints::default(), vec![f.clone()]);
        optimizer.add_expr(&f, ExprConstraints::default(), vec![a, b]);
        assert_eq!(optimizer.optimize(original.clone())?, original);
        assert!(!optimizer.is_falsy);
        Ok(())
    }

    #[test]
    fn test_constraint_permutations() {
        use ComparisonOp::*;
        let cases = [
            (vec![], Some(vec![])),
            (
                vec![(GT, 0), (LT, 10), (LT, 5)],
                Some(vec![(GT, 0), (LT, 5)]),
            ),
            (vec![(GT, 0), (LT, 5), (GTE, 5)], None),
            (vec![(GTE, 5), (LTE, 5), (NotEqual, 5)], None),
            (
                vec![(GTE, 0), (LTE, 10), (Equal, 5)],
                Some(vec![(Equal, 5)]),
            ),
            (vec![(GT, 0), (Equal, 5), (Equal, 6)], None),
            (
                vec![(NotEqual, 5), (GTE, 5), (LTE, 10)],
                Some(vec![(GT, 5), (LTE, 10)]),
            ),
            (
                vec![(NotEqual, 5), (LTE, 5), (GTE, 0)],
                Some(vec![(GTE, 0), (LT, 5)]),
            ),
            (
                vec![(NotEqual, -1), (GT, 0), (LT, 5)],
                Some(vec![(GT, 0), (LT, 5)]),
            ),
            (
                vec![(GTE, 5), (LTE, 5), (NotEqual, 6)],
                Some(vec![(Equal, 5)]),
            ),
            (
                vec![
                    (GT, 0),
                    (LT, 5),
                    (NotEqual, 1),
                    (NotEqual, 3),
                    (NotEqual, 1),
                ],
                Some(vec![(GT, 0), (LT, 5), (NotEqual, 1), (NotEqual, 3)]),
            ),
        ];
        for (input, expected) in cases {
            for permutation in input.iter().permutations(input.len()) {
                let mut constraints = ExprConstraints::default();
                let is_satisfiable = permutation
                    .iter()
                    .all(|&&(op, value)| constraints.add(predicate(op, int(value))));
                assert_eq!(is_satisfiable, expected.is_some(), "{permutation:?}");
                if let Some(expected) = &expected {
                    constraints.prune_exclusions();
                    let actual = constraints.predicates().cloned().collect::<Vec<_>>();
                    let expected = expected
                        .iter()
                        .map(|&(op, value)| predicate(op, int(value)))
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected, "{permutation:?}");
                    // The consuming iterator must normalize unpruned constraints itself.
                    let mut owned_constraints = ExprConstraints::default();
                    for &&(op, value) in &permutation {
                        assert!(owned_constraints.add(predicate(op, int(value))));
                    }
                    assert_eq!(
                        owned_constraints.into_predicates().collect::<Vec<_>>(),
                        expected,
                        "owned: {permutation:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_constraint_semantics_exhaustive() {
        use ComparisonOp::*;

        // Evaluate both input and normalized predicates with native integer operators,
        // independently of the optimizer's numeric keys and comparison helper.
        fn matches_i64(predicate: &Predicate, value: i64) -> bool {
            let Scalar::Number(NumberScalar::Int64(bound)) = &predicate.constant.value else {
                unreachable!("this test only constructs Int64 predicates");
            };
            match predicate.op {
                Equal => value == *bound,
                NotEqual => value != *bound,
                GT => value > *bound,
                GTE => value >= *bound,
                LT => value < *bound,
                LTE => value <= *bound,
            }
        }

        let predicates = [Equal, NotEqual, GT, GTE, LT, LTE]
            .into_iter()
            .cartesian_product(-1..=1)
            .map(|(op, value)| predicate(op, int(value)))
            .collect::<Vec<_>>();
        // All triples, including duplicates and all orders. The samples include every
        // endpoint and both exterior regions; no integer-domain emptiness inference is required.
        for input in predicates.iter().combinations_with_replacement(3) {
            let expected = (-2..=2)
                .filter(|&value| input.iter().all(|predicate| matches_i64(predicate, value)))
                .collect::<Vec<_>>();
            for permutation in input.iter().permutations(3) {
                let mut constraints = ExprConstraints::default();
                let satisfiable = permutation
                    .iter()
                    .all(|predicate| constraints.add((***predicate).clone()));
                constraints.prune_exclusions();
                let actual = (-2..=2)
                    .filter(|&value| {
                        satisfiable
                            && constraints
                                .predicates()
                                .all(|predicate| matches_i64(predicate, value))
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{permutation:?}");
                let borrowed = constraints.predicates().cloned().collect::<Vec<_>>();
                assert_eq!(
                    constraints.into_predicates().collect::<Vec<_>>(),
                    borrowed,
                    "owned: {permutation:?}"
                );
            }
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "prune_exclusions must be called before predicates")]
    fn test_unpruned_out_of_bounds_exclusion_is_rejected() {
        let mut constraints = ExprConstraints::default();
        assert!(constraints.add(predicate(ComparisonOp::NotEqual, int(1))));
        assert!(constraints.add(predicate(ComparisonOp::GT, int(5))));
        constraints.predicates().count();
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "prune_exclusions must be called before predicates")]
    fn test_unpruned_exclusion_with_equality_is_rejected() {
        let mut constraints = ExprConstraints::default();
        assert!(constraints.add(predicate(ComparisonOp::NotEqual, int(1))));
        assert!(constraints.add(predicate(ComparisonOp::Equal, int(5))));
        constraints.predicates().count();
    }

    #[test]
    fn test_many_exclusions() {
        let mut constraints = ExprConstraints::default();
        for value in 0..10_000 {
            assert!(constraints.add(predicate(ComparisonOp::NotEqual, int(value))));
            assert!(constraints.add(predicate(ComparisonOp::NotEqual, int(value))));
        }
        constraints.prune_exclusions();
        assert_eq!(constraints.predicates().count(), 10_000);
        assert!(constraints.add(predicate(ComparisonOp::GTE, int(9_999))));
        constraints.prune_exclusions();
        assert_eq!(constraints.predicates().count(), 1);
        assert!(!constraints.add(predicate(ComparisonOp::LTE, int(9_999))));
    }

    #[test]
    fn test_equivalence_class_permutations() {
        let left = column(0, DataType::Number(NumberDataType::Int32));
        let right = column(1, DataType::Number(NumberDataType::Int64));
        let predicates = [
            comparison(left.clone(), ComparisonOp::Equal, right.clone()),
            filter(&left, ComparisonOp::GT, int(0)),
            filter(&left, ComparisonOp::LT, int(5)),
            filter(&right, ComparisonOp::GTE, int(5)),
        ];
        for permutation in predicates.iter().permutations(predicates.len()) {
            let result = InferFilterOptimizer::new(None)
                .optimize(permutation.into_iter().cloned().collect())
                .unwrap();
            assert_eq!(result, vec![
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false)
                }
                .into()
            ]);
        }
    }

    #[test]
    fn test_null_comparisons() {
        let col = column(0, DataType::Number(NumberDataType::Int64).wrap_nullable());
        for op in [
            ComparisonOp::Equal,
            ComparisonOp::NotEqual,
            ComparisonOp::GT,
            ComparisonOp::LTE,
        ] {
            for comparison in [
                filter(&col, op, Scalar::Null),
                comparison(
                    ConstantExpr {
                        span: None,
                        value: Scalar::Null,
                    }
                    .into(),
                    op,
                    col.clone(),
                ),
            ] {
                let result = InferFilterOptimizer::new(None)
                    .optimize(vec![comparison])
                    .unwrap();
                assert_eq!(result, vec![
                    ConstantExpr {
                        span: None,
                        value: Scalar::Boolean(false)
                    }
                    .into()
                ]);
            }
        }
        // A NULL-typed column must not bridge integer and floating equivalence classes.
        let null_col = column(1, DataType::Null);
        let float_col = column(2, DataType::Number(NumberDataType::Float64));
        let mut optimizer = InferFilterOptimizer::new(None);
        assert!(!optimizer.add_equal_expr(&col, &null_col));
        assert!(!optimizer.add_equal_expr(&null_col, &float_col));
        assert!(!optimizer.add_equal_expr(&col, &float_col));

        let reflexive = comparison(col.clone(), ComparisonOp::Equal, col);
        assert_eq!(
            InferFilterOptimizer::new(None)
                .optimize(vec![reflexive.clone()])
                .unwrap(),
            vec![reflexive]
        );
    }

    #[test]
    fn test_float_boundaries_and_conversions() {
        let col = column(0, DataType::Number(NumberDataType::Float64));
        let predicates = [
            filter(
                &col,
                ComparisonOp::GTE,
                Scalar::Number(NumberScalar::Float64((-0.0).into())),
            ),
            filter(
                &col,
                ComparisonOp::LTE,
                Scalar::Number(NumberScalar::Float64(0.0.into())),
            ),
            filter(
                &col,
                ComparisonOp::NotEqual,
                Scalar::Number(NumberScalar::Float64(0.0.into())),
            ),
        ];
        for permutation in predicates.iter().permutations(3) {
            let result = InferFilterOptimizer::new(None)
                .optimize(permutation.into_iter().cloned().collect())
                .unwrap();
            assert_eq!(result, vec![
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false)
                }
                .into()
            ]);
        }
        // Non-finite constants remain untouched rather than being compared with finite bounds.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let original = filter(
                &col,
                ComparisonOp::GT,
                Scalar::Number(NumberScalar::Float64(value.into())),
            );
            assert_eq!(
                InferFilterOptimizer::new(None)
                    .optimize(vec![original.clone()])
                    .unwrap(),
                vec![original]
            );
        }
        for data_type in [NumberDataType::Float32, NumberDataType::Float64] {
            let max = if data_type == NumberDataType::Float32 {
                f32::MAX as f64
            } else {
                f64::MAX
            };
            let below_max = if data_type == NumberDataType::Float32 {
                f32::from_bits(f32::MAX.to_bits() - 1) as f64
            } else {
                f64::from_bits(f64::MAX.to_bits() - 1)
            };
            let ty = DataType::Number(data_type);
            let col = column(0, ty.clone());
            let max = adjust_scalar(Scalar::Number(NumberScalar::Float64(max.into())), &ty)
                .1
                .value;
            let below_max =
                adjust_scalar(Scalar::Number(NumberScalar::Float64(below_max.into())), &ty)
                    .1
                    .value;
            let predicates = [
                filter(&col, ComparisonOp::GTE, max),
                filter(&col, ComparisonOp::LTE, below_max),
            ];
            for permutation in predicates.iter().permutations(2) {
                assert_eq!(
                    InferFilterOptimizer::new(None)
                        .optimize(permutation.into_iter().cloned().collect())
                        .unwrap(),
                    vec![
                        ConstantExpr {
                            span: None,
                            value: Scalar::Boolean(false)
                        }
                        .into()
                    ]
                );
            }
        }
        let float32 = DataType::Number(NumberDataType::Float32);
        assert!(
            !adjust_scalar(
                Scalar::Number(NumberScalar::Float64(f64::from_bits(1).into())),
                &float32
            )
            .0
        );
        let value = Scalar::Number(NumberScalar::Float64(0.1.into()));
        assert!(!adjust_scalar(value.clone(), &float32).0);
        let col = column(0, float32);
        let cast: ScalarExpr = CastExpr {
            span: None,
            is_try: false,
            argument: Box::new(col),
            target_type: Box::new(DataType::Number(NumberDataType::Float64)),
        }
        .into();
        let constant: ScalarExpr = ConstantExpr { span: None, value }.into();
        assert_eq!(
            remove_trivial_type_cast(cast.clone(), constant.clone()),
            (cast, constant)
        );
    }

    #[test]
    fn test_cast_removal_is_lossless() {
        let col = column(0, DataType::Number(NumberDataType::Int64));
        for target_type in [NumberDataType::Int32, NumberDataType::Float64] {
            let cast: ScalarExpr = CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(col.clone()),
                target_type: Box::new(DataType::Number(target_type)),
            }
            .into();
            let constant: ScalarExpr = ConstantExpr {
                span: None,
                value: int(5),
            }
            .into();
            assert_eq!(
                remove_trivial_type_cast(cast.clone(), constant.clone()),
                (cast, constant)
            );
        }
        assert!(!adjust_scalar(int(256), &DataType::Number(NumberDataType::UInt8)).0);
        assert!(
            !adjust_scalar(
                Scalar::Number(NumberScalar::UInt64(u64::MAX)),
                &DataType::Number(NumberDataType::Int64)
            )
            .0
        );
    }

    #[test]
    fn test_decimal_comparison_admission() {
        let small_literal = Scalar::Decimal(DecimalScalar::Decimal64(
            500,
            DecimalSize::new(3, 2).unwrap(),
        ));
        let column_type = DataType::Decimal(DecimalSize::new(18, 2).unwrap());
        let (ok, constant) = adjust_scalar(small_literal.clone(), &column_type);
        assert!(ok);
        assert_eq!(constant.value, small_literal);
        assert!(adjust_scalar(int(5), &column_type).0);
        // The common Decimal precision is capped at 38, so rescaling may lose digits.
        let tiny_literal = Scalar::Decimal(DecimalScalar::Decimal128(
            1,
            DecimalSize::new(38, 38).unwrap(),
        ));
        assert!(
            !adjust_scalar(
                tiny_literal,
                &DataType::Decimal(DecimalSize::new(38, 0).unwrap())
            )
            .0
        );
        assert!(
            !adjust_scalar(
                Scalar::Number(NumberScalar::Float64(0.1.into())),
                &column_type
            )
            .0
        );
    }

    #[test]
    fn test_decimal_equivalence_class() {
        let left_value = Scalar::Decimal(DecimalScalar::Decimal64(
            500,
            DecimalSize::new(18, 2).unwrap(),
        ));
        let right_value = Scalar::Decimal(DecimalScalar::Decimal128(
            5000,
            DecimalSize::new(38, 3).unwrap(),
        ));
        let left = column(0, left_value.as_ref().infer_data_type());
        let right = column(1, right_value.as_ref().infer_data_type());
        let predicates = [
            comparison(left.clone(), ComparisonOp::Equal, right.clone()),
            filter(&left, ComparisonOp::LT, left_value),
            filter(&right, ComparisonOp::GTE, right_value),
        ];
        for permutation in predicates.iter().permutations(3) {
            assert_eq!(
                InferFilterOptimizer::new(None)
                    .optimize(permutation.into_iter().cloned().collect())
                    .unwrap(),
                vec![
                    ConstantExpr {
                        span: None,
                        value: Scalar::Boolean(false)
                    }
                    .into()
                ]
            );
        }
    }

    #[test]
    fn test_exact_decimal_boundaries() {
        let sizes = [
            DecimalSize::new(18, 2).unwrap(),
            DecimalSize::new(38, 2).unwrap(),
            DecimalSize::new(76, 2).unwrap(),
        ];
        let values = [
            Scalar::Decimal(DecimalScalar::Decimal64(500, sizes[0])),
            Scalar::Decimal(DecimalScalar::Decimal128(500, sizes[1])),
            Scalar::Decimal(DecimalScalar::Decimal256(i256::from(500_i128), sizes[2])),
        ];
        for value in values {
            let ty = value.as_ref().infer_data_type();
            assert!(adjust_scalar(value.clone(), &ty).0);
            let col = column(0, ty);
            let predicates = [
                filter(&col, ComparisonOp::GTE, value.clone()),
                filter(&col, ComparisonOp::LTE, value.clone()),
                filter(&col, ComparisonOp::NotEqual, value),
            ];
            for permutation in predicates.iter().permutations(3) {
                assert_eq!(
                    InferFilterOptimizer::new(None)
                        .optimize(permutation.into_iter().cloned().collect())
                        .unwrap(),
                    vec![
                        ConstantExpr {
                            span: None,
                            value: Scalar::Boolean(false)
                        }
                        .into()
                    ]
                );
            }
        }
        let huge = PredicateValue::new(&Scalar::Decimal(DecimalScalar::Decimal256(
            i256::max_for_precision(76),
            DecimalSize::new(76, 0).unwrap(),
        )));
        let tiny = PredicateValue::new(&Scalar::Decimal(DecimalScalar::Decimal256(
            i256::one(),
            DecimalSize::new(76, 76).unwrap(),
        )));
        assert!(huge > tiny);
        let max = i256::max_for_precision(76);
        let size = DecimalSize::new(76, 2).unwrap();
        let mut constraints = ExprConstraints::default();
        assert!(constraints.add(predicate(
            ComparisonOp::Equal,
            Scalar::Decimal(DecimalScalar::Decimal256(max, size))
        )));
        assert!(constraints.add(predicate(
            ComparisonOp::NotEqual,
            Scalar::Decimal(DecimalScalar::Decimal256(
                max.checked_sub(i256::one()).unwrap(),
                size
            ))
        )));
        assert!(!constraints.add(predicate(
            ComparisonOp::NotEqual,
            Scalar::Decimal(DecimalScalar::Decimal256(max, size))
        )));
        let negative_huge = PredicateValue::new(&Scalar::Decimal(DecimalScalar::Decimal256(
            i256::min_for_precision(76),
            DecimalSize::new(76, 0).unwrap(),
        )));
        assert!(negative_huge < tiny);
        let decimal_one = PredicateValue::new(&Scalar::Decimal(DecimalScalar::Decimal256(
            i256::e(75),
            DecimalSize::new(76, 75).unwrap(),
        )));
        assert_eq!(PredicateValue::new(&int(1)), decimal_one);
        let unsigned_max = PredicateValue::new(&Scalar::Number(NumberScalar::UInt64(u64::MAX)));
        assert!(unsigned_max > PredicateValue::new(&int(i64::MAX)));
        let five = Scalar::Decimal(DecimalScalar::Decimal64(500, sizes[0]));
        let five_different_scale = Scalar::Decimal(DecimalScalar::Decimal128(
            5000,
            DecimalSize::new(38, 3).unwrap(),
        ));
        let mut constraints = ExprConstraints::default();
        assert!(constraints.add(predicate(ComparisonOp::Equal, five)));
        assert!(!constraints.add(predicate(ComparisonOp::NotEqual, five_different_scale)));
    }
}
