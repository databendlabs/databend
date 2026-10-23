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
use indexmap::IndexSet;
use itertools::Itertools;

use crate::ColumnSet;
use crate::optimizer::optimizers::operator::filter::check_float_range;
use crate::optimizer::optimizers::operator::filter::check_int_range;
use crate::optimizer::optimizers::operator::filter::check_uint_range;
use crate::optimizer::optimizers::operator::filter::remove_trivial_type_cast;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

// Limits apply to optional general-predicate inference, never to input predicates
// or the equality conditions needed by DPhyp.
const MAX_FILTER_TARGETS: usize = 32;
const MAX_FILTER_REWRITES: usize = 256;
const MAX_FILTER_NODES: usize = 256;

struct PropagationInfo {
    columns: ColumnSet,
    nodes: usize,
}

impl PropagationInfo {
    // Equality must imply interchangeability inside arbitrary functions. Floats
    // have signed zero; keep them and other representation-sensitive types out.
    fn substitutable(data_type: &DataType) -> bool {
        match data_type {
            DataType::Number(number) => number.is_integer(),
            DataType::Null
            | DataType::Boolean
            | DataType::String
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::Date => true,
            _ => false,
        }
    }

    fn new(expr: &ScalarExpr) -> Option<Self> {
        #[recursive::recursive]
        fn count(expr: &ScalarExpr, remaining: &mut usize) -> Option<()> {
            *remaining = remaining.checked_sub(1)?;
            match expr {
                ScalarExpr::FunctionCall(func) => func
                    .arguments
                    .iter()
                    .try_for_each(|arg| count(arg, remaining)),
                ScalarExpr::CastExpr(cast) => count(&cast.argument, remaining),
                ScalarExpr::BoundColumnRef(_) => Some(()),
                ScalarExpr::ConstantExpr(constant) | ScalarExpr::TypedConstantExpr(constant, _) => {
                    // Scalar equality can conflate differently represented literals,
                    // such as -0.0 and 0.0, even inside a Boolean predicate.
                    PropagationInfo::substitutable(&constant.value.as_ref().infer_data_type())
                        .then_some(())
                }
                _ => None,
            }
        }
        let mut remaining = MAX_FILTER_NODES;
        count(expr, &mut remaining)?;
        (Self::substitutable(&expr.data_type().remove_nullable()) && expr.is_deterministic()).then(
            || Self {
                columns: expr.used_columns(),
                nodes: MAX_FILTER_NODES - remaining,
            },
        )
    }
}

// Updated as facts arrive: each root owns its class's constraints and members;
// non-root constraints are empty. `roots` always points directly to a class root.
// ```sql
//     a = b AND b = c
//     AND a > 1 AND b > 5 AND c < 10
// ```
// ```
//     class: {a, b, c}
//     constraints: > 5, < 10
// ```
#[derive(Default)]
struct InferenceState {
    roots: Vec<usize>,
    members: HashMap<usize, HashSet<usize>>,
    constraints: Vec<ExprConstraints>,
    domains: Vec<Option<NumericDomain>>,
}

impl InferenceState {
    fn add_expr(&mut self, domain: Option<NumericDomain>) {
        let index = self.roots.len();
        self.roots.push(index);
        self.members.insert(index, HashSet::from([index]));
        self.constraints.push(ExprConstraints::default());
        self.domains.push(domain);
    }

    fn constraints_for(&self, index: usize) -> &ExprConstraints {
        &self.constraints[self.roots[index]]
    }

    fn members_for(&self, index: usize) -> impl Iterator<Item = usize> + '_ {
        self.members[&self.roots[index]].iter().copied()
    }

    fn add_predicate(&mut self, index: usize, predicate: Predicate) -> Option<bool> {
        self.constraints[self.roots[index]].try_add(predicate)
    }

    fn merge(&mut self, left: usize, right: usize) -> Option<bool> {
        let mut root = self.roots[left];
        let mut other = self.roots[right];
        // Check every domain before merging constraints: an unconstrained Float
        // member must not bridge conflicting Exact constants into a contradiction.
        let mut domain = None;
        for candidate in [
            self.domains[root],
            self.domains[other],
            self.constraints[root].domain(),
            self.constraints[other].domain(),
        ]
        .into_iter()
        .flatten()
        {
            if domain.is_some_and(|existing| existing != candidate) {
                return None;
            }
            domain = Some(candidate);
        }
        if root == other {
            return Some(true);
        }
        // Move the smaller class so each member changes roots at most O(log n) times.
        // On a tie, prefer the earlier expression for deterministic materialization.
        let root_size = self.members[&root].len();
        let other_size = self.members[&other].len();
        if root_size < other_size || root_size == other_size && root > other {
            std::mem::swap(&mut root, &mut other);
        }
        let members = self.members.remove(&other).unwrap();
        for &index in &members {
            self.roots[index] = root;
        }
        self.members.get_mut(&root).unwrap().extend(members);
        self.domains[root] = domain;

        let constraints = std::mem::take(&mut self.constraints[other]);
        for predicate in constraints.into_predicates() {
            if !self.constraints[root].try_add(predicate)? {
                return Some(false);
            }
        }
        Some(true)
    }

    fn prune_exclusions(&mut self) {
        // Bounds are already normalized. Prune exclusions only once before output,
        // rather than scanning them again after every bound update.
        for &root in self.members.keys() {
            self.constraints[root].prune_exclusions();
        }
    }
}

// The InferFilterOptimizer tries to infer new predicates from existing predicates, for example:
// 1. [A > 1 and A > 5] => [A > 5], [A > 1 and A <= 1 => false], [A = 1 and A < 10] => [A = 1]
// 2. [A = 10 and A = B] => [B = 10]
// 3. [A = B and A = C] => [B = C]
pub struct InferFilterOptimizer<'a> {
    // All ScalarExprs.
    exprs: Vec<ScalarExpr>,
    // The index of ScalarExpr in `exprs`.
    expr_index: HashMap<ScalarExpr, usize>,
    // Equivalence classes and their normalized constraints.
    state: InferenceState,
    // If the whole predicates is false.
    is_falsy: bool,
    // A domain mismatch disables inference without treating the inputs as false.
    inference_disabled: bool,
    // The `join_prop` is used for filter push down join.
    join_prop: Option<JoinProperty<'a>>,
}

impl<'a> InferFilterOptimizer<'a> {
    pub fn new(join_prop: Option<JoinProperty<'a>>) -> Self {
        Self {
            exprs: vec![],
            expr_index: HashMap::new(),
            state: InferenceState::default(),
            is_falsy: false,
            inference_disabled: false,
            join_prop,
        }
    }

    pub fn optimize(&mut self, predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
        if self.inference_disabled {
            return Ok(predicates);
        }
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
            if let ScalarExpr::FunctionCall(func) = predicate
                && predicate.is_deterministic()
            {
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

        if self.inference_disabled {
            return Ok(predicates);
        }
        let new_predicates = if self.is_falsy {
            vec![
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false),
                }
                .into(),
            ]
        } else {
            // Move only the unabsorbed inputs into the remaining-predicate pass.
            let mut remaining_indexes = remaining_indexes.into_iter().peekable();
            let remaining_predicates =
                predicates
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, predicate)| {
                        if remaining_indexes.peek() == Some(&index) {
                            remaining_indexes.next();
                            Some(predicate)
                        } else {
                            None
                        }
                    });
            self.state.prune_exclusions();
            self.derive_predicates()
                .chain(self.derive_remaining_predicates(remaining_predicates))
                .collect()
        };

        Ok(new_predicates)
    }

    #[recursive::recursive]
    fn add_expr(&mut self, expr: &ScalarExpr) -> usize {
        if let Some(&index) = self.expr_index.get(expr) {
            return index;
        }
        let index = self.exprs.len();
        self.expr_index.insert(expr.clone(), index);
        self.exprs.push(expr.clone());
        self.state
            .add_expr(NumericDomain::of_type(expr.data_type().as_ref()));
        index
    }

    pub fn add_equal_expr(&mut self, left: &ScalarExpr, right: &ScalarExpr) -> bool {
        // Separate evaluations of volatile expressions cannot form an equivalence
        // class, even when their expression trees happen to be identical.
        if !left.is_deterministic() || !right.is_deterministic() {
            return false;
        }
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
        let left = self.add_expr(left);
        let right = self.add_expr(right);
        match self.state.merge(left, right) {
            Some(satisfiable) => self.is_falsy |= !satisfiable,
            None => self.inference_disabled = true,
        }
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
        let index = self.add_expr(expr);
        let Some(satisfiable) = self.state.add_predicate(index, predicate) else {
            return false;
        };
        self.is_falsy |= !satisfiable;
        true
    }

    fn derive_predicates(&self) -> impl Iterator<Item = ScalarExpr> + '_ {
        // Expression positions are the indexes recorded in `expr_index`.
        let constraints = self
            .exprs
            .iter()
            .enumerate()
            .flat_map(move |(index, expr)| {
                self.state
                    .constraints_for(index)
                    .predicates()
                    .filter_map(move |predicate| {
                        // A transitive class can connect types whose pairwise comparison
                        // would round or overflow. Only emit safely comparable constraints.
                        if !common_super_type_with_conversion(
                            expr.data_type().as_ref(),
                            predicate.constant.value.as_ref().infer_data_type(),
                        )
                        .is_some_and(|conversion| conversion.is_safe_for_equality_inference())
                        {
                            return None;
                        }
                        let return_type =
                            ScalarExpr::passthrough_nullable_type(DataType::Boolean, [expr]);
                        Some(ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: String::from(predicate.op.to_func_name()),
                            params: vec![],
                            arguments: vec![
                                expr.clone(),
                                ScalarExpr::ConstantExpr(predicate.constant.clone()),
                            ],
                            return_type: Box::new(return_type),
                        }))
                    })
            });

        // DPhyp still needs all pairs. Sort member indexes for stable output, but
        // construct equality expressions lazily rather than collecting a Vec.
        let equalities = (0..self.exprs.len())
            .filter(move |&index| self.state.roots[index] == index)
            .flat_map(move |index| self.state.members_for(index).sorted().tuple_combinations())
            .filter_map(move |(left, right): (usize, usize)| {
                let (left, right) = (&self.exprs[left], &self.exprs[right]);
                if !common_super_type_with_conversion(
                    left.data_type().as_ref(),
                    right.data_type().as_ref(),
                )
                .is_some_and(|conversion| conversion.is_safe_for_equality_inference())
                {
                    return None;
                }
                let return_type =
                    ScalarExpr::passthrough_nullable_type(DataType::Boolean, [left, right]);
                Some(ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: String::from(ComparisonOp::Equal.to_func_name()),
                    params: vec![],
                    arguments: vec![left.clone(), right.clone()],
                    return_type: Box::new(return_type),
                }))
            });
        constraints.chain(equalities)
    }

    #[recursive::recursive]
    fn find_equal_expr(&self, expr: &ScalarExpr) -> Option<usize> {
        if let Some(&index) = self.expr_index.get(expr)
            && self.state.members[&self.state.roots[index]].len() > 1
        {
            return Some(index);
        }
        match expr {
            ScalarExpr::FunctionCall(func) => func
                .arguments
                .iter()
                .find_map(|arg| self.find_equal_expr(arg)),
            ScalarExpr::CastExpr(cast) => self.find_equal_expr(&cast.argument),
            _ => None,
        }
    }

    // ```sql
    //   a = b AND b = c
    //   AND a LIKE '%foo%'
    // ```
    // return [{a}, {b}, {c}]
    fn filter_targets(&self, predicate: &ScalarExpr, source: &ColumnSet) -> Vec<ColumnSet> {
        if source.is_empty() {
            return vec![];
        }
        let join_target = if let Some(join) = &self.join_prop {
            let on_left = source.is_subset(join.left_columns);
            let on_right = source.is_subset(join.right_columns);
            if on_left == on_right {
                return vec![];
            }
            let target = if on_left {
                join.right_columns
            } else {
                join.left_columns
            };
            // Join facts need not hold on unmatched preserved-side rows. Only
            // derive filters for the opposite input, never the source input.
            Some(target)
        } else {
            None
        };
        if source.len() > 1 {
            // Rewrite onto the complete input, not combinations of its columns.
            return join_target.cloned().into_iter().collect();
        }
        let Some(index) = self.find_equal_expr(predicate) else {
            return vec![];
        };
        let source_type = self.exprs[index].data_type().remove_nullable();
        // Select a stable target prefix before skipping the source column.
        self.state
            .members_for(index)
            .filter_map(|member| {
                let info = PropagationInfo::new(&self.exprs[member])?;
                (info.columns.len() == 1
                    && self.exprs[member].data_type().remove_nullable() == source_type
                    && join_target.is_none_or(|target| info.columns.is_subset(target)))
                .then_some(info.columns)
            })
            .sorted()
            .dedup()
            .take(MAX_FILTER_TARGETS)
            .collect()
    }

    fn derive_remaining_predicates(
        &self,
        predicates: impl IntoIterator<Item = ScalarExpr>,
    ) -> impl Iterator<Item = ScalarExpr> {
        // Replace expressions using members available on one target input.
        struct ReplaceScalarExpr<'a> {
            optimizer: &'a InferFilterOptimizer<'a>,
            target: ColumnSet,
            replacement_limit: usize,
            can_replace: bool,
        }

        impl ReplaceScalarExpr<'_> {
            fn reset(&mut self, target: ColumnSet, replacement_limit: usize) {
                self.target = target;
                self.replacement_limit = replacement_limit;
                self.can_replace = true;
            }
        }

        impl VisitorMut<'_> for ReplaceScalarExpr<'_> {
            #[recursive::recursive]
            fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
                if let Some(&index) = self.optimizer.expr_index.get(expr) {
                    let data_type = expr.data_type().remove_nullable();
                    // Prefer the smallest safe member on this target to limit expression
                    // growth; break ties by index rather than HashSet iteration order.
                    // This is a local heuristic, not a cost-based choice.
                    let member = self
                        .optimizer
                        .state
                        .members_for(index)
                        .filter_map(|member| {
                            let candidate = &self.optimizer.exprs[member];
                            if candidate.data_type().remove_nullable() != data_type {
                                return None;
                            }
                            let info = PropagationInfo::new(candidate)?;
                            (info.nodes <= self.replacement_limit
                                && info.columns.is_subset(&self.target))
                            .then_some((info.nodes, member))
                        })
                        .min();
                    if let Some((_, member)) = member {
                        *expr = self.optimizer.exprs[member].clone();
                        return Ok(());
                    }
                }
                match expr {
                    ScalarExpr::FunctionCall(expr) => self.visit_function_call(expr),
                    ScalarExpr::CastExpr(expr) => self.visit_cast_expr(expr),
                    ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _) => Ok(()),
                    ScalarExpr::BoundColumnRef(column)
                        if self.target.contains(&column.column.index) =>
                    {
                        Ok(())
                    }
                    ScalarExpr::BoundColumnRef(_)
                    | ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::LambdaFunction(_)
                    | ScalarExpr::SubqueryExpr(_)
                    | ScalarExpr::UDFCall(_)
                    | ScalarExpr::UDFLambdaCall(_)
                    | ScalarExpr::UDAFCall(_)
                    | ScalarExpr::AsyncFunctionCall(_) => {
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
            optimizer: self,
            target: ColumnSet::new(),
            replacement_limit: 0,
            can_replace: true,
        };
        // Preserve occurrences we cannot safely rewrite or deduplicate.
        let mut result_predicates: IndexSet<_> = predicates
            .into_iter()
            .enumerate()
            .map(|(index, predicate)| {
                let occurrence = PropagationInfo::new(&predicate).is_none().then_some(index);
                (predicate, occurrence)
            })
            .collect();
        let mut attempts = 0;
        // The range is fixed before insertion, so only original predicates are sources.
        for index in 0..result_predicates.len() {
            if attempts == MAX_FILTER_REWRITES {
                break;
            }
            let (predicate, _) = result_predicates.get_index(index).unwrap();
            let Some(source) = PropagationInfo::new(predicate) else {
                continue;
            };
            let targets = self.filter_targets(predicate, &source.columns);
            for target in targets {
                if attempts == MAX_FILTER_REWRITES {
                    break;
                }
                if target == source.columns {
                    continue;
                }
                attempts += 1;
                // At most source.nodes replacements: bound size before cloning.
                replace.reset(target, MAX_FILTER_NODES / source.nodes);
                let (predicate, _) = result_predicates.get_index(index).unwrap();
                let mut new_predicate = predicate.clone();
                if replace.visit(&mut new_predicate).is_err() || !replace.can_replace {
                    continue;
                }
                if new_predicate != *predicate
                    && PropagationInfo::new(&new_predicate)
                        .is_some_and(|info| info.columns.is_subset(&replace.target))
                {
                    result_predicates.insert((new_predicate, None));
                }
            }
        }
        result_predicates
            .into_iter()
            .map(|(predicate, _)| predicate)
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
    use crate::Symbol;
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
        let index = optimizer.add_expr(&col);
        optimizer.state.constraints[index] = incompatible;
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
        let a = optimizer.add_expr(&a);
        let b = optimizer.add_expr(&b);
        let f = optimizer.add_expr(&f);
        for index in [a, b] {
            if optimizer.state.merge(index, f).is_none() {
                optimizer.inference_disabled = true;
            }
        }
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
    fn test_normalized_inference_state() {
        let columns: Vec<_> = (0..4)
            .map(|index| column(index, DataType::Number(NumberDataType::Int64)))
            .collect();
        let mut optimizer = InferFilterOptimizer::new(None);
        for (index, op, value) in [
            (0, ComparisonOp::GT, 1),
            (1, ComparisonOp::GT, 5),
            (2, ComparisonOp::LT, 10),
            (3, ComparisonOp::Equal, 42),
        ] {
            optimizer.add_expr_predicate(&columns[index], predicate(op, int(value)));
        }
        // Equality immediately merges the existing bounds, without a rebuild pass.
        assert!(optimizer.add_equal_expr(&columns[1], &columns[2]));
        assert!(std::ptr::eq(
            optimizer.state.constraints_for(1),
            optimizer.state.constraints_for(2)
        ));
        assert_eq!(
            optimizer
                .state
                .constraints_for(1)
                .predicates()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                predicate(ComparisonOp::GT, int(5)),
                predicate(ComparisonOp::LT, int(10))
            ]
        );
        // Merge the smaller singleton into the larger class, whose root is not
        // the first member. No normalization call is needed before reading it.
        assert!(optimizer.add_equal_expr(&columns[0], &columns[2]));
        assert!(!optimizer.is_falsy);
        let state = &optimizer.state;
        let root = state.roots[0];
        assert_ne!(root, 0);
        assert_eq!(state.members[&root], HashSet::from([0, 1, 2]));
        assert_eq!(state.members[&state.roots[3]], HashSet::from([3]));
        let expected = vec![
            predicate(ComparisonOp::GT, int(5)),
            predicate(ComparisonOp::LT, int(10)),
        ];
        for index in 0..3 {
            assert!(std::ptr::eq(
                state.constraints_for(index),
                &state.constraints[root]
            ));
            if index != root {
                assert!(matches!(state.constraints[index], ExprConstraints::Empty));
            }
            assert_eq!(
                state
                    .constraints_for(index)
                    .predicates()
                    .cloned()
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(
            state
                .constraints_for(3)
                .predicates()
                .cloned()
                .collect::<Vec<_>>(),
            vec![predicate(ComparisonOp::Equal, int(42))]
        );

        // Only redundant exclusion cleanup is deferred until materialization.
        optimizer.add_expr_predicate(&columns[2], predicate(ComparisonOp::NotEqual, int(20)));
        optimizer.state.prune_exclusions();
        // Materialization only reads the state and preserves the full equality
        // closure expected by existing consumers.
        let emitted = optimizer.derive_predicates().collect::<Vec<_>>();
        assert_eq!(emitted.len(), 10);
        for (left, right) in [(0, 1), (0, 2), (1, 2)] {
            assert!(emitted.contains(&comparison(
                columns[left].clone(),
                ComparisonOp::Equal,
                columns[right].clone()
            )));
        }
        assert!(optimizer.derive_predicates().eq(emitted.iter().cloned()));
        optimizer.state.prune_exclusions();
        assert!(optimizer.derive_predicates().eq(emitted.iter().cloned()));
        // Repeated equality is a no-op, including for class membership.
        assert!(optimizer.add_equal_expr(&columns[2], &columns[0]));
        assert!(optimizer.derive_predicates().eq(emitted.iter().cloned()));
        assert_eq!(optimizer.state.members.len(), 2);

        // A predicate on a non-root updates the class immediately, without
        // losing its previous constraints or waiting for a normalization pass.
        optimizer.add_expr_predicate(&columns[2], predicate(ComparisonOp::LT, int(8)));
        assert_eq!(
            optimizer
                .state
                .constraints_for(0)
                .predicates()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                predicate(ComparisonOp::GT, int(5)),
                predicate(ComparisonOp::LT, int(8))
            ]
        );
    }

    #[test]
    fn test_redundant_equalities_do_not_accumulate() {
        let columns: Vec<_> = (0..100)
            .map(|index| column(index, DataType::Number(NumberDataType::Int64)))
            .collect();
        let mut optimizer = InferFilterOptimizer::new(None);
        // Dense input and duplicate edges update one class, not a stored adjacency
        // graph. No pairwise ScalarExpr output is constructed while collecting it.
        for left in 0..columns.len() {
            for right in left + 1..columns.len() {
                assert!(optimizer.add_equal_expr(&columns[left], &columns[right]));
                assert!(optimizer.add_equal_expr(&columns[right], &columns[left]));
            }
        }
        assert_eq!(optimizer.exprs.len(), 100);
        assert_eq!(optimizer.state.roots, vec![0; 100]);
        assert_eq!(optimizer.state.members.len(), 1);
        assert_eq!(optimizer.state.members[&0].len(), 100);
        assert_eq!(optimizer.derive_predicates().count(), 100 * 99 / 2);
    }

    fn function(name: &str, arguments: Vec<ScalarExpr>) -> ScalarExpr {
        let mut function = FunctionCall {
            span: None,
            func_name: name.to_string(),
            params: vec![],
            arguments,
            return_type: Box::new(DataType::Boolean),
        };
        function.refresh_return_type().unwrap();
        function.into()
    }

    fn like(column: &ScalarExpr) -> ScalarExpr {
        function("like", vec![
            column.clone(),
            ConstantExpr {
                span: None,
                value: Scalar::String("%foo%".to_string()),
            }
            .into(),
        ])
    }

    #[test]
    fn test_filter_propagation_uses_complete_class() {
        let columns: Vec<_> = (0..3)
            .map(|index| {
                let data_type = if index == 1 {
                    DataType::Nullable(Box::new(DataType::String))
                } else {
                    DataType::String
                };
                column(index, data_type)
            })
            .collect();
        for edges in [[(0, 1), (1, 2)], [(1, 2), (0, 1)], [(2, 1), (1, 0)]] {
            let mut optimizer = InferFilterOptimizer::new(None);
            for (left, right) in edges {
                assert!(optimizer.add_equal_expr(&columns[left], &columns[right]));
            }
            for source in &columns {
                let predicates = optimizer
                    .derive_remaining_predicates([like(source)])
                    .collect::<Vec<_>>();
                assert_eq!(predicates.len(), 3);
                for target in &columns {
                    assert!(predicates.contains(&like(target)));
                }
                assert!(
                    optimizer
                        .derive_remaining_predicates(predicates.iter().cloned())
                        .eq(predicates.iter().cloned())
                );
            }
            let repeated = function("concat", vec![columns[0].clone(), columns[0].clone()]);
            let predicates = optimizer
                .derive_remaining_predicates([like(&repeated)])
                .collect::<Vec<_>>();
            assert_eq!(predicates.len(), 3);
            for target in &columns {
                assert!(predicates.contains(&like(&function("concat", vec![
                    target.clone(),
                    target.clone()
                ]))));
            }
        }
    }

    #[test]
    fn test_filter_propagation_rewrites_whole_join_input() {
        let columns: Vec<_> = (0..5)
            .map(|index| column(index, DataType::String))
            .collect();
        let left = ColumnSet::from([Symbol::from_field_index(0), Symbol::from_field_index(1)]);
        let right = ColumnSet::from([Symbol::from_field_index(2), Symbol::from_field_index(3)]);
        let mut optimizer = InferFilterOptimizer::new(Some(JoinProperty::new(&left, &right)));
        for (a, b) in [(0, 4), (4, 2), (1, 3)] {
            assert!(optimizer.add_equal_expr(&columns[a], &columns[b]));
        }
        let original = like(&function("concat", vec![
            columns[0].clone(),
            columns[1].clone(),
        ]));
        let expected = like(&function("concat", vec![
            columns[2].clone(),
            columns[3].clone(),
        ]));
        assert!(
            optimizer
                .derive_remaining_predicates([original.clone()])
                .eq([original, expected])
        );
        assert!(
            optimizer
                .derive_remaining_predicates([like(&columns[0])])
                .eq([like(&columns[0]), like(&columns[2])])
        );
        // An unmapped reference cannot leave a partially rewritten predicate.
        let original = like(&function("concat", vec![
            columns[0].clone(),
            columns[1].clone(),
        ]));
        let mut partial = InferFilterOptimizer::new(Some(JoinProperty::new(&left, &right)));
        assert!(partial.add_equal_expr(&columns[0], &columns[2]));
        assert!(
            partial
                .derive_remaining_predicates([original.clone()])
                .eq([original])
        );
        let mut same_input = InferFilterOptimizer::new(Some(JoinProperty::new(&left, &right)));
        assert!(same_input.add_equal_expr(&columns[0], &columns[2]));
        assert!(same_input.add_equal_expr(&columns[1], &columns[2]));
        assert!(
            same_input
                .derive_remaining_predicates([like(&columns[0])])
                .eq([like(&columns[0]), like(&columns[2])])
        );
        // A mixed-input or out-of-region source must not be treated as one-sided.
        for original in [
            like(&function("concat", vec![
                columns[0].clone(),
                columns[2].clone(),
            ])),
            like(&function("concat", vec![
                columns[0].clone(),
                columns[4].clone(),
            ])),
        ] {
            assert!(
                optimizer
                    .derive_remaining_predicates([original.clone()])
                    .eq([original])
            );
        }
    }

    #[test]
    fn test_filter_propagation_budget_is_stable() {
        let columns: Vec<_> = (0..40)
            .map(|index| column(index, DataType::String))
            .collect();
        let mut optimizer = InferFilterOptimizer::new(None);
        for pair in columns.windows(2) {
            assert!(optimizer.add_equal_expr(&pair[0], &pair[1]));
        }
        let original = like(&columns[39]);
        let result = optimizer
            .derive_remaining_predicates([original.clone()])
            .collect::<Vec<_>>();
        assert_eq!(result.len(), MAX_FILTER_TARGETS + 1);
        assert!(result.contains(&original));
        for column in columns.iter().take(MAX_FILTER_TARGETS) {
            assert!(result.contains(&like(column)));
        }
        assert!(
            optimizer
                .derive_remaining_predicates(result.iter().cloned())
                .eq(result.iter().cloned())
        );
        let originals: Vec<_> = (0..20)
            .map(|index| {
                let suffix: ScalarExpr = ConstantExpr {
                    span: None,
                    value: Scalar::String(index.to_string()),
                }
                .into();
                like(&function("concat", vec![columns[0].clone(), suffix]))
            })
            .collect();
        let bounded = optimizer
            .derive_remaining_predicates(originals.iter().cloned())
            .collect::<Vec<_>>();
        assert_eq!(bounded.len(), originals.len() + MAX_FILTER_REWRITES);
        assert!(
            originals
                .iter()
                .all(|predicate| bounded.contains(predicate))
        );

        let mut large = columns[0].clone();
        for _ in 0..MAX_FILTER_NODES {
            large = function("concat", vec![large, columns[0].clone()]);
        }
        let original = like(&large);
        assert!(
            optimizer
                .derive_remaining_predicates([original.clone()])
                .eq([original])
        );
        // Both inputs fit separately, but substitution would exceed the output budget.
        let mut expanding = InferFilterOptimizer::new(None);
        let target = function("concat", vec![columns[1].clone(); 20]);
        assert!(expanding.add_equal_expr(&columns[0], &target));
        let original = like(&function("concat", vec![columns[0].clone(); 20]));
        assert!(
            expanding
                .derive_remaining_predicates([original.clone()])
                .eq([original])
        );
    }

    #[test]
    fn test_filter_propagation_result_uniqueness() {
        let columns = [column(0, DataType::String), column(1, DataType::String)];
        let mut optimizer = InferFilterOptimizer::new(None);
        assert!(optimizer.add_equal_expr(&columns[0], &columns[1]));
        let original = like(&columns[0]);
        let target = like(&columns[1]);
        assert!(
            optimizer
                .derive_remaining_predicates([original.clone(), original.clone(), target.clone()])
                .eq([original, target])
        );

        let float = column(2, DataType::Number(NumberDataType::Float64));
        let predicate = |zero: f64| {
            let zero = ConstantExpr {
                span: None,
                value: Scalar::Number(NumberScalar::Float64(OrderedFloat(zero))),
            }
            .into();
            function("like", vec![
                function("to_string", vec![function("plus", vec![
                    float.clone(),
                    zero,
                ])]),
                ConstantExpr {
                    span: None,
                    value: Scalar::String("-%".to_string()),
                }
                .into(),
            ])
        };
        let negative = predicate(-0.0);
        let positive = predicate(0.0);
        // ScalarExpr equality conflates signed-zero literals, but formatting can
        // distinguish their results. Such originals must not be deduplicated.
        assert_eq!(negative, positive);
        assert!(negative.is_deterministic());
        assert!(PropagationInfo::new(&negative).is_none());
        assert_eq!(
            optimizer
                .derive_remaining_predicates([negative, positive])
                .count(),
            2
        );
    }

    #[test]
    fn test_filter_propagation_guards() {
        let columns = [
            column(0, DataType::Number(NumberDataType::Int32)),
            column(1, DataType::Number(NumberDataType::Int64)),
        ];
        let mut optimizer = InferFilterOptimizer::new(None);
        assert!(optimizer.add_equal_expr(&columns[0], &columns[1]));
        let original = function("is_not_null", vec![columns[0].clone()]);
        assert!(
            optimizer
                .derive_remaining_predicates([original.clone()])
                .eq([original])
        );

        let random = function("rand", vec![]);
        assert!(!optimizer.add_equal_expr(&random, &random));
        let columns = [
            column(0, DataType::Number(NumberDataType::Float64)),
            column(1, DataType::Number(NumberDataType::Float64)),
        ];
        let mut optimizer = InferFilterOptimizer::new(None);
        assert!(optimizer.add_equal_expr(&columns[0], &columns[1]));
        let original = function("is_not_null", vec![columns[0].clone()]);
        assert!(
            optimizer
                .derive_remaining_predicates([original.clone()])
                .eq([original])
        );
        let noisy = function("plus", vec![columns[0].clone(), random.clone()]);
        assert!(!optimizer.add_equal_expr(&noisy, &columns[1]));
        let original = comparison(columns[0].clone(), ComparisonOp::GT, random);
        assert_eq!(
            optimizer.optimize(vec![original.clone()]).unwrap().last(),
            Some(&original)
        );
        let duplicates = [original.clone(), original];
        assert!(
            optimizer
                .derive_remaining_predicates(duplicates.iter().cloned())
                .eq(duplicates.iter().cloned())
        );
    }

    #[test]
    fn test_incremental_class_contradictions() {
        let left = column(0, DataType::Number(NumberDataType::Int64));
        let right = column(1, DataType::Number(NumberDataType::Int64));
        // Detect both conflicting class merges and predicates added after a merge.
        for equality_first in [false, true] {
            let mut optimizer = InferFilterOptimizer::new(None);
            if equality_first {
                assert!(optimizer.add_equal_expr(&left, &right));
            }
            optimizer.add_expr_predicate(&left, predicate(ComparisonOp::GTE, int(5)));
            assert!(!optimizer.is_falsy);
            optimizer.add_expr_predicate(&right, predicate(ComparisonOp::LT, int(5)));
            if !equality_first {
                assert!(!optimizer.is_falsy);
                assert!(optimizer.add_equal_expr(&left, &right));
            }
            assert!(optimizer.is_falsy);
        }
    }

    #[test]
    fn test_incremental_class_permutations() {
        let columns: Vec<_> = (0..3)
            .map(|index| column(index, DataType::Number(NumberDataType::Int64)))
            .collect();
        let predicates = [
            comparison(columns[0].clone(), ComparisonOp::Equal, columns[1].clone()),
            comparison(columns[1].clone(), ComparisonOp::Equal, columns[2].clone()),
            filter(&columns[0], ComparisonOp::GT, int(1)),
            filter(&columns[1], ComparisonOp::GT, int(5)),
            filter(&columns[2], ComparisonOp::LT, int(10)),
        ];
        for permutation in predicates.iter().permutations(predicates.len()) {
            let result = InferFilterOptimizer::new(None)
                .optimize(permutation.into_iter().cloned().collect())
                .unwrap();
            assert_eq!(result.len(), 9);
            for column in &columns {
                assert!(result.contains(&filter(column, ComparisonOp::GT, int(5))));
                assert!(result.contains(&filter(column, ComparisonOp::LT, int(10))));
            }
            for (left, right) in [(0, 1), (0, 2), (1, 2)] {
                assert!(
                    result.contains(&comparison(
                        columns[left].clone(),
                        ComparisonOp::Equal,
                        columns[right].clone()
                    )) || result.contains(&comparison(
                        columns[right].clone(),
                        ComparisonOp::Equal,
                        columns[left].clone()
                    ))
                );
            }
        }
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
