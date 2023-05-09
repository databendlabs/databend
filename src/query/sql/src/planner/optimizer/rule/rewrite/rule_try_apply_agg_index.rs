// Copyright 2023 Datafuse Labs
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

use common_exception::Result;
use common_expression::RawExpr;
use common_expression::Scalar;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::FunctionCall;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

pub struct RuleTryApplyAggIndex {
    id: RuleID,
    patterns: Vec<SExpr>,
    metadata: MetadataRef,
}

impl RuleTryApplyAggIndex {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::TryApplyAggIndex,
            metadata,
            patterns: vec![
                // Expression
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Scan,
                        }
                        .into(),
                    ),
                ),
                // Expression
                //     |
                //   Filter
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Filter,
                        }
                        .into(),
                        SExpr::create_leaf(
                            PatternPlan {
                                plan_type: RelOp::Scan,
                            }
                            .into(),
                        ),
                    ),
                ),
                // Aggregation
                //     |
                // Expression
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::EvalScalar,
                        }
                        .into(),
                        SExpr::create_leaf(
                            PatternPlan {
                                plan_type: RelOp::Scan,
                            }
                            .into(),
                        ),
                    ),
                ),
                // Aggregation
                //     |
                // Expression
                //     |
                //   Filter
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::EvalScalar,
                        }
                        .into(),
                        SExpr::create_unary(
                            PatternPlan {
                                plan_type: RelOp::Filter,
                            }
                            .into(),
                            SExpr::create_leaf(
                                PatternPlan {
                                    plan_type: RelOp::Scan,
                                }
                                .into(),
                            ),
                        ),
                    ),
                ),
            ],
        }
    }
}

impl Rule for RuleTryApplyAggIndex {
    fn id(&self) -> RuleID {
        self.id
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        if s_expr.match_pattern(&self.patterns[0]) || s_expr.match_pattern(&self.patterns[1]) {
            // EvalScalar - Scan
            // EvalScalar - Filter - Scan
            self.without_aggregation(s_expr)
        } else {
            // Aggregate - EvalScalar - Scan
            // Aggregate - EvalScalar - Filter - Scan
            self.with_aggregation(s_expr)
        }
    }
}

/// [`Range`] is to represent the value range of a column according to the predicates.
///
/// Notes that only conjunctions will be parsed, and disjunctions will be ignored.
#[derive(Default)]
struct Range<'a> {
    min: Option<&'a Scalar>,
    min_close: bool,
    max: Option<&'a Scalar>,
    max_close: bool,
}

impl<'a> Range<'a> {
    fn new(val: &'a Scalar, op: &str) -> Self {
        let mut range = Range::default();
        range.set_bound(val, op);
        range
    }

    #[inline]
    fn set_bound(&mut self, val: &'a Scalar, op: &str) {
        match op {
            "gt" => self.set_min(val, false),
            "gte" => self.set_min(val, true),
            "lt" => self.set_max(val, false),
            "lte" => self.set_max(val, true),
            _ => unreachable!(),
        }
    }

    #[inline]
    fn set_min(&mut self, val: &'a Scalar, close: bool) {
        match self.min {
            Some(min) if val < min => {
                self.min = Some(val);
                self.min_close = close;
            }
            Some(min) if val == min => {
                self.min_close = self.min_close || close;
            }
            None => {
                self.min = Some(val);
                self.min_close = close;
            }
            _ => {}
        }
    }

    #[inline]
    fn set_max(&mut self, val: &'a Scalar, close: bool) {
        match self.max {
            Some(max) if val > max => {
                self.max = Some(val);
                self.max_close = close;
            }
            Some(max) if val == max => {
                self.max_close = self.max_close || close;
            }
            None => {
                self.max = Some(val);
                self.max_close = close;
            }
            _ => {}
        }
    }

    #[inline]
    fn is_valid(&self) -> bool {
        match (self.min, self.max) {
            (Some(min), Some(max)) => min < max || (min == max && self.min_close && self.max_close),
            _ => true,
        }
    }

    /// If current range contains the other range.
    #[inline]
    fn contains(&self, other: &Range) -> bool {
        if !self.is_valid() || !other.is_valid() {
            return false;
        }

        match (self.min, other.min) {
            (Some(m1), Some(m2)) => {
                if m1 > m2 || (m1 == m2 && !self.min_close && other.min_close) {
                    return false;
                }
            }
            (Some(_), None) => {
                return false;
            }
            _ => {}
        }

        match (self.max, other.max) {
            (Some(m1), Some(m2)) => {
                if m1 < m2 || (m1 == m2 && !self.max_close && other.max_close) {
                    return false;
                }
            }
            (Some(_), None) => {
                return false;
            }
            _ => {}
        }

        true
    }
}

/// Each element is the operands of each equal predicate.
type EqualPredicates<'a> = Vec<(&'a ScalarExpr, &'a ScalarExpr)>;
/// Each element is the operands and the operator of each range predicate.
/// Currently, range predicates should have one column and one constant.
type RangePredicates<'a> = HashMap<IndexType, Range<'a>>;
/// Each element is the full expression of each other predicate .
type OtherPredicates<'a> = Vec<&'a ScalarExpr>;

type Predicates<'a> = (
    EqualPredicates<'a>,
    RangePredicates<'a>,
    OtherPredicates<'a>,
);

impl RuleTryApplyAggIndex {
    fn with_aggregation(&self, s_expr: &SExpr) -> Result<()> {
        todo!("agg index")
    }

    fn without_aggregation(&self, s_expr: &SExpr) -> Result<()> {
        if let RelOperator::EvalScalar(expr) = s_expr.plan() {
            match s_expr.child(0)?.plan() {
                RelOperator::Filter(filter) => {
                    // 1. Check if the outputs of query and index are matched.
                    // TODO: check `expr`.
                    // 2. Check if the predicates of query and index are matched.
                    // TODO: check `filter`.
                    todo!("agg index");
                }
                RelOperator::Scan(scan) => {
                    todo!("agg index");
                }
                _ => unreachable!(),
            }
            return Ok(());
        }
        unreachable!()
    }

    fn get_index_plans(&self) -> Vec<SExpr> {
        todo!("agg index")
    }

    /// Collect three kinds of predicates:
    /// 1. `Equal`. Such as `column = constant`.
    /// 2. `Range`. Such as `column op constant`m `op` should be `gt`, `gte`, `lt` or `lte`.
    /// 3. `Other`. Predicates except `Equal` and `Range`.
    fn distinguish_predicates(predicates: &[ScalarExpr]) -> Predicates<'_> {
        let mut equal_predicates = vec![];
        let mut range_predicates = HashMap::new();
        let mut other_predicates = vec![];

        for pred in predicates {
            match pred {
                ScalarExpr::FunctionCall(FunctionCall {
                    func_name,
                    arguments,
                    ..
                }) => match func_name.as_str() {
                    "eq" => {
                        let left = &arguments[0];
                        let right = &arguments[1];
                        equal_predicates.push((left, right));
                    }
                    "gt" | "gte" | "lt" | "lte" => {
                        let left = &arguments[0];
                        let right = &arguments[1];
                        match (left, right) {
                            (ScalarExpr::BoundColumnRef(col), ScalarExpr::ConstantExpr(val)) => {
                                range_predicates
                                    .entry(col.column.index)
                                    .and_modify(|v: &mut Range| v.set_bound(&val.value, func_name))
                                    .or_insert(Range::new(&val.value, func_name));
                            }
                            (ScalarExpr::ConstantExpr(val), ScalarExpr::BoundColumnRef(col)) => {
                                range_predicates
                                    .entry(col.column.index)
                                    .and_modify(|v: &mut Range| v.set_bound(&val.value, func_name))
                                    .or_insert(Range::new(
                                        &val.value,
                                        &Self::reverse_op(func_name),
                                    ));
                            }
                            _ => other_predicates.push(pred),
                        }
                    }
                    _ => other_predicates.push(pred),
                },
                _ => other_predicates.push(pred),
            }
        }

        (equal_predicates, range_predicates, other_predicates)
    }

    #[inline(always)]
    fn reverse_op(op: &str) -> String {
        match op {
            "gt" => "lt".to_string(),
            "gte" => "lte".to_string(),
            "lt" => "gt".to_string(),
            "lte" => "gte".to_string(),
            _ => op.to_string(),
        }
    }

    /// Check if equal predicates of the index fit the query.
    ///
    /// For each predicate of index, it should be in the query.
    fn check_predicates_equal(query: &EqualPredicates, index: &EqualPredicates) -> bool {
        // TBD: if there is a better way.
        for (left, right) in index {
            if !query
                .iter()
                .any(|(l, r)| (l == left && r == right) || (l == right && r == left))
            {
                return false;
            }
        }
        true
    }

    /// Check if other predicates of the index fit the query.
    ///
    /// For each predicate of index, the column side should be found in the query.
    /// And the range of the predicate in index should be more wide than the one in query.
    ///
    /// For example:
    ///
    /// - Valid: query predicate: `a > 1`, index predicate: `a > 0`
    /// - Invalid: query predicate: `a > 1`, index predicate: `a > 2`
    fn check_predicates_range(query: &RangePredicates, index: &RangePredicates) -> bool {
        for (col, index_range) in index {
            if let Some(query_range) = query.get(col) {
                if !index_range.contains(query_range) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    /// Check if other predicates of the index fit the query.
    ///
    /// For each predicate of index, it should be in the query.
    fn check_predicates_other(query: &OtherPredicates, index: &OtherPredicates) -> bool {
        // TBD: if there is a better way.
        for pred in index {
            if !query.iter().any(|p| p == pred) {
                return false;
            }
        }
        true
    }
}
