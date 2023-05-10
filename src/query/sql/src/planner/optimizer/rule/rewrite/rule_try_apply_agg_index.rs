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

use common_exception::Result;
use common_expression::Scalar;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

pub struct RuleTryApplyAggIndex {
    id: RuleID,
    patterns: Vec<SExpr>,
    _metadata: MetadataRef,
}

impl RuleTryApplyAggIndex {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::TryApplyAggIndex,
            _metadata: metadata,
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
                // Expression
                //     |
                // Aggregation
                //     |
                // Expression
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
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
                ),
                // Expression
                //     |
                // Aggregation
                //     |
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
        let index_plans = self.get_index_plans();
        if index_plans.is_empty() {
            return Ok(());
        }

        let query_info = Self::collect_information(s_expr)?;
        let query_predicates = query_info.predicates.map(Self::distinguish_predicates);

        // Search all index plans, find the first matched index to rewrite the query.
        for plan in index_plans.iter() {
            let index_info = Self::collect_information(plan)?;
            // 1. Check selection and aggregation.
            // match (&query_info.aggregation, &index_info.aggregation) {
            //     (Some((agg_q, args_q)), Some((agg_i, args_i))) => {
            //         // Group items should be the same.
            //     }
            //     (None, Some(_)) => {
            //         // Not matched.
            //         continue;
            //     }
            //     (Some((agg, args)), None) => {
            //         // Query's aggregation arguments should be found in index's output.
            //         for (_, scalar) in args {}
            //     }
            //     (None, None) => {}
            // }

            // All selected columns in query should be contained in index.
            let mut flag = true;
            for item in query_info.selection.items.iter() {
                if !index_info.check_select_item(&item.scalar, &query_info.aggregation) {
                    flag = false;
                    break;
                }
            }
            if !flag {
                continue;
            }

            // 2. Check filter predicates.
            let output_bound_cols = index_info.output_bound_cols();
            let index_predicates = index_info.predicates.map(Self::distinguish_predicates);
            match (&query_predicates, &index_predicates) {
                (Some((qe, qr, qo)), Some((ie, ir, io))) => {
                    if !Self::check_predicates_equal(qe, ie) {
                        continue;
                    }
                    if !Self::check_predicates_other(qo, io) {
                        continue;
                    }
                    if !Self::check_predicates_range(qr, ir, &output_bound_cols) {
                        continue;
                    }
                }
                (Some(_), _) => { /* Matched */ }
                (None, _) => { /* Not matched */ }
            }
        }

        // Do nothing now.
        // TODO(agg index)
        let mut result = s_expr.clone();
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }
}

/// [`Range`] is to represent the value range of a column according to the predicates.
///
/// Notes that only conjunctions will be parsed, and disjunctions will be ignored.
#[derive(Default, PartialEq)]
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

type AggregationInfo<'a> = (&'a Aggregate, HashMap<IndexType, &'a ScalarExpr>);

// Record information helping to rewrite the query plan.
struct RewriteInfomartion<'a> {
    table_index: IndexType,
    selection: &'a EvalScalar,
    predicates: Option<&'a [ScalarExpr]>,
    aggregation: Option<AggregationInfo<'a>>,
}

impl RewriteInfomartion<'_> {
    // Check the if information contains the select item.
    fn check_select_item(&self, _item: &ScalarExpr, _agg: &Option<AggregationInfo<'_>>) -> bool {
        // TODO
        true
    }

    fn output_bound_cols(&self) -> ColumnSet {
        let mut cols = ColumnSet::new();
        for item in self.selection.items.iter() {
            if let ScalarExpr::BoundColumnRef(col) = &item.scalar {
                cols.insert(col.column.index);
            }
        }

        cols
    }
}

impl RuleTryApplyAggIndex {
    fn collect_information(s_expr: &SExpr) -> Result<RewriteInfomartion<'_>> {
        // The plan tree should be started with [`EvalScalar`].
        if let RelOperator::EvalScalar(eval) = s_expr.plan() {
            let mut info = RewriteInfomartion {
                table_index: 0,
                selection: eval,
                predicates: None,
                aggregation: None,
            };
            Self::collect_information_impl(s_expr.child(0)?, &mut info)?;
            return Ok(info);
        }

        unreachable!()
    }

    fn collect_information_impl<'a>(
        s_expr: &'a SExpr,
        info: &mut RewriteInfomartion<'a>,
    ) -> Result<()> {
        match s_expr.plan() {
            RelOperator::Aggregate(agg) => {
                let child = s_expr.child(0)?;
                if let RelOperator::EvalScalar(eval) = child.plan() {
                    // This eval scalar hold aggregation's arguments.
                    let args_map = eval
                        .items
                        .iter()
                        .map(|item| (item.index, &item.scalar))
                        .collect();
                    info.aggregation.replace((agg, args_map));
                    Self::collect_information_impl(child.child(0)?, info)
                } else {
                    Self::collect_information_impl(child, info)
                }
            }
            RelOperator::Filter(filter) => {
                info.predicates.replace(&filter.predicates);
                Self::collect_information_impl(s_expr.child(0)?, info)
            }
            RelOperator::Scan(scan) => {
                info.table_index = scan.table_index;
                // Finish the recursion.
                Ok(())
            }
            _ => Self::collect_information_impl(s_expr.child(0)?, info),
        }
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
    fn check_predicates_range(
        query: &RangePredicates,
        index: &RangePredicates,
        index_output_bound_cols: &ColumnSet,
    ) -> bool {
        for (col, index_range) in index {
            if let Some(query_range) = query.get(col) {
                if !index_range.contains(query_range) {
                    return false;
                }
                // If query range is not equal to index range,
                // we need to filter the index data.
                // So we need to check if the columns in query predicates exist in index output columns.
                if index_range != query_range && !index_output_bound_cols.contains(col) {
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
