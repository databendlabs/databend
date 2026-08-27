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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_statistics::Histogram;

use super::ColumnStat;
use super::Selectivity;
use super::join_column::JoinColumnStats;
use super::join_column::aggregate_column_stats;
use super::join_condition::CompleteStats;
use super::join_condition::EquiCondition;
use super::join_condition::EquiEstimate;
use super::join_condition::EquiStats;
use super::join_condition::JoinEstimate;
use super::join_condition::NonEquiCondition;
use super::join_condition::join_key_null_count_for_cardinality;
use crate::ColumnSet;
use crate::Symbol;
use crate::optimizer::ir::Side;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;

const DEFAULT_NON_EQUI_SELECTIVITY: f64 = 0.5;

pub(super) struct JoinStats {
    pub(super) output_rows: f64,
    pub(super) left: JoinSideStats,
    pub(super) right: JoinSideStats,
    pub(super) columns: HashMap<Symbol, JoinColumnStats>,
}
pub(super) struct JoinSideStats {
    pub(super) input_rows: f64,
    pub(super) surviving_input_rows: f64,
    pub(super) ndv_surviving_input_rows: f64,
    pub(super) residual_ndv_selectivity: f64,
    pub(super) value_output_rows: f64,
    pub(super) null_extension_rows: f64,
    pub(super) expression_output: ExpressionStatOutput,
    pub(super) rejects_null: bool,
    pub(super) histogram_output: HistogramStatOutput,
}

#[derive(Clone, Copy)]
struct JoinSideMatches {
    input_rows: f64,
    matched_rows: f64,
    uncertain_matched_rows: f64,
}

#[derive(Clone, Copy)]
struct NonEquiSideMatches {
    left_rows: f64,
    right_rows: f64,
    confirmed: bool,
}

#[derive(Clone, Copy)]
struct JoinMatchPolicy {
    include_non_equi_side_matches: bool,
    cap_side_matches_by_pair_rows: bool,
}

#[derive(Default)]
struct JoinSideConditionEstimates {
    matched_rows: Vec<f64>,
    ndv_matched_rows: Vec<f64>,
    confirmed_rows: Vec<f64>,
}

struct JoinConditionEstimates {
    pair_rows: Vec<f64>,
    left: JoinSideConditionEstimates,
    right: JoinSideConditionEstimates,
    strongest_equi_pair_rows: f64,
    strongest_condition_ndv: Option<NdvEstimate>,
}

#[derive(Clone, Copy)]
struct CombinedJoinSideEstimates {
    matches: JoinSideMatches,
    ndv_matched_rows: f64,
}

struct CombinedJoinConditionEstimates {
    matched_pair_rows: f64,
    strongest_condition_ndv: Option<NdvEstimate>,
    left: CombinedJoinSideEstimates,
    right: CombinedJoinSideEstimates,
}

pub(super) struct EquiExpressionStats {
    pub(super) dependencies: ColumnSet,
    pub(super) identity_column: Option<Symbol>,
    pub(super) null_rejected_column: Option<Symbol>,
    pub(super) local_selectivity: Selectivity,
    pub(super) matched_distribution: Option<ColumnStat>,
    pub(super) side_matched_histogram: Option<Histogram>,
}

pub(super) enum JoinConditionContribution {
    Equi(Box<EquiConditionContribution>),
    NonEqui(NonEquiCondition),
}

pub(super) struct EquiConditionContribution {
    estimate: Option<JoinEstimateContribution>,
    pub(super) left: EquiExpressionStats,
    pub(super) right: EquiExpressionStats,
}

#[derive(Clone, Copy)]
struct JoinEstimateContribution {
    matched_pair_rows: f64,
    ndv: Option<NdvEstimate>,
    left_matched_rows: f64,
    right_matched_rows: f64,
    left_histogram_estimated_matched_rows: f64,
    right_histogram_estimated_matched_rows: f64,
}

impl JoinSideMatches {
    fn unmatched_rows(&self) -> f64 {
        (self.input_rows - self.matched_rows).clamp(0.0, self.input_rows)
    }
}

impl JoinMatchPolicy {
    fn for_join_type(join_type: JoinType) -> Self {
        let include_non_equi_side_matches = matches!(
            join_type,
            JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        );
        // ASOF does not use the independent peer-match model, but it still
        // benefits from the logical side-match upper bound during combination.
        let cap_side_matches_by_pair_rows = include_non_equi_side_matches
            || matches!(
                join_type,
                JoinType::Asof | JoinType::LeftAsof | JoinType::RightAsof | JoinType::FullAsof
            );
        Self {
            include_non_equi_side_matches,
            cap_side_matches_by_pair_rows,
        }
    }
}

impl JoinConditionEstimates {
    fn collect(
        contributions: &[JoinConditionContribution],
        left_input_rows: f64,
        right_input_rows: f64,
        policy: JoinMatchPolicy,
    ) -> Self {
        let input_pair_rows = left_input_rows * right_input_rows;
        let mut estimates = Self {
            pair_rows: Vec::new(),
            left: JoinSideConditionEstimates::default(),
            right: JoinSideConditionEstimates::default(),
            strongest_equi_pair_rows: input_pair_rows,
            strongest_condition_ndv: None,
        };
        for contribution in contributions {
            estimates.add_contribution(
                contribution,
                left_input_rows,
                right_input_rows,
                input_pair_rows,
                policy.include_non_equi_side_matches,
            );
        }
        estimates
    }

    fn add_contribution(
        &mut self,
        contribution: &JoinConditionContribution,
        left_input_rows: f64,
        right_input_rows: f64,
        input_pair_rows: f64,
        include_non_equi_side_matches: bool,
    ) {
        if let Some(pair_rows) = contribution.matched_pair_rows(input_pair_rows) {
            self.pair_rows.push(pair_rows);
        }
        if include_non_equi_side_matches
            && let Some(matches) =
                contribution.non_equi_matched_rows(left_input_rows, right_input_rows)
        {
            self.left
                .add_non_equi_matches(matches.left_rows, matches.confirmed);
            self.right
                .add_non_equi_matches(matches.right_rows, matches.confirmed);
        }
        if let Some((left_rows, right_rows)) =
            contribution.ndv_matched_rows(left_input_rows, right_input_rows)
        {
            self.left.ndv_matched_rows.push(left_rows);
            self.right.ndv_matched_rows.push(right_rows);
        }
        if let Some(estimate) = contribution.equi_estimate() {
            self.add_equi_estimate(estimate);
        }
    }

    fn add_equi_estimate(&mut self, estimate: JoinEstimateContribution) {
        if estimate.matched_pair_rows < self.strongest_equi_pair_rows {
            self.strongest_equi_pair_rows = estimate.matched_pair_rows;
            self.strongest_condition_ndv = estimate.ndv;
        }
        self.left.add_equi_matches(
            estimate.left_matched_rows,
            estimate.left_histogram_estimated_matched_rows,
        );
        self.right.add_equi_matches(
            estimate.right_matched_rows,
            estimate.right_histogram_estimated_matched_rows,
        );
    }

    fn combine(
        mut self,
        left_input_rows: f64,
        right_input_rows: f64,
        cap_side_matches_by_pair_rows: bool,
    ) -> CombinedJoinConditionEstimates {
        let input_pair_rows = left_input_rows * right_input_rows;
        let matched_pair_rows =
            combine_condition_estimates(input_pair_rows, &mut self.pair_rows, input_pair_rows);
        CombinedJoinConditionEstimates {
            matched_pair_rows,
            strongest_condition_ndv: self.strongest_condition_ndv,
            left: self.left.combine(
                left_input_rows,
                fallback_matched_rows(left_input_rows, right_input_rows),
                matched_pair_rows,
                cap_side_matches_by_pair_rows,
            ),
            right: self.right.combine(
                right_input_rows,
                fallback_matched_rows(right_input_rows, left_input_rows),
                matched_pair_rows,
                cap_side_matches_by_pair_rows,
            ),
        }
    }
}

impl JoinSideConditionEstimates {
    fn add_non_equi_matches(&mut self, matched_rows: f64, confirmed: bool) {
        self.matched_rows.push(matched_rows);
        self.confirmed_rows
            .push(if confirmed { matched_rows } else { 0.0 });
    }

    fn add_equi_matches(&mut self, matched_rows: f64, uncertain_rows: f64) {
        self.matched_rows.push(matched_rows);
        self.confirmed_rows.push(matched_rows - uncertain_rows);
    }

    fn combine(
        mut self,
        input_rows: f64,
        fallback: f64,
        matched_pair_rows: f64,
        cap_by_pair_rows: bool,
    ) -> CombinedJoinSideEstimates {
        let mut matched_rows =
            combine_condition_estimates(input_rows, &mut self.matched_rows, fallback);
        if cap_by_pair_rows {
            // Every matched input row contributes at least one matched pair.
            matched_rows = matched_rows.min(matched_pair_rows);
        }
        let ndv_matched_rows =
            combine_condition_estimates(input_rows, &mut self.ndv_matched_rows, fallback);
        let confirmed_rows =
            combine_condition_estimates(input_rows, &mut self.confirmed_rows, fallback);
        CombinedJoinSideEstimates {
            matches: JoinSideMatches {
                input_rows,
                matched_rows,
                uncertain_matched_rows: (matched_rows - confirmed_rows).clamp(0.0, matched_rows),
            },
            ndv_matched_rows,
        }
    }
}

fn fallback_matched_rows(input_rows: f64, peer_rows: f64) -> f64 {
    if input_rows > 0.0 && peer_rows > 0.0 {
        input_rows
    } else {
        0.0
    }
}

pub(crate) struct JoinStatsEstimator {
    join_type: JoinType,
    left_input: Arc<StatInfo>,
    right_input: Arc<StatInfo>,
    contributions: Vec<JoinConditionContribution>,
}

impl JoinStatsEstimator {
    pub(crate) fn new(
        join_type: JoinType,
        left_input: Arc<StatInfo>,
        right_input: Arc<StatInfo>,
    ) -> Self {
        Self {
            join_type,
            left_input,
            right_input,
            contributions: Vec::new(),
        }
    }

    pub(crate) fn evaluate_join(&mut self, join: &Join) -> Result<()> {
        let left_stat_cardinality = self
            .left_input
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(self.left_input.cardinality));
        let right_stat_cardinality = self
            .right_input
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(self.right_input.cardinality));
        for condition in &join.equi_conditions {
            let (left, right) = condition.canonical_keys();
            let evaluated = EquiCondition::estimate_locally(
                left,
                right,
                &self.left_input.statistics,
                &self.right_input.statistics,
                left_stat_cardinality,
                right_stat_cardinality,
            )?;
            self.contributions
                .push(JoinConditionContribution::from_equi(
                    &evaluated,
                    condition.is_null_equal,
                    self.left_input.cardinality,
                    self.right_input.cardinality,
                )?);
        }

        if !join.non_equi_conditions.is_empty() {
            let input = combined_input_statistics(
                &self.left_input.statistics,
                &self.right_input.statistics,
            );
            let input_cardinality = input
                .precise_cardinality
                .map(StatCardinality::exact)
                .unwrap_or_else(|| {
                    StatCardinality::estimate(
                        self.left_input.cardinality * self.right_input.cardinality,
                    )
                });
            for predicate in &join.non_equi_conditions {
                let evaluated =
                    NonEquiCondition::estimate_locally(predicate, &input, input_cardinality)?;
                self.contributions
                    .push(JoinConditionContribution::from_non_equi(&evaluated));
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Arc<StatInfo>> {
        let mut stats = self.finish_join_stats()?;
        let output_rows = stats.output_rows;
        let column_stats = if output_rows == 0.0 {
            HashMap::new()
        } else {
            std::iter::chain(
                stats.output_column_stats(&self.left_input.statistics.column_stats, Side::Left)?,
                stats
                    .output_column_stats(&self.right_input.statistics.column_stats, Side::Right)?,
            )
            .collect()
        };
        Ok(Arc::new(StatInfo {
            cardinality: output_rows,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        }))
    }

    fn finish_join_stats(&self) -> Result<JoinStats> {
        let left_input_rows = self.left_input.cardinality;
        let right_input_rows = self.right_input.cardinality;
        let policy = JoinMatchPolicy::for_join_type(self.join_type);
        let combined = JoinConditionEstimates::collect(
            &self.contributions,
            left_input_rows,
            right_input_rows,
            policy,
        )
        .combine(
            left_input_rows,
            right_input_rows,
            policy.cap_side_matches_by_pair_rows,
        );
        let left_matches = combined.left.matches;
        let right_matches = combined.right.matches;
        let output_rows = self.output_cardinality(
            combined.matched_pair_rows,
            combined.strongest_condition_ndv,
            &left_matches,
            &right_matches,
        );
        let left = self.finish_side_stats(
            Side::Left,
            left_matches,
            left_matches,
            right_matches,
            combined.matched_pair_rows,
            output_rows,
            combined.left.ndv_matched_rows,
        );
        let right = self.finish_side_stats(
            Side::Right,
            right_matches,
            left_matches,
            right_matches,
            combined.matched_pair_rows,
            output_rows,
            combined.right.ndv_matched_rows,
        );

        Ok(JoinStats {
            output_rows,
            left,
            right,
            columns: aggregate_column_stats(&self.contributions)?,
        })
    }

    fn output_cardinality(
        &self,
        matched_pair_rows: f64,
        ndv: Option<NdvEstimate>,
        left: &JoinSideMatches,
        right: &JoinSideMatches,
    ) -> f64 {
        // Cardinality symbols used by the formulas below:
        //
        //   L, R   = left/right input rows.
        //   P      = matched input pairs after combining all join conditions.
        //   ML, MR = left/right input rows that match at least one peer row.
        //   UL, UR = unmatched input rows = L - ML, R - MR.
        //   D      = expected NDV of the strongest equality condition, when available.
        //
        // P is pair cardinality and may exceed ML or MR when one input row matches several peer
        // rows. ML and MR are row coverage estimates, so SEMI/ANTI and null-extension formulas use
        // them instead of P.
        match self.join_type {
            // INNER/CROSS = P. With no conditions, P falls back to L * R.
            JoinType::Inner | JoinType::Cross => matched_pair_rows,
            // INNER ANY = D when equality NDV is known; otherwise min(ML, MR).
            JoinType::InnerAny => ndv
                .and_then(|ndv| ndv.expected)
                .unwrap_or_else(|| left.matched_rows.min(right.matched_rows)),
            // ASOF = MR. ASOF plans swap logical children, so the right child is the original
            // probe side and contributes at most one output row per matched input row.
            JoinType::Asof => right.matched_rows,
            // LEFT OUTER = P + UL; RIGHT OUTER = P + UR; FULL OUTER = P + UL + UR.
            JoinType::Left => matched_pair_rows + left.unmatched_rows(),
            JoinType::Right => matched_pair_rows + right.unmatched_rows(),
            JoinType::Full => matched_pair_rows + left.unmatched_rows() + right.unmatched_rows(),
            // ANY OUTER preserves exactly one row for every row on its preserved side.
            // LEFT ANY = L; RIGHT ANY = R.
            JoinType::LeftAny => self.left_input.cardinality,
            JoinType::RightAny => self.right_input.cardinality,
            // ASOF plans use swapped logical children. LEFT ASOF = R, RIGHT ASOF = L, and
            // FULL ASOF = R + UL (all original probe rows plus unmatched original build rows).
            JoinType::LeftAsof => self.right_input.cardinality,
            JoinType::RightAsof => self.left_input.cardinality,
            JoinType::FullAsof => self.right_input.cardinality + left.unmatched_rows(),
            // LEFT SEMI = ML; RIGHT SEMI = MR.
            JoinType::LeftSemi => left.matched_rows,
            JoinType::RightSemi => right.matched_rows,
            // ANTI starts from I - M, but estimate_anti_join_cardinality() caps only the uncertain
            // portion of M so modeled overlap cannot eliminate more than 90% of the input.
            JoinType::LeftAnti => estimate_anti_join_cardinality(
                self.left_input.cardinality,
                left.matched_rows,
                left.uncertain_matched_rows,
            ),
            JoinType::RightAnti => estimate_anti_join_cardinality(
                self.right_input.cardinality,
                right.matched_rows,
                right.uncertain_matched_rows,
            ),
            // SINGLE and MARK preserve the row count of the side that appears in their output:
            // LEFT SINGLE / RIGHT MARK = L; RIGHT SINGLE / LEFT MARK = R.
            JoinType::LeftSingle | JoinType::RightMark => self.left_input.cardinality,
            JoinType::RightSingle | JoinType::LeftMark => self.right_input.cardinality,
        }
    }

    fn finish_side_stats(
        &self,
        side: Side,
        matches: JoinSideMatches,
        left: JoinSideMatches,
        right: JoinSideMatches,
        matched_pair_rows: f64,
        output_rows: f64,
        ndv_matched_rows: f64,
    ) -> JoinSideStats {
        let expression_output = match (self.join_type, side) {
            (
                JoinType::Inner
                | JoinType::InnerAny
                | JoinType::Asof
                | JoinType::LeftSemi
                | JoinType::RightSemi,
                _,
            ) => ExpressionStatOutput::Estimated,
            (JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle, Side::Right)
            | (JoinType::Right | JoinType::RightAny | JoinType::RightSingle, Side::Left)
            | (JoinType::LeftAsof, Side::Left)
            | (JoinType::RightAsof, Side::Right) => {
                ExpressionStatOutput::EstimatedWithoutHistograms
            }
            _ => ExpressionStatOutput::Input,
        };
        let rejects_null = matches!(
            self.join_type,
            JoinType::Inner
                | JoinType::InnerAny
                | JoinType::Asof
                | JoinType::LeftSemi
                | JoinType::RightSemi
        );
        let histogram_output = match (self.join_type, side) {
            (JoinType::Inner, _) => HistogramStatOutput::Estimated,
            (JoinType::LeftSemi, Side::Left) | (JoinType::RightSemi, Side::Right) => {
                HistogramStatOutput::Semi
            }
            (
                JoinType::Left
                | JoinType::Right
                | JoinType::Full
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftAsof
                | JoinType::RightAsof
                | JoinType::FullAsof,
                _,
            ) => HistogramStatOutput::PreserveUnmatched,
            _ => HistogramStatOutput::Clear,
        };
        let (surviving_input_rows, value_output_rows) = match (self.join_type, side) {
            (JoinType::Inner | JoinType::InnerAny | JoinType::Asof, _) => {
                (matches.matched_rows, output_rows)
            }
            (JoinType::Cross, _) => (matches.input_rows, output_rows),
            (JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle, Side::Left)
            | (JoinType::Right | JoinType::RightAny | JoinType::RightSingle, Side::Right) => {
                (matches.input_rows, output_rows)
            }
            (JoinType::Left, Side::Right) | (JoinType::Right, Side::Left) => {
                (matches.matched_rows, matched_pair_rows)
            }
            (JoinType::LeftAny | JoinType::LeftSingle, Side::Right) => {
                (matches.matched_rows, left.matched_rows)
            }
            (JoinType::RightAny | JoinType::RightSingle, Side::Left) => {
                (matches.matched_rows, right.matched_rows)
            }
            (JoinType::Full, Side::Left) => (
                matches.input_rows,
                matched_pair_rows + left.unmatched_rows(),
            ),
            (JoinType::Full, Side::Right) => (
                matches.input_rows,
                matched_pair_rows + right.unmatched_rows(),
            ),
            (JoinType::LeftSemi, Side::Left) | (JoinType::RightSemi, Side::Right) => {
                (matches.matched_rows, output_rows)
            }
            (JoinType::LeftAnti, Side::Left) | (JoinType::RightAnti, Side::Right) => {
                (output_rows, output_rows)
            }
            (JoinType::LeftAsof, Side::Right) | (JoinType::RightAsof, Side::Left) => {
                (matches.input_rows, output_rows)
            }
            (JoinType::LeftAsof, Side::Left) => (matches.matched_rows, right.matched_rows),
            (JoinType::RightAsof, Side::Right) => (matches.matched_rows, left.matched_rows),
            (JoinType::FullAsof, Side::Left) => (
                matches.input_rows,
                right.matched_rows + left.unmatched_rows(),
            ),
            (JoinType::FullAsof, Side::Right) => (matches.input_rows, matches.input_rows),
            _ => (0.0, 0.0),
        };
        let null_extension_rows = match (self.join_type, side) {
            (JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle, Side::Right) => {
                left.unmatched_rows()
            }
            (JoinType::Right | JoinType::RightAny | JoinType::RightSingle, Side::Left) => {
                right.unmatched_rows()
            }
            (JoinType::LeftAsof, Side::Left) => right.unmatched_rows(),
            (JoinType::RightAsof, Side::Right) => left.unmatched_rows(),
            (JoinType::Full | JoinType::FullAsof, Side::Left) => right.unmatched_rows(),
            (JoinType::Full | JoinType::FullAsof, Side::Right) => left.unmatched_rows(),
            _ => 0.0,
        };
        let combines_condition_ndv = matches!(
            self.join_type,
            JoinType::Inner | JoinType::InnerAny | JoinType::LeftSemi | JoinType::RightSemi
        );
        let (ndv_surviving_input_rows, residual_ndv_selectivity) =
            if !combines_condition_ndv || expression_output == ExpressionStatOutput::Input {
                (surviving_input_rows, 1.0)
            } else {
                (
                    ndv_matched_rows,
                    if surviving_input_rows > 0.0 {
                        (ndv_matched_rows / surviving_input_rows).clamp(0.0, 1.0)
                    } else {
                        1.0
                    },
                )
            };

        JoinSideStats {
            input_rows: matches.input_rows,
            surviving_input_rows,
            ndv_surviving_input_rows,
            residual_ndv_selectivity,
            value_output_rows,
            null_extension_rows,
            expression_output,
            rejects_null,
            histogram_output,
        }
    }
}

impl JoinConditionContribution {
    fn from_equi(
        condition: &EquiCondition<'_, '_>,
        is_null_equal: bool,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> Result<Self> {
        match &condition.stats {
            EquiStats::Missing => Ok(Self::from_incomplete_equi(
                condition.left,
                condition.right,
                is_null_equal,
                left_cardinality,
                right_cardinality,
                0.0,
                0.0,
            )),
            EquiStats::Left(stat) => Ok(Self::from_incomplete_equi(
                condition.left,
                condition.right,
                is_null_equal,
                left_cardinality,
                right_cardinality,
                join_key_null_count_for_cardinality(stat, left_cardinality),
                0.0,
            )),
            EquiStats::Right(stat) => Ok(Self::from_incomplete_equi(
                condition.left,
                condition.right,
                is_null_equal,
                left_cardinality,
                right_cardinality,
                0.0,
                join_key_null_count_for_cardinality(stat, right_cardinality),
            )),
            EquiStats::Complete(stats) => Self::from_complete_equi(
                condition.left,
                condition.right,
                stats,
                is_null_equal,
                left_cardinality,
                right_cardinality,
            ),
        }
    }

    fn from_incomplete_equi(
        left: &ScalarExpr,
        right: &ScalarExpr,
        is_null_equal: bool,
        left_cardinality: f64,
        right_cardinality: f64,
        left_null_count: f64,
        right_null_count: f64,
    ) -> Self {
        let estimate = if is_null_equal {
            None
        } else {
            Some(Self::missing_statistics_estimate(
                left_null_count,
                right_null_count,
                left_cardinality,
                right_cardinality,
            ))
        };
        Self::Equi(Box::new(EquiConditionContribution {
            estimate,
            left: EquiExpressionStats::new(left, Selectivity::Unknown, None, None, !is_null_equal),
            right: EquiExpressionStats::new(
                right,
                Selectivity::Unknown,
                None,
                None,
                !is_null_equal,
            ),
        }))
    }

    fn from_complete_equi(
        left: &ScalarExpr,
        right: &ScalarExpr,
        stats: &CompleteStats<'_>,
        is_null_equal: bool,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> Result<Self> {
        let left_stat = stats.left_stat.as_ref();
        let right_stat = stats.right_stat.as_ref();
        let left_null_count = join_key_null_count_for_cardinality(left_stat, left_cardinality);
        let right_null_count = join_key_null_count_for_cardinality(right_stat, right_cardinality);
        let null_match_cardinality = if is_null_equal {
            left_null_count * right_null_count
        } else {
            0.0
        };
        let null_count = StatCount::estimate(null_match_cardinality, null_match_cardinality);
        let all_null = matches!(left_stat, ColumnStat::AllNull { .. })
            || matches!(right_stat, ColumnStat::AllNull { .. });

        let (left_distribution, right_distribution, left_histogram, right_histogram) =
            if all_null && is_null_equal {
                let distribution = ColumnStat::AllNull { null_count };
                (Some(distribution.clone()), Some(distribution), None, None)
            } else {
                match &stats.estimate {
                    EquiEstimate::NoOverlap if null_match_cardinality > 0.0 => {
                        let distribution = ColumnStat::AllNull { null_count };
                        (Some(distribution.clone()), Some(distribution), None, None)
                    }
                    EquiEstimate::Matched(matched) => {
                        let mut left_distribution = matched.left_distribution.clone();
                        let mut right_distribution = matched.right_distribution.clone();
                        Self::restore_direct_peer_histogram(
                            left,
                            right,
                            &mut left_distribution,
                            left_stat,
                            matched.estimate.left_matched_rows,
                        )?;
                        Self::restore_direct_peer_histogram(
                            right,
                            left,
                            &mut right_distribution,
                            right_stat,
                            matched.estimate.right_matched_rows,
                        )?;
                        left_distribution.set_null_count(null_count);
                        right_distribution.set_null_count(null_count);
                        (
                            Some(left_distribution),
                            Some(right_distribution),
                            matched.estimate.left_matched_histogram.clone(),
                            matched.estimate.right_matched_histogram.clone(),
                        )
                    }
                    EquiEstimate::CardinalityOnly(_) | EquiEstimate::NoOverlap => {
                        (None, None, None, None)
                    }
                }
            };

        let left_null_matched_rows = if is_null_equal && right_null_count > 0.0 {
            left_null_count
        } else {
            0.0
        };
        let right_null_matched_rows = if is_null_equal && left_null_count > 0.0 {
            right_null_count
        } else {
            0.0
        };
        let estimate = Some(Self::estimate_equi_condition(
            stats,
            is_null_equal,
            all_null,
            null_match_cardinality,
            left_null_matched_rows,
            right_null_matched_rows,
        ));
        Ok(Self::Equi(Box::new(EquiConditionContribution {
            estimate,
            left: EquiExpressionStats::new(
                left,
                stats.selectivity,
                left_distribution,
                left_histogram,
                !is_null_equal,
            ),
            right: EquiExpressionStats::new(
                right,
                stats.selectivity,
                right_distribution,
                right_histogram,
                !is_null_equal,
            ),
        })))
    }

    fn restore_direct_peer_histogram(
        expression: &ScalarExpr,
        peer: &ScalarExpr,
        matched_distribution: &mut ColumnStat,
        input_distribution: &ColumnStat,
        matched_rows: f64,
    ) -> Result<()> {
        if identity_column(expression).is_some()
            && identity_column(peer).is_none()
            && matched_distribution.histogram().is_none()
        {
            matched_distribution
                .replace_histogram_from(input_distribution, matched_rows)
                .map_err(ErrorCode::Internal)?;
        }
        Ok(())
    }

    fn from_non_equi(condition: &NonEquiCondition) -> Self {
        Self::NonEqui(*condition)
    }

    fn matched_pair_rows(&self, input_pair_rows: f64) -> Option<f64> {
        match self {
            Self::Equi(condition) => condition
                .estimate
                .map(|estimate| estimate.matched_pair_rows),
            Self::NonEqui(condition) => {
                Some(input_pair_rows * join_selectivity(condition.selectivity))
            }
        }
    }

    fn non_equi_matched_rows(
        &self,
        left_input_rows: f64,
        right_input_rows: f64,
    ) -> Option<NonEquiSideMatches> {
        let Self::NonEqui(condition) = self else {
            return None;
        };
        let selectivity = join_selectivity(condition.selectivity);
        Some(NonEquiSideMatches {
            left_rows: estimate_non_equi_side_matched_rows(
                left_input_rows,
                right_input_rows,
                selectivity,
            ),
            right_rows: estimate_non_equi_side_matched_rows(
                right_input_rows,
                left_input_rows,
                selectivity,
            ),
            confirmed: matches!(condition.selectivity, Selectivity::Zero | Selectivity::All),
        })
    }

    fn ndv_matched_rows(&self, left_input_rows: f64, right_input_rows: f64) -> Option<(f64, f64)> {
        match self {
            Self::Equi(condition) => condition
                .estimate
                .map(|estimate| (estimate.left_matched_rows, estimate.right_matched_rows)),
            Self::NonEqui(condition) => {
                let selectivity = join_selectivity(condition.selectivity);
                Some((
                    left_input_rows * selectivity,
                    right_input_rows * selectivity,
                ))
            }
        }
    }

    fn equi_estimate(&self) -> Option<JoinEstimateContribution> {
        match self {
            Self::Equi(condition) => condition.estimate,
            Self::NonEqui(_) => None,
        }
    }

    fn missing_statistics_estimate(
        left_null_count: f64,
        right_null_count: f64,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> JoinEstimateContribution {
        let left_non_null = (left_cardinality - left_null_count).max(0.0);
        let right_non_null = (right_cardinality - right_null_count).max(0.0);
        let left_matched_rows = if right_non_null > 0.0 {
            left_non_null
        } else {
            0.0
        };
        let right_matched_rows = if left_non_null > 0.0 {
            right_non_null
        } else {
            0.0
        };
        JoinEstimateContribution {
            matched_pair_rows: left_non_null * right_non_null,
            ndv: None,
            left_matched_rows,
            right_matched_rows,
            left_histogram_estimated_matched_rows: 0.0,
            right_histogram_estimated_matched_rows: 0.0,
        }
    }

    fn estimate_equi_condition(
        stats: &CompleteStats<'_>,
        is_null_equal: bool,
        all_null: bool,
        null_match_cardinality: f64,
        left_null_matched_rows: f64,
        right_null_matched_rows: f64,
    ) -> JoinEstimateContribution {
        if !is_null_equal && all_null {
            return JoinEstimateContribution::no_value_matches(0.0, 0.0, 0.0);
        }
        if is_null_equal && all_null {
            return JoinEstimateContribution::no_value_matches(
                null_match_cardinality,
                left_null_matched_rows,
                right_null_matched_rows,
            );
        }

        match &stats.estimate {
            EquiEstimate::NoOverlap => JoinEstimateContribution::no_value_matches(
                null_match_cardinality,
                left_null_matched_rows,
                right_null_matched_rows,
            ),
            EquiEstimate::CardinalityOnly(estimate) => Self::estimated_condition(
                estimate,
                null_match_cardinality,
                left_null_matched_rows,
                right_null_matched_rows,
            ),
            EquiEstimate::Matched(matched) => Self::estimated_condition(
                &matched.estimate,
                null_match_cardinality,
                left_null_matched_rows,
                right_null_matched_rows,
            ),
        }
    }

    fn estimated_condition(
        estimated: &JoinEstimate,
        null_match_cardinality: f64,
        left_null_matched_rows: f64,
        right_null_matched_rows: f64,
    ) -> JoinEstimateContribution {
        let JoinEstimate {
            card,
            ndv,
            left_matched_rows,
            right_matched_rows,
            left_estimated_matched_rows,
            right_estimated_matched_rows,
            ..
        } = estimated;
        let matched_pair_rows = *card + null_match_cardinality;
        JoinEstimateContribution {
            matched_pair_rows,
            ndv: *ndv,
            left_matched_rows: *left_matched_rows + left_null_matched_rows,
            right_matched_rows: *right_matched_rows + right_null_matched_rows,
            left_histogram_estimated_matched_rows: *left_estimated_matched_rows,
            right_histogram_estimated_matched_rows: *right_estimated_matched_rows,
        }
    }
}

fn join_selectivity(selectivity: Selectivity) -> f64 {
    match selectivity {
        Selectivity::Unknown | Selectivity::LowerBound => DEFAULT_NON_EQUI_SELECTIVITY,
        Selectivity::Zero => 0.0,
        Selectivity::All => 1.0,
        Selectivity::N(value) => value,
    }
}

fn estimate_non_equi_side_matched_rows(
    input_rows: f64,
    peer_rows: f64,
    pair_selectivity: f64,
) -> f64 {
    if input_rows <= 0.0 || peer_rows <= 0.0 {
        return 0.0;
    }
    let pair_selectivity = pair_selectivity.clamp(0.0, 1.0);
    // Model peer comparisons independently. For I input rows, N peer rows, and pair selectivity
    // s, the expected matched input rows are I * (1 - (1 - s)^N). ln_1p/exp_m1 evaluates the same
    // formula without cancellation for small selectivities.
    let no_match_log_probability = peer_rows * (-pair_selectivity).ln_1p();
    let match_probability = -no_match_log_probability.exp_m1();
    (input_rows * match_probability).clamp(0.0, input_rows)
}

impl EquiExpressionStats {
    fn new(
        expression: &ScalarExpr,
        local_selectivity: Selectivity,
        matched_distribution: Option<ColumnStat>,
        side_matched_histogram: Option<Histogram>,
        rejects_null: bool,
    ) -> Self {
        Self {
            dependencies: expression.used_columns(),
            identity_column: identity_column(expression),
            null_rejected_column: rejects_null
                .then(|| null_rejected_column(expression))
                .flatten(),
            local_selectivity,
            matched_distribution,
            side_matched_histogram,
        }
    }
}

impl JoinEstimateContribution {
    fn no_value_matches(
        matched_pair_rows: f64,
        left_matched_rows: f64,
        right_matched_rows: f64,
    ) -> Self {
        Self {
            matched_pair_rows,
            ndv: Some(NdvEstimate::exact(if matched_pair_rows > 0.0 {
                1.0
            } else {
                0.0
            })),
            left_matched_rows,
            right_matched_rows,
            left_histogram_estimated_matched_rows: 0.0,
            right_histogram_estimated_matched_rows: 0.0,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ExpressionStatOutput {
    Input,
    Estimated,
    EstimatedWithoutHistograms,
}

#[derive(Clone, Copy)]
pub(super) enum HistogramStatOutput {
    Estimated,
    Semi,
    PreserveUnmatched,
    Clear,
}

fn combine_condition_estimates(
    input_cardinality: f64,
    estimates: &mut [f64],
    fallback: f64,
) -> f64 {
    if estimates.is_empty() || input_cardinality <= 0.0 {
        return fallback;
    }

    estimates.sort_by(f64::total_cmp);
    let mut cardinality = input_cardinality;
    let mut exponent = 1.0;
    // Let I be input_cardinality and sort condition selectivities from strongest to weakest as
    // s1 <= s2 <= ... <= sn. Exponential backoff computes:
    //
    //   I * s1 * s2^(1/2) * s3^(1/4) * ... * sn^(1/2^(n-1)).
    //
    // This same combiner is used for matched pairs and per-side matched-row/NDV estimates.
    for estimate in estimates {
        let selectivity = (*estimate / input_cardinality).clamp(0.0, 1.0);
        cardinality *= selectivity.powf(exponent);
        exponent *= 0.5;
    }
    cardinality
}

fn estimate_anti_join_cardinality(
    input_cardinality: f64,
    matched_rows: f64,
    uncertain_matched_rows: f64,
) -> f64 {
    if input_cardinality <= 0.0 {
        return 0.0;
    }

    let matched_rows = matched_rows.clamp(0.0, input_cardinality);
    let uncertain_matched_rows = uncertain_matched_rows.clamp(0.0, matched_rows);
    // Let I be input rows, M matched rows, U the uncertain part of M, and C = M - U the confirmed
    // part. Histogram overlap and modeled non-equi selectivity cannot prove complete coverage, so:
    //
    //   budget   = max(0, 0.9 * I - C)
    //   adjusted = C + min(U, budget)
    //   ANTI     = I - adjusted
    //
    // Confirmed matches are never capped; only uncertain overlap is limited to the 90% budget.
    const MAX_ANTI_JOIN_ESTIMATED_OVERLAP: f64 = 0.9;
    let confirmed_matched_rows = matched_rows - uncertain_matched_rows;
    let estimated_match_budget =
        (input_cardinality * MAX_ANTI_JOIN_ESTIMATED_OVERLAP - confirmed_matched_rows).max(0.0);
    let adjusted_matched_rows =
        confirmed_matched_rows + uncertain_matched_rows.min(estimated_match_budget);
    input_cardinality - adjusted_matched_rows
}

fn combined_input_statistics(left: &Statistics, right: &Statistics) -> Statistics {
    Statistics {
        precise_cardinality: left.precise_cardinality.and_then(|left| {
            right
                .precise_cardinality
                .and_then(|right| left.checked_mul(right))
        }),
        column_stats: left
            .column_stats
            .iter()
            .chain(&right.column_stats)
            .map(|(column, stat)| (*column, stat.clone()))
            .collect(),
        top_n: left
            .top_n
            .iter()
            .chain(&right.top_n)
            .map(|(column, top_n)| (*column, top_n.clone()))
            .collect(),
        count_min_sketch: left
            .count_min_sketch
            .iter()
            .chain(&right.count_min_sketch)
            .map(|(column, sketch)| (*column, sketch.clone()))
            .collect(),
    }
}

fn identity_column(expression: &ScalarExpr) -> Option<Symbol> {
    match expression {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        _ => None,
    }
}

// This whitelist proves only that rejecting NULL from the expression also
// rejects NULL from the source column. It does not permit value-distribution
// writeback through the expression.
fn null_rejected_column(expression: &ScalarExpr) -> Option<Symbol> {
    match expression {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        ScalarExpr::CastExpr(cast) if !cast.is_try => {
            let source_type = cast.argument.data_type();
            if classify_conversion(source_type.as_ref(), cast.target_type.as_ref())
                .is_lossless_injective()
            {
                identity_column(&cast.argument)
            } else {
                None
            }
        }
        _ => None,
    }
}
