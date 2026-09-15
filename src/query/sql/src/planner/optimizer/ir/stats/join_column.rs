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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_statistics::Histogram;

use super::ColumnStat;
use super::ColumnStatSet;
use super::Selectivity;
use super::join::EquiExpressionStats;
use super::join::ExpressionStatOutput;
use super::join::HistogramStatOutput;
use super::join::JoinConditionContribution;
use super::join::JoinSideStats;
use super::join::JoinStats;
use crate::Symbol;
use crate::optimizer::ir::Side;

pub(super) struct JoinColumnStats {
    is_identity: bool,
    rejects_null: bool,
    pub(super) matched_distribution: Option<ColumnStat>,
    has_selected_identity_distribution: bool,
    side_matched_histogram: Option<Histogram>,
}

#[derive(Default)]
struct JoinColumnStatsBuilder<'a> {
    equi_expressions: Vec<&'a EquiExpressionStats>,
}

impl JoinColumnStatsBuilder<'_> {
    fn finish(self, column: Symbol) -> Result<JoinColumnStats> {
        let is_identity = self
            .equi_expressions
            .iter()
            .any(|expression| expression.identity_column == Some(column));
        let rejects_null = self
            .equi_expressions
            .iter()
            .any(|expression| expression.null_rejected_column == Some(column));
        let selected = self.selected_identity_expression(column);
        let matched_distribution = self.combined_identity_distribution(column, selected)?;
        Ok(JoinColumnStats {
            is_identity,
            rejects_null,
            matched_distribution,
            has_selected_identity_distribution: selected.is_some(),
            side_matched_histogram: selected
                .and_then(|expression| expression.side_matched_histogram.clone()),
        })
    }

    fn selected_identity_expression(&self, column: Symbol) -> Option<&EquiExpressionStats> {
        let expression = self
            .equi_expressions
            .iter()
            .copied()
            .min_by(|left, right| {
                left.compare_selectivity(right).then_with(|| {
                    let left_has_histogram = left.identity_column == Some(column)
                        && (left
                            .matched_distribution
                            .as_ref()
                            .and_then(|distribution| distribution.histogram())
                            .is_some()
                            || left.side_matched_histogram.is_some());
                    let right_has_histogram = right.identity_column == Some(column)
                        && (right
                            .matched_distribution
                            .as_ref()
                            .and_then(|distribution| distribution.histogram())
                            .is_some()
                            || right.side_matched_histogram.is_some());
                    right_has_histogram.cmp(&left_has_histogram)
                })
            })?;
        (expression.identity_column == Some(column) && expression.matched_distribution.is_some())
            .then_some(expression)
    }

    fn combined_identity_distribution(
        &self,
        column: Symbol,
        selected: Option<&EquiExpressionStats>,
    ) -> Result<Option<ColumnStat>> {
        let candidates = self
            .equi_expressions
            .iter()
            .filter(|expression| expression.identity_column == Some(column))
            .filter_map(|expression| expression.matched_distribution.as_ref())
            .collect::<Vec<_>>();
        let Some(first) = candidates.first() else {
            return Ok(None);
        };
        if candidates.len() == 1 {
            return Ok(Some((*first).clone()));
        }

        if candidates
            .iter()
            .any(|candidate| matches!(candidate, ColumnStat::AllNull { .. }))
        {
            // An AllNull distribution is a stronger intersection constraint than a value
            // distribution: every row satisfying this equality has a NULL value. Keep it
            // when another null-safe equality on the same column has ordinary value bounds.
            // Its NULL matches still have to satisfy every condition, so retain the minimum
            // count across all candidate distributions.
            let null_count = candidates
                .iter()
                .map(|candidate| candidate.null_count())
                .reduce(min_count)
                .unwrap();
            return Ok(Some(ColumnStat::AllNull { null_count }));
        }

        let mut bounds = first.bounds().unwrap();
        let mut ndv = first.ndv();
        let mut null_count = first.null_count();
        for candidate in candidates.iter().skip(1) {
            let Some(intersection) = bounds.intersection(&candidate.bounds().unwrap()) else {
                return Ok(None);
            };
            bounds = intersection;
            ndv = ndv.min(candidate.ndv());
            null_count = min_count(null_count, candidate.null_count());
        }

        // The most selective local equality supplies the histogram shape. The
        // remaining equalities narrow its bounds here; the combined Join
        // selectivity is applied once when the output histogram is reconciled.
        let histogram = selected
            .and_then(|expression| expression.matched_distribution.as_ref())
            .and_then(|distribution| distribution.histogram())
            .map(|histogram| histogram.to_owned());
        let mut distribution = (*first).clone();
        distribution
            .set_histogram(histogram)
            .map_err(ErrorCode::Internal)?;
        distribution
            .restrict_to_bounds(bounds)
            .map_err(ErrorCode::Internal)?;
        distribution.set_ndv(distribution.ndv().min(ndv));
        distribution.set_null_count(null_count);
        Ok(Some(distribution))
    }
}

pub(super) fn aggregate_column_stats(
    contributions: &[JoinConditionContribution],
) -> Result<HashMap<Symbol, JoinColumnStats>> {
    let mut columns = HashMap::<Symbol, JoinColumnStatsBuilder<'_>>::new();
    for contribution in contributions {
        match contribution {
            JoinConditionContribution::Equi(condition) => {
                for expression in [&condition.left, &condition.right] {
                    for column in &expression.dependencies {
                        columns
                            .entry(*column)
                            .or_default()
                            .equi_expressions
                            .push(expression);
                    }
                }
            }
            JoinConditionContribution::NonEqui(_) => {}
        }
    }
    columns
        .into_iter()
        .map(|(column, builder)| builder.finish(column).map(|stats| (column, stats)))
        .collect()
}

fn min_count(left: StatCount, right: StatCount) -> StatCount {
    StatCount::estimate(
        left.expected().min(right.expected()),
        left.upper().min(right.upper()),
    )
}

impl EquiExpressionStats {
    fn compare_selectivity(&self, other: &Self) -> std::cmp::Ordering {
        match (self.local_selectivity, other.local_selectivity) {
            (Selectivity::N(left), Selectivity::N(right)) => left.total_cmp(&right),
            (Selectivity::N(_), _) => std::cmp::Ordering::Less,
            (_, Selectivity::N(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl JoinStats {
    pub(super) fn output_column_stats(
        &mut self,
        input_columns: &ColumnStatSet,
        side: Side,
    ) -> Result<ColumnStatSet> {
        input_columns
            .iter()
            .map(|(column, input_stat)| {
                let stat = self.rebuild_column(*column, input_stat, side)?;
                Ok((*column, stat))
            })
            .collect()
    }

    fn rebuild_column(
        &mut self,
        column: Symbol,
        input_stat: &ColumnStat,
        side: Side,
    ) -> Result<ColumnStat> {
        let side = match side {
            Side::Left => &self.left,
            Side::Right => &self.right,
        };

        let output_rows = self.output_rows;
        let (mut stat, is_identity, has_selected_identity_distribution, uses_matched_distribution) =
            if let Some(column_stats) = self.columns.get_mut(&column) {
                let uses_matched_distribution = side.expression_output
                    != ExpressionStatOutput::Input
                    && column_stats.matched_distribution.is_some();
                (
                    Self::rebuild_expression_stat(input_stat, column_stats, output_rows, side)?,
                    column_stats.is_identity,
                    column_stats.has_selected_identity_distribution,
                    uses_matched_distribution,
                )
            } else {
                (input_stat.clone(), false, false, false)
            };
        Self::propagate_generic_stat(&mut stat, side, is_identity, uses_matched_distribution);
        Self::cap_count(&mut stat, output_rows);
        Self::apply_outer_null_extension(&mut stat, output_rows, side.null_extension_rows);
        Self::finish_histogram(
            &mut stat,
            side,
            is_identity,
            has_selected_identity_distribution,
        );
        Self::reconcile_histogram(
            &mut stat,
            output_rows,
            side,
            has_selected_identity_distribution,
        );
        Ok(stat)
    }

    fn rebuild_expression_stat(
        input_stat: &ColumnStat,
        column_stats: &mut JoinColumnStats,
        output_rows: f64,
        side: &JoinSideStats,
    ) -> Result<ColumnStat> {
        if side.expression_output != ExpressionStatOutput::Input
            && let Some(distribution) = column_stats.matched_distribution.take()
        {
            if matches!(side.histogram_output, HistogramStatOutput::Semi) {
                let histogram = if matches!(distribution, ColumnStat::AllNull { .. }) {
                    None
                } else {
                    column_stats
                        .side_matched_histogram
                        .take()
                        .or_else(|| input_stat.histogram().map(|histogram| histogram.to_owned()))
                };
                return Self::rebuild_distribution(distribution, histogram, output_rows);
            }
            if side.expression_output == ExpressionStatOutput::EstimatedWithoutHistograms {
                return Self::rebuild_distribution(distribution, None, output_rows);
            }
            return Ok(distribution);
        }

        if side.rejects_null && column_stats.rejects_null {
            let Some(bounds) = input_stat.bounds() else {
                return Ok(ColumnStat::AllNull {
                    null_count: StatCount::exact(0),
                });
            };
            return ColumnStat::new(
                bounds,
                input_stat.ndv(),
                StatCount::exact(0),
                input_stat.histogram().map(|histogram| histogram.to_owned()),
            )
            .map_err(ErrorCode::Internal);
        }
        Ok(input_stat.clone())
    }

    fn rebuild_distribution(
        distribution: ColumnStat,
        histogram: Option<Histogram>,
        max_num_values: f64,
    ) -> Result<ColumnStat> {
        let Some(bounds) = distribution.bounds() else {
            return if histogram.is_none() {
                Ok(distribution)
            } else {
                Err(ErrorCode::Internal(format!(
                    "column statistic cannot carry histogram: stat={distribution:?}, histogram={histogram:?}"
                )))
            };
        };
        let mut rebuilt = ColumnStat::new(
            bounds.clone(),
            distribution.ndv(),
            distribution.null_count(),
            histogram,
        )
        .map_err(ErrorCode::Internal)?;
        rebuilt
            .restrict_to_bounds(bounds)
            .map_err(ErrorCode::Internal)?;
        if let Some(num_values) = rebuilt.histogram().map(|histogram| histogram.num_values())
            && num_values > max_num_values
            && num_values > 0.0
        {
            rebuilt.scale_histogram_to(max_num_values);
        }
        Ok(rebuilt)
    }

    fn propagate_generic_stat(
        stat: &mut ColumnStat,
        side: &JoinSideStats,
        is_identity: bool,
        uses_matched_distribution: bool,
    ) {
        if is_identity {
            if side.residual_ndv_selectivity < 1.0 {
                let input_rows = if uses_matched_distribution {
                    side.surviving_input_rows
                } else {
                    side.input_rows
                };
                Self::scale_ndv(stat, input_rows, side.residual_ndv_selectivity);
            }
            return;
        }

        let ndv_input_rows = side.input_rows;
        let ndv_surviving_input_rows = side.ndv_surviving_input_rows;
        if ndv_input_rows <= 0.0 || side.value_output_rows <= 0.0 {
            return;
        }

        // Selection changes NDV, while join fanout only duplicates surviving
        // values. NULL counts are row counts, so selection and fanout combine
        // into the total output/input row scale under the independence model.
        let survival_rate = (ndv_surviving_input_rows / ndv_input_rows).clamp(0.0, 1.0);
        let row_scale = side.value_output_rows / side.input_rows;
        match stat {
            ColumnStat::Boolean {
                ndv, null_count, ..
            } => {
                let input_non_null =
                    (ndv_input_rows - null_count.expected()).clamp(0.0, ndv_input_rows);
                *ndv = ndv.reduce_by_selectivity(input_non_null, survival_rate);
                *null_count = Self::scale_count(*null_count, row_scale, side.value_output_rows);
            }
            ColumnStat::Int {
                ndv, null_count, ..
            }
            | ColumnStat::UInt {
                ndv, null_count, ..
            }
            | ColumnStat::Float {
                ndv, null_count, ..
            }
            | ColumnStat::Bytes {
                ndv, null_count, ..
            } => {
                let input_non_null =
                    (ndv_input_rows - null_count.expected()).clamp(0.0, ndv_input_rows);
                *ndv = ndv.reduce_by_selectivity(input_non_null, survival_rate);
                *null_count = Self::scale_count(*null_count, row_scale, side.value_output_rows);
                stat.clear_histogram();
            }
            ColumnStat::AllNull { null_count } => {
                *null_count = Self::scale_count(*null_count, row_scale, side.value_output_rows);
            }
        }
    }

    fn scale_ndv(stat: &mut ColumnStat, input_rows: f64, selectivity: f64) {
        match stat {
            ColumnStat::Boolean {
                ndv, null_count, ..
            }
            | ColumnStat::Int {
                ndv, null_count, ..
            }
            | ColumnStat::UInt {
                ndv, null_count, ..
            }
            | ColumnStat::Float {
                ndv, null_count, ..
            }
            | ColumnStat::Bytes {
                ndv, null_count, ..
            } => {
                let input_non_null = (input_rows - null_count.expected()).clamp(0.0, input_rows);
                *ndv = ndv.reduce_by_selectivity(input_non_null, selectivity);
            }
            ColumnStat::AllNull { .. } => {}
        }
    }

    fn cap_count(stat: &mut ColumnStat, cardinality: f64) {
        match stat {
            ColumnStat::Boolean {
                ndv, null_count, ..
            }
            | ColumnStat::Int {
                ndv, null_count, ..
            }
            | ColumnStat::UInt {
                ndv, null_count, ..
            }
            | ColumnStat::Float {
                ndv, null_count, ..
            }
            | ColumnStat::Bytes {
                ndv, null_count, ..
            } => {
                *ndv = ndv.reduce(cardinality);
                *null_count = null_count.reduce(cardinality);
            }
            ColumnStat::AllNull { null_count } => {
                *null_count = null_count.reduce(cardinality);
            }
        }
    }

    fn apply_outer_null_extension(
        stat: &mut ColumnStat,
        output_rows: f64,
        null_extension_rows: f64,
    ) {
        if null_extension_rows <= 0.0 {
            return;
        }

        if null_extension_rows >= output_rows {
            *stat = ColumnStat::AllNull {
                null_count: StatCount::estimate(output_rows, output_rows),
            };
            return;
        }

        let null_count = stat.null_count();
        stat.set_null_count(StatCount::estimate(
            (null_count.expected() + null_extension_rows).min(output_rows),
            (null_count.upper() + null_extension_rows).min(output_rows),
        ));
    }

    fn reconcile_histogram(
        stat: &mut ColumnStat,
        output_rows: f64,
        side: &JoinSideStats,
        has_selected_identity_distribution: bool,
    ) {
        let keeps_histograms = matches!(
            side.histogram_output,
            HistogramStatOutput::Estimated | HistogramStatOutput::Semi
        );
        let Some(histogram_rows) = stat.histogram().map(|histogram| histogram.num_values()) else {
            return;
        };
        let expected_non_null_rows = (output_rows - stat.null_count().expected()).max(0.0);
        let tolerance = output_rows.max(1.0) * 1e-9;
        if (histogram_rows - expected_non_null_rows).abs() > tolerance {
            if !keeps_histograms
                || !has_selected_identity_distribution
                || !stat.scale_histogram_to(expected_non_null_rows)
            {
                stat.clear_histogram();
            }
        }
    }

    fn finish_histogram(
        stat: &mut ColumnStat,
        side: &JoinSideStats,
        is_identity: bool,
        has_selected_identity_distribution: bool,
    ) {
        let uses_estimated_histograms = matches!(
            side.histogram_output,
            HistogramStatOutput::Estimated | HistogramStatOutput::Semi
        );
        let preserves_unmatched_histograms = matches!(
            side.histogram_output,
            HistogramStatOutput::PreserveUnmatched
        );
        let keep_histogram = if uses_estimated_histograms {
            is_identity && has_selected_identity_distribution
        } else if preserves_unmatched_histograms {
            is_identity && !has_selected_identity_distribution
        } else {
            false
        };
        if !keep_histogram {
            // Other columns' histograms are inaccurate after the join cardinality update.
            stat.clear_histogram();
        }
    }

    fn scale_count(count: StatCount, factor: f64, upper: f64) -> StatCount {
        if count == StatCount::exact(0) {
            return count;
        }
        StatCount::estimate(
            (count.expected() * factor).min(upper),
            (count.upper() * factor).min(upper),
        )
    }
}
