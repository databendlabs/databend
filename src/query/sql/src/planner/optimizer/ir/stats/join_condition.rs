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

use std::borrow::Cow;
use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_statistics::BorrowedHistogram;
use databend_common_statistics::Histogram;
use databend_common_statistics::NumericHistogramType;
use databend_common_statistics::StatBounds;

use super::ColumnStat;
use super::Selectivity;
use super::SelectivityVisitor;
use crate::Symbol;
use crate::optimizer::ir::Statistics;
use crate::plans::EvalScalar;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug)]
pub(super) struct EquiCondition<'expr, 'stats> {
    pub left: &'expr ScalarExpr,
    pub right: &'expr ScalarExpr,
    pub stats: EquiStats<'stats>,
}

#[derive(Clone, Debug)]
pub(super) enum EquiStats<'stats> {
    Missing,
    Left(Cow<'stats, ColumnStat>),
    Right(Cow<'stats, ColumnStat>),
    Complete(Box<CompleteStats<'stats>>),
}

#[derive(Clone, Debug)]
pub(super) struct CompleteStats<'stats> {
    pub left_stat: Cow<'stats, ColumnStat>,
    pub right_stat: Cow<'stats, ColumnStat>,
    pub selectivity: Selectivity,
    pub estimate: EquiEstimate,
}

#[derive(Clone, Debug)]
pub(super) enum EquiEstimate {
    NoOverlap,
    CardinalityOnly(Box<JoinEstimate>),
    Matched(Box<MatchedEstimate>),
}

impl EquiEstimate {
    pub(super) fn cardinality(&self) -> f64 {
        match self {
            Self::NoOverlap => 0.0,
            Self::CardinalityOnly(estimate) => estimate.card,
            Self::Matched(matched) => matched.estimate.card,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct MatchedEstimate {
    pub estimate: JoinEstimate,
    pub left_distribution: ColumnStat,
    pub right_distribution: ColumnStat,
}

impl<'expr, 'stats> EquiCondition<'expr, 'stats> {
    pub fn estimate_locally(
        left: &'expr ScalarExpr,
        right: &'expr ScalarExpr,
        left_input: &'stats Statistics,
        right_input: &'stats Statistics,
        left_cardinality: StatCardinality,
        right_cardinality: StatCardinality,
    ) -> Result<Self> {
        let left_value_distribution = derive_expression_stat(left, left_input, left_cardinality)?;
        let right_value_distribution =
            derive_expression_stat(right, right_input, right_cardinality)?;
        let stats = match (left_value_distribution, right_value_distribution) {
            (Some(left_stat), Some(right_stat)) => {
                let left_rows = left_cardinality.value();
                let right_rows = right_cardinality.value();
                let input_cardinality = left_rows * right_rows;
                let left_type = left.data_type().into_owned();
                let right_type = right.data_type().into_owned();
                let estimate = estimate_join_condition(
                    left_rows,
                    right_rows,
                    &left_type,
                    &right_type,
                    left_stat.as_ref(),
                    right_stat.as_ref(),
                )?;
                let cardinality = estimate.cardinality();
                EquiStats::Complete(Box::new(CompleteStats {
                    left_stat,
                    right_stat,
                    selectivity: Selectivity::N(if input_cardinality == 0.0 {
                        0.0
                    } else {
                        (cardinality / input_cardinality).clamp(0.0, 1.0)
                    }),
                    estimate,
                }))
            }
            (Some(left), None) => EquiStats::Left(left),
            (None, Some(right)) => EquiStats::Right(right),
            (None, None) => EquiStats::Missing,
        };
        Ok(Self { left, right, stats })
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NonEquiCondition {
    pub selectivity: Selectivity,
}

impl NonEquiCondition {
    pub fn estimate_locally(
        predicate: &ScalarExpr,
        input: &Statistics,
        input_cardinality: StatCardinality,
        column_row_scales: &HashMap<Symbol, StatCardinality>,
    ) -> Result<Self> {
        let selectivity = SelectivityVisitor::estimate(
            predicate,
            input_cardinality,
            &input.column_stats,
            &input.top_n,
            &input.count_min_sketch,
            column_row_scales,
        )?;
        Ok(Self { selectivity })
    }
}

fn derive_expression_stat<'a>(
    scalar: &ScalarExpr,
    statistics: &'a Statistics,
    cardinality: StatCardinality,
) -> Result<Option<Cow<'a, ColumnStat>>> {
    if let ScalarExpr::BoundColumnRef(column) = scalar {
        return Ok(statistics
            .column_stats
            .get(&column.column.index)
            .map(Cow::Borrowed));
    }

    EvalScalar::derive_item_stat(scalar, statistics, cardinality)
        .map(|distribution| distribution.map(Cow::Owned))
}

fn estimate_join_condition(
    left_cardinality: f64,
    right_cardinality: f64,
    left_type: &DataType,
    right_type: &DataType,
    left_col_stat: &ColumnStat,
    right_col_stat: &ColumnStat,
) -> Result<EquiEstimate> {
    if matches!(left_col_stat, ColumnStat::AllNull { .. })
        || matches!(right_col_stat, ColumnStat::AllNull { .. })
    {
        return Ok(EquiEstimate::NoOverlap);
    }

    JoinConditionEstimation {
        left_type,
        right_type,
        left_col_stat,
        right_col_stat,
        left_cardinality: (left_cardinality
            - join_key_null_count_for_cardinality(left_col_stat, left_cardinality))
        .max(0.0),
        right_cardinality: (right_cardinality
            - join_key_null_count_for_cardinality(right_col_stat, right_cardinality))
        .max(0.0),
    }
    .estimate()
}

pub(super) fn join_key_null_count_for_cardinality(stat: &ColumnStat, cardinality: f64) -> f64 {
    let known_non_null_count = stat.ndv().expected.unwrap_or(0.0);
    let max_null_count = (cardinality - known_non_null_count).max(0.0);
    stat.null_count().expected().min(max_null_count)
}

struct JoinConditionEstimation<'a> {
    left_type: &'a DataType,
    right_type: &'a DataType,
    left_col_stat: &'a ColumnStat,
    right_col_stat: &'a ColumnStat,
    left_cardinality: f64,
    right_cardinality: f64,
}

impl<'a> JoinConditionEstimation<'a> {
    fn estimate(&self) -> Result<EquiEstimate> {
        let (Some(left_bounds), Some(right_bounds)) =
            (self.left_col_stat.bounds(), self.right_col_stat.bounds())
        else {
            return Err(ErrorCode::Internal(
                "join value estimation received all-NULL column statistics",
            ));
        };
        let left_type = self.left_type.remove_nullable();
        let right_type = self.right_type.remove_nullable();
        if matches!(
            (&left_type, &right_type),
            (DataType::Number(_), DataType::Number(_))
        ) {
            if !left_bounds.is_numeric() || !right_bounds.is_numeric() {
                return Err(ErrorCode::Internal(format!(
                    "numeric join condition requires numeric statistics bounds: left_type={left_type:?}, left_bounds={left_bounds:?}, right_type={right_type:?}, right_bounds={right_bounds:?}"
                )));
            }
            let Some(return_type) = numeric_join_return_type(&left_type, &right_type) else {
                return Ok(EquiEstimate::CardinalityOnly(Box::new(
                    JoinEstimate::from_ndv_inputs(
                        self.left_col_stat,
                        self.right_col_stat,
                        self.left_cardinality,
                        self.right_cardinality,
                    ),
                )));
            };
            let Some((left_output_bounds, right_output_bounds)) =
                left_bounds.numeric_intersection(&right_bounds, return_type)?
            else {
                return Ok(EquiEstimate::NoOverlap);
            };
            let estimate = JoinEstimate::from_inputs(
                self.left_col_stat,
                self.right_col_stat,
                self.left_cardinality,
                self.right_cardinality,
                Some(return_type),
            )?;
            return self.matched_estimate(left_output_bounds, right_output_bounds, estimate);
        }

        if !same_semantic_stat_type(&left_type, &right_type) {
            return Ok(EquiEstimate::CardinalityOnly(Box::new(
                JoinEstimate::from_ndv_inputs(
                    self.left_col_stat,
                    self.right_col_stat,
                    self.left_cardinality,
                    self.right_cardinality,
                ),
            )));
        }
        let Some(bounds) = left_bounds.intersection(&right_bounds) else {
            return Ok(EquiEstimate::NoOverlap);
        };
        let estimate = JoinEstimate::from_inputs(
            self.left_col_stat,
            self.right_col_stat,
            self.left_cardinality,
            self.right_cardinality,
            None,
        )?;
        self.matched_estimate(bounds.clone(), bounds, estimate)
    }

    fn matched_estimate(
        &self,
        left_bounds: StatBounds,
        right_bounds: StatBounds,
        estimate: JoinEstimate,
    ) -> Result<EquiEstimate> {
        let mut left_distribution = self.left_col_stat.clone();
        let mut right_distribution = self.right_col_stat.clone();
        left_distribution
            .restrict_to_bounds(left_bounds)
            .map_err(ErrorCode::Internal)?;
        right_distribution
            .restrict_to_bounds(right_bounds)
            .map_err(ErrorCode::Internal)?;
        if let Some(ndv) = estimate.ndv {
            left_distribution.set_ndv(ndv);
            right_distribution.set_ndv(ndv);
        }
        left_distribution
            .set_histogram(estimate.histogram.clone())
            .map_err(ErrorCode::Internal)?;
        right_distribution
            .set_histogram(estimate.histogram.clone())
            .map_err(ErrorCode::Internal)?;
        left_distribution.set_null_count(StatCount::exact(0));
        right_distribution.set_null_count(StatCount::exact(0));
        Ok(EquiEstimate::Matched(Box::new(MatchedEstimate {
            estimate,
            left_distribution,
            right_distribution,
        })))
    }
}

#[derive(Clone, Debug)]
pub(super) struct JoinEstimate {
    pub(super) card: f64,
    pub(super) ndv: Option<NdvEstimate>,
    histogram: Option<Histogram>,
    pub(super) left_matched_histogram: Option<Histogram>,
    pub(super) right_matched_histogram: Option<Histogram>,
    pub(super) left_matched_rows: f64,
    pub(super) right_matched_rows: f64,
    pub(super) left_estimated_matched_rows: f64,
    pub(super) right_estimated_matched_rows: f64,
}

impl JoinEstimate {
    fn from_inputs(
        left: &ColumnStat,
        right: &ColumnStat,
        left_cardinality: f64,
        right_cardinality: f64,
        numeric_return_type: Option<NumericHistogramType>,
    ) -> Result<Self> {
        let histogram_estimation = match (left.histogram(), right.histogram()) {
            (Some(left_hist), Some(right_hist))
                if left_hist.is_range_distorted() || right_hist.is_range_distorted() =>
            {
                None
            }
            (Some(left_hist), Some(right_hist)) => match numeric_return_type {
                Some(return_type) => left_hist.estimate_join_numeric(right_hist, return_type)?,
                None => match (left_hist, right_hist) {
                    (BorrowedHistogram::Int(left), BorrowedHistogram::Int(right)) => {
                        Some(left.estimate_join(right))
                    }
                    (BorrowedHistogram::UInt(left), BorrowedHistogram::UInt(right)) => {
                        Some(left.estimate_join(right))
                    }
                    (BorrowedHistogram::Float(left), BorrowedHistogram::Float(right)) => {
                        Some(left.estimate_join(right))
                    }
                    _ => None,
                },
            },
            _ => None,
        };
        if let Some(estimation) = histogram_estimation {
            return Ok(Self {
                card: estimation.cardinality.expected,
                ndv: Some(estimation.ndv),
                histogram: estimation.histogram,
                left_matched_histogram: estimation.left_matched_histogram,
                right_matched_histogram: estimation.right_matched_histogram,
                left_matched_rows: estimation.left_matched_rows.min(left_cardinality),
                right_matched_rows: estimation.right_matched_rows.min(right_cardinality),
                left_estimated_matched_rows: estimation
                    .left_estimated_matched_rows
                    .min(left_cardinality),
                right_estimated_matched_rows: estimation
                    .right_estimated_matched_rows
                    .min(right_cardinality),
            });
        }

        Ok(Self::from_ndv_inputs(
            left,
            right,
            left_cardinality,
            right_cardinality,
        ))
    }

    fn from_ndv_inputs(
        left: &ColumnStat,
        right: &ColumnStat,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> Self {
        let left_ndv = left.ndv();
        let right_ndv = right.ndv();
        let max_ndv = match (left_ndv.expected, right_ndv.expected) {
            (Some(left), Some(right)) => left.max(right),
            (Some(left), None) => left,
            (None, Some(right)) => right,
            (None, None) => {
                if left_ndv.upper == 0.0 && right_ndv.upper == 0.0 {
                    0.0
                } else {
                    left_cardinality * right_cardinality
                }
            }
        };

        let card = if max_ndv == 0.0 {
            0.0
        } else {
            left_cardinality * right_cardinality / max_ndv
        };
        let ndv = left_ndv.min(right_ndv);
        Self {
            card,
            ndv: Some(ndv),
            histogram: None,
            left_matched_histogram: None,
            right_matched_histogram: None,
            left_matched_rows: estimate_matched_rows(left_cardinality, left_ndv, ndv),
            right_matched_rows: estimate_matched_rows(right_cardinality, right_ndv, ndv),
            left_estimated_matched_rows: 0.0,
            right_estimated_matched_rows: 0.0,
        }
    }
}

fn estimate_matched_rows(
    cardinality: f64,
    input_ndv: NdvEstimate,
    matched_ndv: NdvEstimate,
) -> f64 {
    // Keep the matched-row estimate consistent with the uniform-frequency and
    // value-set-containment assumptions used by the NDV join cardinality formula.
    // A matched value retains its input rows; join fanout is accounted for later.
    let Some(input_ndv) = input_ndv.expected else {
        return cardinality;
    };
    let Some(matched_ndv) = matched_ndv.expected else {
        return cardinality;
    };
    if input_ndv <= 0.0 {
        return 0.0;
    }
    (cardinality * matched_ndv / input_ndv).clamp(0.0, cardinality)
}

fn numeric_join_return_type(
    left_type: &DataType,
    right_type: &DataType,
) -> Option<NumericHistogramType> {
    let cast_rules = &BUILTIN_FUNCTIONS.get_auto_cast_rules("eq");
    let return_type = common_super_type(left_type.clone(), right_type.clone(), cast_rules)?;

    match return_type.remove_nullable() {
        DataType::Number(number) if number.is_float() => Some(NumericHistogramType::Float),
        DataType::Number(number) if number.is_signed() => Some(NumericHistogramType::Int),
        DataType::Number(
            NumberDataType::UInt8
            | NumberDataType::UInt16
            | NumberDataType::UInt32
            | NumberDataType::UInt64,
        ) => Some(NumericHistogramType::UInt),
        _ => None,
    }
}

fn same_semantic_stat_type(left_type: &DataType, right_type: &DataType) -> bool {
    matches!(
        (left_type, right_type),
        (DataType::Boolean, DataType::Boolean)
            | (DataType::String, DataType::String)
            | (DataType::Binary, DataType::Binary)
            | (DataType::Decimal(_), DataType::Decimal(_))
            | (DataType::Date, DataType::Date)
            | (DataType::Timestamp, DataType::Timestamp)
            | (DataType::TimestampTz, DataType::TimestampTz)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::stat_distribution::NdvEstimate;
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;

    fn statistics(
        precise_cardinality: u64,
        columns: impl IntoIterator<Item = (Symbol, ColumnStat)>,
    ) -> Statistics {
        Statistics {
            precise_cardinality: Some(precise_cardinality),
            column_stats: HashMap::from_iter(columns),
            ..Default::default()
        }
    }

    fn int_stat(min: i64, max: i64, ndv: f64) -> ColumnStat {
        ColumnStat::Int {
            min,
            max,
            ndv: NdvEstimate::exact(ndv),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }

    fn int_column(index: usize) -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(DataType::Number(NumberDataType::Int64)),
                Visibility::Visible,
            )
            .build(),
        })
    }

    #[test]
    fn local_cardinality_uses_explicit_input_cardinalities() -> Result<()> {
        let left_input = statistics(100, [(Symbol::new(0), int_stat(1, 50, 50.0))]);
        let right_input = statistics(100, [(Symbol::new(10), int_stat(1, 40, 40.0))]);
        let left_key = int_column(0);
        let right_key = int_column(10);

        let condition = EquiCondition::estimate_locally(
            &left_key,
            &right_key,
            &left_input,
            &right_input,
            StatCardinality::estimate(200.0),
            StatCardinality::exact(100),
        )?;

        let EquiStats::Complete(stats) = condition.stats else {
            panic!("expected complete equality-condition statistics");
        };
        let Selectivity::N(selectivity) = stats.selectivity else {
            panic!("expected numeric selectivity");
        };
        assert!((selectivity - 1.0 / 50.0).abs() < 1e-12);
        assert_eq!(stats.estimate.cardinality(), 400.0);
        Ok(())
    }

    #[test]
    fn numeric_join_requires_numeric_statistics_bounds() {
        let left_stat = ColumnStat::Boolean {
            min: false,
            max: true,
            ndv: NdvEstimate::exact(2.0),
            null_count: StatCount::exact(0),
        };
        let right_stat = int_stat(0, 1, 2.0);

        let err = JoinConditionEstimation {
            left_type: &DataType::Number(NumberDataType::Int8),
            right_type: &DataType::Number(NumberDataType::Int8),
            left_col_stat: &left_stat,
            right_col_stat: &right_stat,
            left_cardinality: 2.0,
            right_cardinality: 2.0,
        }
        .estimate()
        .expect_err("invalid numeric statistics bounds must return an error");

        assert!(
            err.message()
                .starts_with("numeric join condition requires numeric statistics bounds")
        );
    }
}
