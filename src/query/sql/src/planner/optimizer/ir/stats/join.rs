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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::stat_distribution::NdvEstimate;
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
use super::ColumnStatSet;
use crate::Symbol;

pub(crate) struct JoinStats {
    pub(crate) cardinality: f64,
    pub(crate) ndv: Option<NdvEstimate>,
    pub(crate) left: JoinSideStats,
    pub(crate) right: JoinSideStats,
    pub(crate) updated_columns: Option<JoinConditionColumns>,
}

pub(crate) struct JoinSideStats {
    input_cardinality: f64,
    pub(crate) matched_rows: f64,
    pub(crate) estimated_matched_rows: Option<f64>,
    pub(crate) matched_histogram: Option<Histogram>,
}

impl JoinSideStats {
    pub(crate) fn unmatched_rows(&self) -> f64 {
        (self.input_cardinality - self.matched_rows).clamp(0.0, self.input_cardinality)
    }
}

pub(crate) struct JoinStatsEstimator {
    cardinality: f64,
    ndv: Option<NdvEstimate>,
    left: JoinSideStats,
    right: JoinSideStats,
    updated_columns: Option<JoinConditionColumns>,
    drop_null_join_keys: bool,
}

impl JoinStatsEstimator {
    pub(crate) fn new(
        left_cardinality: f64,
        right_cardinality: f64,
        drop_null_join_keys: bool,
    ) -> Self {
        let has_join_pairs = left_cardinality > 0.0 && right_cardinality > 0.0;
        Self {
            cardinality: left_cardinality * right_cardinality,
            ndv: None,
            left: JoinSideStats {
                input_cardinality: left_cardinality,
                matched_rows: if has_join_pairs {
                    left_cardinality
                } else {
                    0.0
                },
                estimated_matched_rows: None,
                matched_histogram: None,
            },
            right: JoinSideStats {
                input_cardinality: right_cardinality,
                matched_rows: if has_join_pairs {
                    right_cardinality
                } else {
                    0.0
                },
                estimated_matched_rows: None,
                matched_histogram: None,
            },
            updated_columns: None,
            drop_null_join_keys,
        }
    }

    pub(crate) fn has_no_matches(&self) -> bool {
        self.cardinality == 0.0
    }

    pub(crate) fn finish(self) -> JoinStats {
        JoinStats {
            cardinality: self.cardinality,
            ndv: self.ndv,
            left: self.left,
            right: self.right,
            updated_columns: self.updated_columns,
        }
    }

    pub(crate) fn apply_missing_condition_statistics(
        &mut self,
        left_stat: Option<&ColumnStat>,
        right_stat: Option<&ColumnStat>,
        is_null_equal: bool,
    ) {
        if is_null_equal {
            return;
        }

        let left_null_count = left_stat
            .map(|stat| join_key_null_count_for_cardinality(stat, self.left.input_cardinality))
            .unwrap_or(0.0);
        let right_null_count = right_stat
            .map(|stat| join_key_null_count_for_cardinality(stat, self.right.input_cardinality))
            .unwrap_or(0.0);
        let left_non_null = (self.left.input_cardinality - left_null_count).max(0.0);
        let right_non_null = (self.right.input_cardinality - right_null_count).max(0.0);
        let card = left_non_null * right_non_null;
        if card < self.cardinality {
            self.cardinality = card;
            self.ndv = None;
            self.left.matched_rows = if right_non_null > 0.0 {
                left_non_null
            } else {
                0.0
            };
            self.right.matched_rows = if left_non_null > 0.0 {
                right_non_null
            } else {
                0.0
            };
            self.left.estimated_matched_rows = None;
            self.right.estimated_matched_rows = None;
            self.left.matched_histogram = None;
            self.right.matched_histogram = None;
        }
    }

    fn apply_estimated_condition(
        &mut self,
        estimated: EstimatedJoinCondition,
        output_columns: Option<JoinConditionColumns>,
        left_column_stats: &mut ColumnStatSet,
        right_column_stats: &mut ColumnStatSet,
        null_match_cardinality: f64,
        left_null_matched_rows: f64,
        right_null_matched_rows: f64,
    ) -> Result<()> {
        let EstimatedJoinCondition {
            left_bounds,
            right_bounds,
            estimate:
                JoinEstimate {
                    card,
                    ndv,
                    histogram,
                    left_matched_histogram,
                    right_matched_histogram,
                    left_matched_rows,
                    right_matched_rows,
                    left_estimated_matched_rows,
                    right_estimated_matched_rows,
                },
        } = estimated;

        if let Some(columns) = output_columns {
            let left_stat = left_column_stats.get_mut(&columns.left).unwrap();
            let right_stat = right_column_stats.get_mut(&columns.right).unwrap();
            left_stat
                .restrict_to_bounds(left_bounds)
                .map_err(ErrorCode::Internal)?;
            right_stat
                .restrict_to_bounds(right_bounds)
                .map_err(ErrorCode::Internal)?;
            if let Some(ndv) = ndv {
                left_stat.set_ndv(ndv);
                right_stat.set_ndv(ndv);
            }
            left_stat
                .set_histogram(histogram.clone())
                .map_err(ErrorCode::Internal)?;
            right_stat
                .set_histogram(histogram)
                .map_err(ErrorCode::Internal)?;

            let output_null_count =
                StatCount::estimate(null_match_cardinality, null_match_cardinality);
            left_stat.set_null_count(output_null_count);
            right_stat.set_null_count(output_null_count);
        }

        let card = card + null_match_cardinality;
        if card < self.cardinality {
            self.cardinality = card;
            self.ndv = ndv;
            self.left.matched_rows = left_matched_rows + left_null_matched_rows;
            self.right.matched_rows = right_matched_rows + right_null_matched_rows;
            self.left.estimated_matched_rows = Some(left_estimated_matched_rows);
            self.right.estimated_matched_rows = Some(right_estimated_matched_rows);
            self.left.matched_histogram = left_matched_histogram;
            self.right.matched_histogram = right_matched_histogram;
            if let Some(columns) = output_columns {
                self.updated_columns = Some(columns);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_condition(
        &mut self,
        output_columns: Option<JoinConditionColumns>,
        left_type: &DataType,
        right_type: &DataType,
        left_col_stat: &ColumnStat,
        right_col_stat: &ColumnStat,
        is_null_equal: bool,
        left_column_stats: &mut ColumnStatSet,
        right_column_stats: &mut ColumnStatSet,
    ) -> Result<()> {
        let left_null_count =
            join_key_null_count_for_cardinality(left_col_stat, self.left.input_cardinality);
        let right_null_count =
            join_key_null_count_for_cardinality(right_col_stat, self.right.input_cardinality);
        let left_cardinality = (self.left.input_cardinality - left_null_count).max(0.0);
        let right_cardinality = (self.right.input_cardinality - right_null_count).max(0.0);
        let null_match_cardinality = if is_null_equal {
            left_null_count * right_null_count
        } else {
            0.0
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
        if self.drop_null_join_keys
            && !is_null_equal
            && let Some(columns) = output_columns
        {
            if let Some(stat) = left_column_stats.get_mut(&columns.left) {
                stat.set_null_count(StatCount::exact(0));
            }
            if let Some(stat) = right_column_stats.get_mut(&columns.right) {
                stat.set_null_count(StatCount::exact(0));
            }
        }

        let left_all_null = matches!(left_col_stat, ColumnStat::AllNull { .. });
        let right_all_null = matches!(right_col_stat, ColumnStat::AllNull { .. });
        if !is_null_equal && (left_all_null || right_all_null) {
            self.cardinality = 0.0;
            self.ndv = Some(NdvEstimate::exact(0.0));
            self.left.matched_rows = 0.0;
            self.right.matched_rows = 0.0;
            self.left.estimated_matched_rows = None;
            self.right.estimated_matched_rows = None;
            self.left.matched_histogram = None;
            self.right.matched_histogram = None;
            return Ok(());
        }
        if is_null_equal && (left_all_null || right_all_null) {
            let card = null_match_cardinality;
            if let Some(columns) = output_columns {
                left_column_stats.insert(columns.left, ColumnStat::AllNull {
                    null_count: StatCount::estimate(card, card),
                });
                right_column_stats.insert(columns.right, ColumnStat::AllNull {
                    null_count: StatCount::estimate(card, card),
                });
            }
            if card < self.cardinality {
                self.cardinality = card;
                self.ndv = Some(NdvEstimate::exact(if card > 0.0 { 1.0 } else { 0.0 }));
                self.left.matched_rows = left_null_matched_rows;
                self.right.matched_rows = right_null_matched_rows;
                self.left.estimated_matched_rows = None;
                self.right.estimated_matched_rows = None;
                self.left.matched_histogram = None;
                self.right.matched_histogram = None;
                if let Some(columns) = output_columns {
                    self.updated_columns = Some(columns);
                }
            }
            return Ok(());
        }

        let condition_stats = JoinConditionEstimation {
            left_type,
            right_type,
            left_col_stat,
            right_col_stat,
            left_cardinality,
            right_cardinality,
        }
        .estimate()?;

        match condition_stats {
            JoinConditionStats::Skip => {}
            JoinConditionStats::NoOverlap => {
                if null_match_cardinality > 0.0
                    && let Some(columns) = output_columns
                {
                    left_column_stats.insert(columns.left, ColumnStat::AllNull {
                        null_count: StatCount::estimate(
                            null_match_cardinality,
                            null_match_cardinality,
                        ),
                    });
                    right_column_stats.insert(columns.right, ColumnStat::AllNull {
                        null_count: StatCount::estimate(
                            null_match_cardinality,
                            null_match_cardinality,
                        ),
                    });
                }
                if null_match_cardinality < self.cardinality {
                    self.cardinality = null_match_cardinality;
                    self.ndv = Some(NdvEstimate::exact(if null_match_cardinality > 0.0 {
                        1.0
                    } else {
                        0.0
                    }));
                    self.left.matched_rows = left_null_matched_rows;
                    self.right.matched_rows = right_null_matched_rows;
                    self.left.estimated_matched_rows = None;
                    self.right.estimated_matched_rows = None;
                    self.left.matched_histogram = None;
                    self.right.matched_histogram = None;
                    if null_match_cardinality > 0.0
                        && let Some(columns) = output_columns
                    {
                        self.updated_columns = Some(columns);
                    }
                }
            }
            JoinConditionStats::Estimated(estimated) => self.apply_estimated_condition(
                *estimated,
                output_columns,
                left_column_stats,
                right_column_stats,
                null_match_cardinality,
                left_null_matched_rows,
                right_null_matched_rows,
            )?,
        };
        Ok(())
    }
}

fn join_key_null_count_for_cardinality(stat: &ColumnStat, cardinality: f64) -> f64 {
    let known_non_null_count = stat.ndv().expected.unwrap_or(0.0);
    let max_null_count = (cardinality - known_non_null_count).max(0.0);
    stat.null_count().expected().min(max_null_count)
}

#[derive(Clone, Copy)]
pub(crate) struct JoinConditionColumns {
    pub(crate) left: Symbol,
    pub(crate) right: Symbol,
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
    fn estimate(&self) -> Result<JoinConditionStats> {
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
                return Ok(JoinConditionStats::Skip);
            };
            let Some((left_output_bounds, right_output_bounds)) =
                left_bounds.numeric_intersection(&right_bounds, return_type)?
            else {
                return Ok(JoinConditionStats::NoOverlap);
            };
            let estimate = JoinEstimate::from_inputs(
                self.left_col_stat,
                self.right_col_stat,
                self.left_cardinality,
                self.right_cardinality,
                Some(return_type),
            )?;
            return Ok(JoinConditionStats::Estimated(Box::new(
                EstimatedJoinCondition {
                    left_bounds: left_output_bounds,
                    right_bounds: right_output_bounds,
                    estimate,
                },
            )));
        }

        if !same_semantic_stat_type(&left_type, &right_type) {
            return Ok(JoinConditionStats::Skip);
        }
        let Some(bounds) = left_bounds.intersection(&right_bounds) else {
            return Ok(JoinConditionStats::NoOverlap);
        };
        let estimate = JoinEstimate::from_inputs(
            self.left_col_stat,
            self.right_col_stat,
            self.left_cardinality,
            self.right_cardinality,
            None,
        )?;
        Ok(JoinConditionStats::Estimated(Box::new(
            EstimatedJoinCondition {
                left_bounds: bounds.clone(),
                right_bounds: bounds,
                estimate,
            },
        )))
    }
}

enum JoinConditionStats {
    Skip,
    NoOverlap,
    Estimated(Box<EstimatedJoinCondition>),
}

struct EstimatedJoinCondition {
    left_bounds: StatBounds,
    right_bounds: StatBounds,
    estimate: JoinEstimate,
}

struct JoinEstimate {
    card: f64,
    ndv: Option<NdvEstimate>,
    histogram: Option<Histogram>,
    left_matched_histogram: Option<Histogram>,
    right_matched_histogram: Option<Histogram>,
    left_matched_rows: f64,
    right_matched_rows: f64,
    left_estimated_matched_rows: f64,
    right_estimated_matched_rows: f64,
}

impl JoinEstimate {
    fn from_inputs(
        left: &ColumnStat,
        right: &ColumnStat,
        left_cardinality: f64,
        right_cardinality: f64,
        numeric_return_type: Option<NumericHistogramType>,
    ) -> Result<Self> {
        let left_ndv = left.ndv();
        let right_ndv = right.ndv();
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
        Ok(Self {
            card,
            ndv: Some(ndv),
            histogram: None,
            left_matched_histogram: None,
            right_matched_histogram: None,
            left_matched_rows: estimate_matched_rows(left_cardinality, left_ndv, ndv),
            right_matched_rows: estimate_matched_rows(right_cardinality, right_ndv, ndv),
            left_estimated_matched_rows: 0.0,
            right_estimated_matched_rows: 0.0,
        })
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

    use databend_common_expression::types::NumberDataType;
    use databend_common_statistics::TypedHistogram;
    use databend_common_statistics::TypedHistogramBucket;

    use super::*;

    fn int_join_column_stat(min: i64, max: i64, ndv: NdvEstimate) -> ColumnStat {
        ColumnStat::Int {
            min,
            max,
            ndv,
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }

    #[test]
    fn test_mixed_numeric_stats_get_comparison_return_type() {
        let return_type = numeric_join_return_type(
            &DataType::Number(NumberDataType::Int32),
            &DataType::Number(NumberDataType::UInt8),
        )
        .expect("mixed numeric stats should have a comparison type");
        assert_eq!(return_type, NumericHistogramType::Int);
        assert_eq!(
            numeric_join_return_type(
                &DataType::Number(NumberDataType::Int64),
                &DataType::Number(NumberDataType::UInt64),
            ),
            Some(NumericHistogramType::Int)
        );
        assert_eq!(
            numeric_join_return_type(
                &DataType::Number(NumberDataType::Int64),
                &DataType::Number(NumberDataType::Float32),
            ),
            Some(NumericHistogramType::Float)
        );
    }

    #[test]
    fn test_numeric_join_requires_numeric_statistics_bounds() {
        let left_stat = ColumnStat::Boolean {
            min: false,
            max: true,
            ndv: NdvEstimate::exact(2.0),
            null_count: StatCount::exact(0),
        };
        let right_stat = int_join_column_stat(0, 1, NdvEstimate::exact(2.0));

        let err = JoinConditionEstimation {
            left_type: &DataType::Number(NumberDataType::Int8),
            right_type: &DataType::Number(NumberDataType::Int8),
            left_col_stat: &left_stat,
            right_col_stat: &right_stat,
            left_cardinality: 2.0,
            right_cardinality: 2.0,
        }
        .estimate()
        .err()
        .expect("invalid numeric statistics bounds must return an error");

        assert!(
            err.message()
                .starts_with("numeric join condition requires numeric statistics bounds")
        );
    }

    #[test]
    fn test_all_null_join_key_matches_nulls_for_null_safe_equality() -> Result<()> {
        let mut left_stats = HashMap::from([(Symbol::new(0), ColumnStat::AllNull {
            null_count: StatCount::exact(4),
        })]);
        let mut right_stats = HashMap::from([(Symbol::new(1), ColumnStat::Int {
            min: 1,
            max: 3,
            ndv: NdvEstimate::exact(3.0),
            null_count: StatCount::exact(2),
            histogram: None,
        })]);
        let mut estimator = JoinStatsEstimator::new(4.0, 10.0, false);
        let left_stat = left_stats[&Symbol::new(0)].clone();
        let right_stat = right_stats[&Symbol::new(1)].clone();

        estimator.apply_condition(
            Some(JoinConditionColumns {
                left: Symbol::new(0),
                right: Symbol::new(1),
            }),
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &left_stat,
            &right_stat,
            true,
            &mut left_stats,
            &mut right_stats,
        )?;

        let stats = estimator.finish();
        assert_eq!(stats.cardinality, 8.0);
        assert_eq!(stats.left.matched_rows, 4.0);
        assert_eq!(stats.right.matched_rows, 2.0);
        assert_eq!(
            left_stats.get(&Symbol::new(0)),
            Some(&ColumnStat::AllNull {
                null_count: StatCount::estimate(8.0, 8.0),
            }),
            "the left join key is NULL in every output row"
        );
        assert_eq!(
            right_stats.get(&Symbol::new(1)),
            Some(&ColumnStat::AllNull {
                null_count: StatCount::estimate(8.0, 8.0),
            }),
            "the right join key is NULL in every output row"
        );
        Ok(())
    }

    #[test]
    fn test_null_safe_equality_combines_value_and_null_matches() -> Result<()> {
        let mut left_stats = HashMap::from([(Symbol::new(0), ColumnStat::Int {
            min: 1,
            max: 1,
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(2),
            histogram: None,
        })]);
        let mut right_stats = HashMap::from([(Symbol::new(1), ColumnStat::Int {
            min: 1,
            max: 1,
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(3),
            histogram: None,
        })]);
        let mut estimator = JoinStatsEstimator::new(3.0, 4.0, false);
        let left_stat = left_stats[&Symbol::new(0)].clone();
        let right_stat = right_stats[&Symbol::new(1)].clone();

        estimator.apply_condition(
            Some(JoinConditionColumns {
                left: Symbol::new(0),
                right: Symbol::new(1),
            }),
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            &left_stat,
            &right_stat,
            true,
            &mut left_stats,
            &mut right_stats,
        )?;

        // [1, NULL, NULL] NULL-safe joined with [1, NULL, NULL, NULL] has
        // one value match and six NULL matches.
        let stats = estimator.finish();
        assert_eq!(stats.cardinality, 7.0);
        assert_eq!(stats.left.matched_rows, 3.0);
        assert_eq!(stats.right.matched_rows, 4.0);
        Ok(())
    }

    #[test]
    fn test_date_timestamp_join_skips_int_histogram_estimation() -> Result<()> {
        let left_stat = ColumnStat::Int {
            min: 0,
            max: 10,
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: Some(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
                avg_spacing: None,
            }),
        };
        let right_stat = ColumnStat::Int {
            min: 0,
            max: 10,
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: Some(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(0, 10, 10.0, 10.0)],
                avg_spacing: None,
            }),
        };

        let estimation = JoinConditionEstimation {
            left_type: &DataType::Date,
            right_type: &DataType::Timestamp,
            left_col_stat: &left_stat,
            right_col_stat: &right_stat,
            left_cardinality: 10.0,
            right_cardinality: 10.0,
        }
        .estimate()?;

        assert!(matches!(estimation, JoinConditionStats::Skip));
        Ok(())
    }
}
