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

use databend_common_exception::Result;
use databend_common_expression::conversion::ConversionClass;
use databend_common_expression::conversion::common_super_type_with_conversion;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_statistics::Datum;
use databend_common_statistics::DatumKind;
use databend_common_statistics::Histogram;
use databend_common_statistics::UniformSampleSet;

use super::ColumnStat;
use crate::Symbol;
use crate::optimizer::ir::property::Statistics;

pub(crate) struct JoinStatsEstimator {
    left_cardinality: f64,
    right_cardinality: f64,
    join_card: f64,
    updated_columns: Option<JoinConditionColumns>,
    drop_null_join_keys: bool,
}

impl JoinStatsEstimator {
    pub(crate) fn new(
        left_cardinality: f64,
        right_cardinality: f64,
        drop_null_join_keys: bool,
    ) -> Self {
        Self {
            left_cardinality,
            right_cardinality,
            join_card: left_cardinality * right_cardinality,
            updated_columns: None,
            drop_null_join_keys,
        }
    }

    pub(crate) fn join_card(&self) -> f64 {
        self.join_card
    }

    pub(crate) fn updated_columns(&self) -> Option<JoinConditionColumns> {
        self.updated_columns
    }

    pub(crate) fn apply_condition(
        &mut self,
        columns: JoinConditionColumns,
        left_type: DataType,
        right_type: DataType,
        is_null_equal: bool,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<()> {
        let (left_cardinality, right_cardinality) = if !is_null_equal {
            let left_null_count = left_statistics
                .column_stats
                .get(&columns.left)
                .map(|stat| join_key_null_count_for_cardinality(stat, self.left_cardinality))
                .unwrap_or(0.0);
            let right_null_count = right_statistics
                .column_stats
                .get(&columns.right)
                .map(|stat| join_key_null_count_for_cardinality(stat, self.right_cardinality))
                .unwrap_or(0.0);
            (
                (self.left_cardinality - left_null_count).max(0.0),
                (self.right_cardinality - right_null_count).max(0.0),
            )
        } else {
            (self.left_cardinality, self.right_cardinality)
        };
        if self.drop_null_join_keys && !is_null_equal {
            if let Some(stat) = left_statistics.column_stats.get_mut(&columns.left) {
                stat.null_count = StatCount::exact(0);
            }
            if let Some(stat) = right_statistics.column_stats.get_mut(&columns.right) {
                stat.null_count = StatCount::exact(0);
            }
        }

        let condition_stats = match try {
            JoinConditionEstimation {
                left_type,
                right_type,
                left_col_stat: left_statistics.column_stats.get(&columns.left)?,
                right_col_stat: right_statistics.column_stats.get(&columns.right)?,
                left_cardinality,
                right_cardinality,
            }
        } {
            Some(estimation) => estimation.estimate()?,
            None => return Ok(()),
        };

        match condition_stats {
            JoinConditionStats::Skip => {}
            JoinConditionStats::NoOverlap => {
                self.join_card = 0.0;
            }
            JoinConditionStats::Estimated { new_stat, card } => {
                let left_stat = left_statistics.column_stats.get_mut(&columns.left).unwrap();
                let right_stat = right_statistics
                    .column_stats
                    .get_mut(&columns.right)
                    .unwrap();
                new_stat.apply(left_stat, right_stat);

                if card < self.join_card {
                    self.join_card = card;
                    self.updated_columns = Some(columns);
                }
            }
        };
        Ok(())
    }
}

fn join_key_null_count_for_cardinality(stat: &ColumnStat, cardinality: f64) -> f64 {
    let known_non_null_count = stat.ndv.expected.unwrap_or(0.0);
    let max_null_count = (cardinality - known_non_null_count).max(0.0);
    stat.null_count.expected().min(max_null_count)
}

#[derive(Clone, Copy)]
pub(crate) struct JoinConditionColumns {
    pub(crate) left: Symbol,
    pub(crate) right: Symbol,
}

struct JoinConditionEstimation<'a> {
    left_type: DataType,
    right_type: DataType,
    left_col_stat: &'a ColumnStat,
    right_col_stat: &'a ColumnStat,
    left_cardinality: f64,
    right_cardinality: f64,
}

impl<'a> JoinConditionEstimation<'a> {
    fn estimate(&self) -> Result<JoinConditionStats> {
        let left_input = JoinColumnInput::from_column_stat(self.left_col_stat);
        let right_input = JoinColumnInput::from_column_stat(self.right_col_stat);
        if left_input
            .interval()
            .has_same_supported_type(&right_input.interval())
        {
            let Some(estimation) = JoinEstimate::from_inputs(
                &left_input,
                &right_input,
                self.left_cardinality,
                self.right_cardinality,
            )?
            else {
                return Ok(JoinConditionStats::NoOverlap);
            };
            return Ok(JoinConditionStats::Estimated {
                new_stat: JoinKeyStatUpdate::same_type(
                    estimation.min,
                    estimation.max,
                    estimation.ndv,
                    estimation.histogram,
                ),
                card: estimation.card,
            });
        }

        let Some(kind) = mixed_numeric_stat_kind(
            self.left_col_stat,
            self.right_col_stat,
            &self.left_type,
            &self.right_type,
        )?
        else {
            return Ok(JoinConditionStats::Skip);
        };
        let Some(left_input) = left_input.normalize_to_kind(kind) else {
            return Ok(JoinConditionStats::Skip);
        };
        let Some(right_input) = right_input.normalize_to_kind(kind) else {
            return Ok(JoinConditionStats::Skip);
        };
        let Some(estimation) = JoinEstimate::from_inputs(
            &left_input,
            &right_input,
            self.left_cardinality,
            self.right_cardinality,
        )?
        else {
            return Ok(JoinConditionStats::NoOverlap);
        };

        Ok(JoinConditionStats::Estimated {
            new_stat: JoinKeyStatUpdate::mixed_type(
                estimation.min,
                estimation.max,
                self.left_col_stat,
                self.right_col_stat,
                estimation.ndv,
                estimation.histogram,
            ),
            card: estimation.card,
        })
    }
}

enum JoinConditionStats {
    Skip,
    NoOverlap,
    Estimated {
        new_stat: JoinKeyStatUpdate,
        card: f64,
    },
}

struct JoinColumnInput<'a> {
    min: Datum,
    max: Datum,
    ndv: NdvEstimate,
    histogram: Option<&'a Histogram>,
}

impl<'a> JoinColumnInput<'a> {
    fn from_column_stat(stat: &'a ColumnStat) -> Self {
        Self {
            min: stat.min.clone(),
            max: stat.max.clone(),
            ndv: stat.ndv,
            histogram: stat.histogram.as_ref(),
        }
    }

    fn normalize_to_kind(&self, kind: DatumKind) -> Option<Self> {
        let min = self.min.normalize_to_kind(kind)?;
        let max = self.max.normalize_to_kind(kind)?;
        if min.compare(&max).ok()? == std::cmp::Ordering::Greater {
            return None;
        }

        Some(Self {
            min,
            max,
            ndv: self.ndv,
            histogram: self.histogram,
        })
    }

    fn interval(&self) -> UniformSampleSet {
        UniformSampleSet::new(self.min.clone(), self.max.clone())
    }
}

struct JoinEstimate {
    min: Option<Datum>,
    max: Option<Datum>,
    card: f64,
    ndv: Option<NdvEstimate>,
    histogram: Option<Histogram>,
}

impl JoinEstimate {
    fn from_inputs(
        left: &JoinColumnInput,
        right: &JoinColumnInput,
        left_cardinality: f64,
        right_cardinality: f64,
    ) -> Result<Option<Self>> {
        let left_interval = left.interval();
        let right_interval = right.interval();
        if !left_interval.has_intersection(&right_interval)? {
            return Ok(None);
        }

        if left.min.is_numeric()
            && let (Some(left_hist), Some(right_hist)) = (left.histogram, right.histogram)
            && let Some(estimation) = left_hist.estimate_join_numeric_compatible(right_hist)?
        {
            let (min, max) = left_interval.intersection(&right_interval)?;
            return Ok(Some(Self {
                min,
                max,
                card: estimation.cardinality.expected,
                ndv: Some(estimation.ndv),
                histogram: estimation.histogram,
            }));
        }

        let ndv = left.ndv.min(right.ndv);
        let max_ndv = match (left.ndv.expected, right.ndv.expected) {
            (Some(left), Some(right)) => left.max(right),
            (Some(left), None) => left,
            (None, Some(right)) => right,
            (None, None) => {
                if left.ndv.upper == 0.0 && right.ndv.upper == 0.0 {
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
        let (min, max) = left_interval.intersection(&right_interval)?;
        Ok(Some(Self {
            min,
            max,
            card,
            ndv: Some(ndv),
            histogram: None,
        }))
    }
}

fn mixed_numeric_stat_kind(
    left_stat: &ColumnStat,
    right_stat: &ColumnStat,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<Option<DatumKind>> {
    let values = [
        &left_stat.min,
        &left_stat.max,
        &right_stat.min,
        &right_stat.max,
    ];
    if !values.iter().all(|value| value.is_numeric()) {
        return Ok(None);
    }

    let Some(conversion) = common_super_type_with_conversion(left_type, right_type) else {
        return Ok(None);
    };
    if !matches!(
        conversion.common_type.remove_nullable(),
        DataType::Number(_) | DataType::Decimal(_)
    ) {
        return Ok(None);
    }
    if matches!(
        (conversion.left, conversion.right),
        (
            ConversionClass::ValueDependent | ConversionClass::TryOnly,
            _
        ) | (
            _,
            ConversionClass::ValueDependent | ConversionClass::TryOnly
        )
    ) {
        return Ok(None);
    }

    if values.iter().any(|value| matches!(value, Datum::Float(_))) {
        return Ok(Some(DatumKind::Float));
    }
    if values.iter().all(|value| value.as_i64().is_some()) {
        return Ok(Some(DatumKind::Int));
    }
    if values.iter().all(|value| value.as_u64().is_some()) {
        return Ok(Some(DatumKind::UInt));
    }
    Ok(Some(DatumKind::Float))
}

#[derive(Debug, Clone)]
pub(crate) struct JoinKeyStatUpdate {
    left_min: Option<Datum>,
    left_max: Option<Datum>,
    right_min: Option<Datum>,
    right_max: Option<Datum>,
    ndv: Option<NdvEstimate>,
    histogram: Option<Histogram>,
}

impl JoinKeyStatUpdate {
    fn same_type(
        min: Option<Datum>,
        max: Option<Datum>,
        ndv: Option<NdvEstimate>,
        histogram: Option<Histogram>,
    ) -> Self {
        Self {
            left_min: min.clone(),
            left_max: max.clone(),
            right_min: min,
            right_max: max,
            ndv,
            histogram,
        }
    }

    fn mixed_type(
        min: Option<Datum>,
        max: Option<Datum>,
        left_stat: &ColumnStat,
        right_stat: &ColumnStat,
        ndv: Option<NdvEstimate>,
        histogram: Option<Histogram>,
    ) -> Self {
        let (left_min, left_max) =
            Self::normalize_bounds_to_stat_type(min.as_ref(), max.as_ref(), left_stat);
        let (right_min, right_max) =
            Self::normalize_bounds_to_stat_type(min.as_ref(), max.as_ref(), right_stat);
        Self {
            left_min,
            left_max,
            right_min,
            right_max,
            ndv,
            histogram,
        }
    }

    fn normalize_bounds_to_stat_type(
        min: Option<&Datum>,
        max: Option<&Datum>,
        stat: &ColumnStat,
    ) -> (Option<Datum>, Option<Datum>) {
        let Some(kind) = stat.min.kind() else {
            return (None, None);
        };
        let min = min.and_then(|value| value.lower_bound_to_kind(kind));
        let max = max.and_then(|value| value.upper_bound_to_kind(kind));
        if let (Some(min), Some(max)) = (&min, &max)
            && min
                .compare(max)
                .is_ok_and(|ordering| ordering == std::cmp::Ordering::Greater)
        {
            return (None, None);
        }
        (min, max)
    }

    fn apply(self, left_stat: &mut ColumnStat, right_stat: &mut ColumnStat) {
        if let Some(new_min) = self.left_min {
            left_stat.min = new_min;
        }
        if let Some(new_max) = self.left_max {
            left_stat.max = new_max;
        }
        if let Some(new_min) = self.right_min {
            right_stat.min = new_min;
        }
        if let Some(new_max) = self.right_max {
            right_stat.max = new_max;
        }
        if let Some(new_ndv) = self.ndv {
            left_stat.ndv = new_ndv;
            right_stat.ndv = new_ndv;
        }
        left_stat.histogram = self.histogram.clone();
        right_stat.histogram = self.histogram;
    }

    pub(crate) fn finish_join_histograms(
        statistics: &mut Statistics,
        joined_column: Symbol,
        keep_join_histogram: bool,
    ) -> Result<()> {
        for (idx, stat) in statistics.column_stats.iter_mut() {
            if !keep_join_histogram || *idx != joined_column {
                // Other columns' histograms are inaccurate after the join cardinality update.
                stat.histogram = None;
            }
        }
        Ok(())
    }

    fn drop_non_join_histograms(statistics: &mut Statistics, joined_column: Symbol) {
        for (idx, stat) in statistics.column_stats.iter_mut() {
            if *idx != joined_column {
                stat.histogram = None;
            }
        }
    }

    pub(crate) fn finish_semi_join_histogram(
        statistics: &mut Statistics,
        original_statistics: &Statistics,
        joined_column: Symbol,
        cardinality: f64,
    ) -> Result<()> {
        Self::drop_non_join_histograms(statistics, joined_column);

        let Some(stat) = statistics.column_stats.get_mut(&joined_column) else {
            return Ok(());
        };
        let Some(original_stat) = original_statistics.column_stats.get(&joined_column) else {
            return Ok(());
        };
        let Some(original_histogram) = &original_stat.histogram else {
            return Ok(());
        };

        let Some(mut histogram) = original_histogram.restrict_to_bounds(&stat.min, &stat.max)?
        else {
            stat.histogram = None;
            return Ok(());
        };
        let num_values = histogram.num_values();
        if num_values > cardinality && num_values > 0.0 {
            histogram.scale_counts(cardinality / num_values);
        }
        stat.refine_ndv_from_histogram(&histogram);
        stat.histogram = Some(histogram);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::types::NumberDataType;
    use databend_common_statistics::F64;
    use databend_common_statistics::TypedHistogram;
    use databend_common_statistics::TypedHistogramBucket;

    use super::*;

    #[test]
    fn test_mixed_type_stat_update_uses_original_stat_types() {
        let left_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(100),
            ndv: NdvEstimate::exact(100.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(10),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let stat = JoinKeyStatUpdate::mixed_type(
            Some(Datum::Int(1)),
            Some(Datum::Int(2)),
            &left_stat,
            &right_stat,
            Some(NdvEstimate::exact(2.0)),
            None,
        );

        assert_eq!(stat.left_min, Some(Datum::Int(1)));
        assert_eq!(stat.left_max, Some(Datum::Int(2)));
        assert_eq!(stat.right_min, Some(Datum::UInt(1)));
        assert_eq!(stat.right_max, Some(Datum::UInt(2)));
    }

    #[test]
    fn test_mixed_type_stat_update_rounds_float_bounds_for_integer_stats() {
        let int_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(100),
            ndv: NdvEstimate::exact(100.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let float_stat = ColumnStat {
            min: Datum::Float(F64::from(1.2)),
            max: Datum::Float(F64::from(8.8)),
            ndv: NdvEstimate::exact(8.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let stat = JoinKeyStatUpdate::mixed_type(
            Some(Datum::Float(F64::from(1.2))),
            Some(Datum::Float(F64::from(8.8))),
            &int_stat,
            &float_stat,
            Some(NdvEstimate::exact(8.0)),
            None,
        );

        assert_eq!(stat.left_min, Some(Datum::Int(2)));
        assert_eq!(stat.left_max, Some(Datum::Int(8)));
        assert_eq!(stat.right_min, Some(Datum::Float(F64::from(1.2))));
        assert_eq!(stat.right_max, Some(Datum::Float(F64::from(8.8))));
    }

    #[test]
    fn test_mixed_numeric_stats_normalize_to_smaller_common_stat_kind() -> Result<()> {
        let left_stat = ColumnStat {
            min: Datum::Int(0),
            max: Datum::Int(100),
            ndv: NdvEstimate::exact(100.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let right_stat = ColumnStat {
            min: Datum::UInt(0),
            max: Datum::UInt(10),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };

        let kind = mixed_numeric_stat_kind(
            &left_stat,
            &right_stat,
            &DataType::Number(NumberDataType::Int32),
            &DataType::Number(NumberDataType::UInt8),
        )?
        .expect("mixed numeric stats should be normalized");
        let left = JoinColumnInput::from_column_stat(&left_stat)
            .normalize_to_kind(kind)
            .expect("left stats should normalize");
        let right = JoinColumnInput::from_column_stat(&right_stat)
            .normalize_to_kind(kind)
            .expect("right stats should normalize");

        assert_eq!(left.min, Datum::Int(0));
        assert_eq!(left.max, Datum::Int(100));
        assert_eq!(right.min, Datum::Int(0));
        assert_eq!(right.max, Datum::Int(10));
        Ok(())
    }

    #[test]
    fn test_join_fallback_uses_known_ndv_when_other_side_is_upper_only() -> Result<()> {
        let left = JoinColumnInput {
            min: Datum::Int(1),
            max: Datum::Int(100),
            ndv: NdvEstimate::exact(10.0),
            histogram: None,
        };
        let right = JoinColumnInput {
            min: Datum::Int(1),
            max: Datum::Int(100),
            ndv: NdvEstimate::upper_bound(200.0),
            histogram: None,
        };

        let estimate =
            JoinEstimate::from_inputs(&left, &right, 100.0, 200.0)?.expect("join ranges overlap");

        assert_eq!(estimate.card, 2000.0);
        assert_eq!(estimate.ndv, Some(NdvEstimate::upper_bound(10.0)));
        Ok(())
    }

    #[test]
    fn test_join_fallback_does_not_use_upper_only_ndv_as_expected() -> Result<()> {
        let left = JoinColumnInput {
            min: Datum::Int(1),
            max: Datum::Int(100),
            ndv: NdvEstimate::upper_bound(100.0),
            histogram: None,
        };
        let right = JoinColumnInput {
            min: Datum::Int(1),
            max: Datum::Int(100),
            ndv: NdvEstimate::upper_bound(200.0),
            histogram: None,
        };

        let estimate =
            JoinEstimate::from_inputs(&left, &right, 100.0, 200.0)?.expect("join ranges overlap");

        assert_eq!(estimate.card, 1.0);
        assert_eq!(estimate.ndv, Some(NdvEstimate::upper_bound(100.0)));
        Ok(())
    }

    #[test]
    fn test_finish_join_histograms_keeps_join_key_histogram_and_drops_others() -> Result<()> {
        let mut statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([
                (Symbol::new(0), ColumnStat {
                    min: Datum::Int(5),
                    max: Datum::Int(10),
                    ndv: NdvEstimate::exact(5.0),
                    null_count: StatCount::exact(0),
                    histogram: Some(Histogram::Int(TypedHistogram {
                        accuracy: true,
                        row_scale: 1.0,
                        buckets: vec![TypedHistogramBucket::new(0, 10, 1000.0, 10.0)],
                        avg_spacing: None,
                    })),
                }),
                (Symbol::new(1), ColumnStat {
                    min: Datum::Int(0),
                    max: Datum::Int(10),
                    ndv: NdvEstimate::exact(10.0),
                    null_count: StatCount::exact(0),
                    histogram: Some(Histogram::Int(TypedHistogram {
                        accuracy: true,
                        row_scale: 1.0,
                        buckets: vec![TypedHistogramBucket::new(0, 10, 1000.0, 10.0)],
                        avg_spacing: None,
                    })),
                }),
            ]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };

        JoinKeyStatUpdate::finish_join_histograms(&mut statistics, Symbol::new(0), true)?;

        let join_histogram = statistics.column_stats[&Symbol::new(0)]
            .histogram
            .as_ref()
            .expect("join key histogram should be propagated");
        assert!((join_histogram.num_values() - 1000.0).abs() < 1e-9);
        assert!((join_histogram.ndv().expected.unwrap() - 10.0).abs() < 1e-9);
        assert!(statistics.column_stats[&Symbol::new(1)].histogram.is_none());
        Ok(())
    }

    #[test]
    fn test_finish_semi_join_histogram_drops_non_join_histograms() -> Result<()> {
        let original_statistics = Statistics {
            precise_cardinality: None,
            column_stats: HashMap::from([
                (Symbol::new(0), ColumnStat {
                    min: Datum::Int(1),
                    max: Datum::Int(10),
                    ndv: NdvEstimate::exact(10.0),
                    null_count: StatCount::exact(0),
                    histogram: Some(Histogram::Int(TypedHistogram {
                        accuracy: true,
                        row_scale: 1.0,
                        buckets: vec![TypedHistogramBucket::new(1, 10, 10.0, 10.0)],
                        avg_spacing: None,
                    })),
                }),
                (Symbol::new(1), ColumnStat {
                    min: Datum::Int(1),
                    max: Datum::Int(10),
                    ndv: NdvEstimate::exact(10.0),
                    null_count: StatCount::exact(0),
                    histogram: Some(Histogram::Int(TypedHistogram {
                        accuracy: true,
                        row_scale: 1.0,
                        buckets: vec![TypedHistogramBucket::new(1, 10, 10.0, 10.0)],
                        avg_spacing: None,
                    })),
                }),
            ]),
            top_n: Default::default(),
            count_min_sketch: Default::default(),
        };
        let mut statistics = original_statistics.clone();
        let join_stat = statistics.column_stats.get_mut(&Symbol::new(0)).unwrap();
        join_stat.min = Datum::Int(1);
        join_stat.max = Datum::Int(5);
        join_stat.ndv = NdvEstimate::exact(5.0);

        JoinKeyStatUpdate::finish_semi_join_histogram(
            &mut statistics,
            &original_statistics,
            Symbol::new(0),
            5.0,
        )?;

        assert!(statistics.column_stats[&Symbol::new(0)].histogram.is_some());
        assert!(statistics.column_stats[&Symbol::new(1)].histogram.is_none());
        Ok(())
    }
}
