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

use databend_common_expression::Domain;
use databend_common_expression::stat_distribution::ArgStat;
use databend_common_expression::stat_distribution::BorrowedDistribution;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_statistics::BorrowedHistogram;
use databend_common_statistics::Datum;
use databend_common_statistics::F64;
use databend_common_statistics::Histogram;
use databend_common_statistics::StatBounds;
use databend_common_statistics::TypedHistogram;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnTopN;

use crate::Symbol;

pub type ColumnStatSet = HashMap<Symbol, ColumnStat>;
pub type TopNSet = HashMap<Symbol, ColumnTopN>;
pub type CountMinSketchSet = HashMap<Symbol, ColumnCountMinSketch>;

#[derive(Debug, Clone, PartialEq)]
/// Statistics information of a column
pub enum ColumnStat {
    Boolean {
        min: bool,
        max: bool,
        ndv: NdvEstimate,
        null_count: StatCount,
    },
    Int {
        min: i64,
        max: i64,
        ndv: NdvEstimate,
        null_count: StatCount,
        histogram: Option<TypedHistogram<i64>>,
    },
    UInt {
        min: u64,
        max: u64,
        ndv: NdvEstimate,
        null_count: StatCount,
        histogram: Option<TypedHistogram<u64>>,
    },
    Float {
        min: F64,
        max: F64,
        ndv: NdvEstimate,
        null_count: StatCount,
        histogram: Option<TypedHistogram<F64>>,
    },
    Bytes {
        min: Vec<u8>,
        max: Vec<u8>,
        ndv: NdvEstimate,
        null_count: StatCount,
        histogram: Option<TypedHistogram<Vec<u8>>>,
    },
    /// A column proven to contain no non-NULL values.
    AllNull { null_count: StatCount },
}

impl ColumnStat {
    pub fn new(
        bounds: StatBounds,
        ndv: NdvEstimate,
        null_count: StatCount,
        histogram: Option<Histogram>,
    ) -> Result<Self, String> {
        let finite_ndv_upper = bounds.finite_ndv_upper();
        let mut stat = match (bounds, histogram) {
            (StatBounds::Bool { min, max }, None) => Ok(Self::Boolean {
                min,
                max,
                ndv,
                null_count,
            }),
            (StatBounds::Int { min, max }, histogram) => Ok(Self::Int {
                min,
                max,
                ndv,
                null_count,
                histogram: match histogram {
                    Some(Histogram::Int(histogram)) => Some(histogram),
                    None => None,
                    Some(histogram) => {
                        return Err(format!("Int column statistic cannot carry {histogram:?}"));
                    }
                },
            }),
            (StatBounds::UInt { min, max }, histogram) => Ok(Self::UInt {
                min,
                max,
                ndv,
                null_count,
                histogram: match histogram {
                    Some(Histogram::UInt(histogram)) => Some(histogram),
                    None => None,
                    Some(histogram) => {
                        return Err(format!("UInt column statistic cannot carry {histogram:?}"));
                    }
                },
            }),
            (StatBounds::Float { min, max }, histogram) => Ok(Self::Float {
                min,
                max,
                ndv,
                null_count,
                histogram: match histogram {
                    Some(Histogram::Float(histogram)) => Some(histogram),
                    None => None,
                    Some(histogram) => {
                        return Err(format!("Float column statistic cannot carry {histogram:?}"));
                    }
                },
            }),
            (StatBounds::Bytes { min, max }, histogram) => Ok(Self::Bytes {
                min,
                max,
                ndv,
                null_count,
                histogram: match histogram {
                    Some(Histogram::Bytes(histogram)) => Some(histogram),
                    None => None,
                    Some(histogram) => {
                        return Err(format!("Bytes column statistic cannot carry {histogram:?}"));
                    }
                },
            }),
            (bounds, histogram) => Err(format!(
                "column statistic bounds do not match histogram: bounds={bounds:?}, histogram={histogram:?}"
            )),
        }?;
        if let Some(upper) = finite_ndv_upper {
            stat.set_ndv(stat.ndv().reduce(upper));
        }
        Ok(stat)
    }

    fn refine_ndv_estimate(
        ndv: &mut NdvEstimate,
        histogram_ndv: NdvEstimate,
        histogram_is_accurate: bool,
    ) {
        if histogram_is_accurate {
            *ndv = ndv.min(histogram_ndv);
            return;
        }

        let upper = ndv.upper.min(histogram_ndv.upper);
        *ndv = match histogram_ndv.expected {
            Some(expected) => NdvEstimate::new(expected.min(upper), upper),
            None => NdvEstimate::upper_bound(upper),
        };
    }

    pub fn to_arg_stat(&self, data_type: &DataType) -> Result<ArgStat<'_>, String> {
        if let ColumnStat::AllNull { null_count } = self {
            if !matches!(data_type, DataType::Null | DataType::Nullable(_)) {
                return Err(format!(
                    "all-NULL statistics require a nullable data type, got {data_type:?}"
                ));
            }
            return Ok(ArgStat {
                domain: Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
                ndv: NdvEstimate::exact(0.0),
                null_count: *null_count,
                distribution: BorrowedDistribution::Unknown,
            });
        }

        let null_count = self.null_count();
        let distribution = self
            .histogram()
            .map(BorrowedDistribution::Histogram)
            .unwrap_or(BorrowedDistribution::Unknown);
        let bounds = self
            .bounds()
            .ok_or_else(|| "all-NULL statistics do not have value bounds".to_string())?;

        Ok(ArgStat {
            domain: Domain::from_bounds(data_type, bounds, null_count.upper() > 0.0)?,
            ndv: self.ndv(),
            null_count,
            distribution,
        })
    }

    pub fn from_const(datum: Datum) -> Self {
        let ndv = NdvEstimate::exact(1.0);
        let null_count = StatCount::exact(0);
        match datum {
            Datum::Bool(value) => Self::Boolean {
                min: value,
                max: value,
                ndv,
                null_count,
            },
            Datum::Int(value) => Self::Int {
                min: value,
                max: value,
                ndv,
                null_count,
                histogram: None,
            },
            Datum::UInt(value) => Self::UInt {
                min: value,
                max: value,
                ndv,
                null_count,
                histogram: None,
            },
            Datum::Float(value) => Self::Float {
                min: value,
                max: value,
                ndv,
                null_count,
                histogram: None,
            },
            Datum::Bytes(value) => Self::Bytes {
                min: value.clone(),
                max: value,
                ndv,
                null_count,
                histogram: None,
            },
        }
    }

    /// Returns the closed interval containing the column's non-NULL values.
    /// An all-NULL column has no value bounds.
    pub fn bounds(&self) -> Option<StatBounds> {
        match self {
            ColumnStat::Boolean { min, max, .. } => Some(StatBounds::Bool {
                min: *min,
                max: *max,
            }),
            ColumnStat::Int { min, max, .. } => Some(StatBounds::Int {
                min: *min,
                max: *max,
            }),
            ColumnStat::UInt { min, max, .. } => Some(StatBounds::UInt {
                min: *min,
                max: *max,
            }),
            ColumnStat::Float { min, max, .. } => Some(StatBounds::Float {
                min: *min,
                max: *max,
            }),
            ColumnStat::Bytes { min, max, .. } => Some(StatBounds::Bytes {
                min: min.clone(),
                max: max.clone(),
            }),
            ColumnStat::AllNull { .. } => None,
        }
    }

    pub(crate) fn restrict_to_bounds(&mut self, bounds: StatBounds) -> Result<(), String> {
        let finite_ndv_upper = bounds.finite_ndv_upper();
        match (&mut *self, bounds) {
            (
                ColumnStat::Boolean { min, max, .. },
                StatBounds::Bool {
                    min: new_min,
                    max: new_max,
                },
            ) => {
                *min = new_min;
                *max = new_max;
            }
            (
                ColumnStat::Int {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                StatBounds::Int {
                    min: new_min,
                    max: new_max,
                },
            ) => {
                *histogram = histogram
                    .as_ref()
                    .and_then(|histogram| histogram.restrict_discrete_buckets(new_min, new_max));
                if let Some(histogram) = histogram {
                    Self::refine_ndv_estimate(ndv, histogram.ndv(), histogram.accuracy);
                }
                *min = new_min;
                *max = new_max;
            }
            (
                ColumnStat::UInt {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                StatBounds::UInt {
                    min: new_min,
                    max: new_max,
                },
            ) => {
                *histogram = histogram
                    .as_ref()
                    .and_then(|histogram| histogram.restrict_discrete_buckets(new_min, new_max));
                if let Some(histogram) = histogram {
                    Self::refine_ndv_estimate(ndv, histogram.ndv(), histogram.accuracy);
                }
                *min = new_min;
                *max = new_max;
            }
            (
                ColumnStat::Float {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                StatBounds::Float {
                    min: new_min,
                    max: new_max,
                },
            ) => {
                *histogram = histogram
                    .as_ref()
                    .and_then(|histogram| histogram.restrict_float_buckets(new_min, new_max));
                if let Some(histogram) = histogram {
                    Self::refine_ndv_estimate(ndv, histogram.ndv(), histogram.accuracy);
                }
                *min = new_min;
                *max = new_max;
            }
            (
                ColumnStat::Bytes {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                StatBounds::Bytes {
                    min: new_min,
                    max: new_max,
                },
            ) => {
                *histogram = histogram
                    .as_ref()
                    .and_then(|histogram| histogram.restrict_bytes_buckets(&new_min, &new_max));
                if let Some(histogram) = histogram {
                    Self::refine_ndv_estimate(ndv, histogram.ndv(), histogram.accuracy);
                }
                *min = new_min;
                *max = new_max;
            }
            (column_stat, bounds) => {
                return Err(format!(
                    "column statistic bounds do not match its data type: stat={column_stat:?}, bounds={bounds:?}"
                ));
            }
        }
        if let Some(upper) = finite_ndv_upper {
            self.set_ndv(self.ndv().reduce(upper));
        }
        Ok(())
    }

    fn replace_typed_histogram_from<T>(
        target: &mut Option<TypedHistogram<T>>,
        source: &Option<TypedHistogram<T>>,
        ndv: &mut NdvEstimate,
        max_num_values: f64,
        restrict: impl FnOnce(&TypedHistogram<T>) -> Option<TypedHistogram<T>>,
    ) {
        *target = source.as_ref().and_then(restrict);
        let Some(histogram) = target else {
            return;
        };
        let num_values = histogram.num_values();
        if num_values > max_num_values && num_values > 0.0 {
            histogram.scale_counts(max_num_values / num_values);
        }
        Self::refine_ndv_estimate(ndv, histogram.ndv(), histogram.accuracy);
    }

    fn scale_typed_histogram_to<T>(
        histogram: &mut Option<TypedHistogram<T>>,
        ndv: &mut NdvEstimate,
        num_values: f64,
    ) -> bool {
        let Some(histogram) = histogram else {
            return false;
        };
        let current_num_values = histogram.num_values();
        if current_num_values <= 0.0 {
            return false;
        }
        histogram.scale_counts(num_values / current_num_values);
        Self::refine_ndv_estimate(ndv, histogram.ndv(), histogram.accuracy);
        true
    }

    pub(crate) fn scale_histogram_to(&mut self, num_values: f64) -> bool {
        match self {
            ColumnStat::Int { ndv, histogram, .. } => {
                Self::scale_typed_histogram_to(histogram, ndv, num_values)
            }
            ColumnStat::UInt { ndv, histogram, .. } => {
                Self::scale_typed_histogram_to(histogram, ndv, num_values)
            }
            ColumnStat::Float { ndv, histogram, .. } => {
                Self::scale_typed_histogram_to(histogram, ndv, num_values)
            }
            ColumnStat::Bytes { ndv, histogram, .. } => {
                Self::scale_typed_histogram_to(histogram, ndv, num_values)
            }
            ColumnStat::Boolean { .. } | ColumnStat::AllNull { .. } => false,
        }
    }

    pub(crate) fn replace_histogram_from(
        &mut self,
        source: &ColumnStat,
        max_num_values: f64,
    ) -> Result<(), String> {
        match (self, source) {
            (ColumnStat::Boolean { .. }, ColumnStat::Boolean { .. })
            | (ColumnStat::AllNull { .. }, ColumnStat::AllNull { .. }) => Ok(()),
            (
                ColumnStat::Int {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                ColumnStat::Int {
                    histogram: source, ..
                },
            ) => {
                Self::replace_typed_histogram_from(
                    histogram,
                    source,
                    ndv,
                    max_num_values,
                    |histogram| histogram.restrict_discrete_buckets(*min, *max),
                );
                Ok(())
            }
            (
                ColumnStat::UInt {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                ColumnStat::UInt {
                    histogram: source, ..
                },
            ) => {
                Self::replace_typed_histogram_from(
                    histogram,
                    source,
                    ndv,
                    max_num_values,
                    |histogram| histogram.restrict_discrete_buckets(*min, *max),
                );
                Ok(())
            }
            (
                ColumnStat::Float {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                ColumnStat::Float {
                    histogram: source, ..
                },
            ) => {
                Self::replace_typed_histogram_from(
                    histogram,
                    source,
                    ndv,
                    max_num_values,
                    |histogram| histogram.restrict_float_buckets(*min, *max),
                );
                Ok(())
            }
            (
                ColumnStat::Bytes {
                    min,
                    max,
                    ndv,
                    histogram,
                    ..
                },
                ColumnStat::Bytes {
                    histogram: source, ..
                },
            ) => {
                Self::replace_typed_histogram_from(
                    histogram,
                    source,
                    ndv,
                    max_num_values,
                    |histogram| histogram.restrict_bytes_buckets(min, max),
                );
                Ok(())
            }
            (stat, source) => Err(format!(
                "cannot copy histogram between different column statistic types: stat={stat:?}, source={source:?}"
            )),
        }
    }

    pub fn null_count(&self) -> StatCount {
        match self {
            ColumnStat::Boolean { null_count, .. }
            | ColumnStat::Int { null_count, .. }
            | ColumnStat::UInt { null_count, .. }
            | ColumnStat::Float { null_count, .. }
            | ColumnStat::Bytes { null_count, .. }
            | ColumnStat::AllNull { null_count } => *null_count,
        }
    }

    pub(crate) fn set_null_count(&mut self, value: StatCount) {
        match self {
            ColumnStat::Boolean { null_count, .. }
            | ColumnStat::Int { null_count, .. }
            | ColumnStat::UInt { null_count, .. }
            | ColumnStat::Float { null_count, .. }
            | ColumnStat::Bytes { null_count, .. }
            | ColumnStat::AllNull { null_count } => *null_count = value,
        }
    }

    /// Replicate count-valued statistics without changing the value domain or NDV.
    pub(crate) fn scale_row_mass(&mut self, scale: StatCardinality) {
        let scale_value = scale.value();
        if scale_value == 1.0 {
            return;
        }

        let null_count = if scale_value == 0.0 {
            StatCount::exact(0)
        } else {
            match (self.null_count(), scale) {
                (StatCount::Exact(count), StatCardinality::Exact(scale)) => count
                    .checked_mul(scale)
                    .map(StatCount::exact)
                    .unwrap_or_else(|| {
                        StatCount::estimate(count as f64 * scale as f64, f64::INFINITY)
                    }),
                (count, scale) => StatCount::estimate(
                    count.expected() * scale.value(),
                    count.upper() * scale.value(),
                ),
            }
        };
        self.set_null_count(null_count);

        match self {
            ColumnStat::Int {
                histogram: Some(histogram),
                ..
            } => histogram.scale_counts(scale_value),
            ColumnStat::UInt {
                histogram: Some(histogram),
                ..
            } => histogram.scale_counts(scale_value),
            ColumnStat::Float {
                histogram: Some(histogram),
                ..
            } => histogram.scale_counts(scale_value),
            ColumnStat::Bytes {
                histogram: Some(histogram),
                ..
            } => histogram.scale_counts(scale_value),
            ColumnStat::Boolean { .. }
            | ColumnStat::AllNull { .. }
            | ColumnStat::Int {
                histogram: None, ..
            }
            | ColumnStat::UInt {
                histogram: None, ..
            }
            | ColumnStat::Float {
                histogram: None, ..
            }
            | ColumnStat::Bytes {
                histogram: None, ..
            } => {}
        }
    }

    pub fn ndv(&self) -> NdvEstimate {
        match self {
            ColumnStat::Boolean { ndv, .. }
            | ColumnStat::Int { ndv, .. }
            | ColumnStat::UInt { ndv, .. }
            | ColumnStat::Float { ndv, .. }
            | ColumnStat::Bytes { ndv, .. } => *ndv,
            ColumnStat::AllNull { .. } => NdvEstimate::exact(0.0),
        }
    }

    pub(crate) fn set_ndv(&mut self, value: NdvEstimate) {
        match self {
            ColumnStat::Boolean { ndv, .. }
            | ColumnStat::Int { ndv, .. }
            | ColumnStat::UInt { ndv, .. }
            | ColumnStat::Float { ndv, .. }
            | ColumnStat::Bytes { ndv, .. } => *ndv = value,
            ColumnStat::AllNull { .. } => (),
        }
    }

    pub(crate) fn clear_histogram(&mut self) {
        match self {
            ColumnStat::Int { histogram, .. } => *histogram = None,
            ColumnStat::UInt { histogram, .. } => *histogram = None,
            ColumnStat::Float { histogram, .. } => *histogram = None,
            ColumnStat::Bytes { histogram, .. } => *histogram = None,
            ColumnStat::Boolean { .. } | ColumnStat::AllNull { .. } => {}
        }
    }

    pub(crate) fn histogram(&self) -> Option<BorrowedHistogram<'_>> {
        match self {
            ColumnStat::Int { histogram, .. } => histogram.as_ref().map(BorrowedHistogram::Int),
            ColumnStat::UInt { histogram, .. } => histogram.as_ref().map(BorrowedHistogram::UInt),
            ColumnStat::Float { histogram, .. } => histogram.as_ref().map(BorrowedHistogram::Float),
            ColumnStat::Bytes { histogram, .. } => histogram.as_ref().map(BorrowedHistogram::Bytes),
            ColumnStat::Boolean { .. } | ColumnStat::AllNull { .. } => None,
        }
    }

    pub(crate) fn set_histogram(&mut self, histogram: Option<Histogram>) -> Result<(), String> {
        match (self, histogram) {
            (stat, None) => {
                stat.clear_histogram();
                Ok(())
            }
            (ColumnStat::Int { histogram, .. }, Some(Histogram::Int(new_histogram))) => {
                *histogram = Some(new_histogram);
                Ok(())
            }
            (ColumnStat::UInt { histogram, .. }, Some(Histogram::UInt(new_histogram))) => {
                *histogram = Some(new_histogram);
                Ok(())
            }
            (ColumnStat::Float { histogram, .. }, Some(Histogram::Float(new_histogram))) => {
                *histogram = Some(new_histogram);
                Ok(())
            }
            (ColumnStat::Bytes { histogram, .. }, Some(Histogram::Bytes(new_histogram))) => {
                *histogram = Some(new_histogram);
                Ok(())
            }
            (stat, Some(histogram)) => Err(format!(
                "column statistic histogram does not match its data type: stat={stat:?}, histogram={histogram:?}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_statistics::F64;
    use databend_common_statistics::TypedHistogram;
    use databend_common_statistics::TypedHistogramBucket;

    use super::*;

    #[test]
    fn test_column_stat_selects_variant_from_bounds() {
        let histogram = TypedHistogram::new(
            vec![TypedHistogramBucket::new(1_u64, 3_u64, 3.0, 3.0)],
            true,
        );
        let stat = ColumnStat::new(
            StatBounds::UInt { min: 1, max: 3 },
            NdvEstimate::exact(30.0),
            StatCount::exact(0),
            Some(Histogram::UInt(histogram.clone())),
        )
        .unwrap();

        assert_eq!(stat, ColumnStat::UInt {
            min: 1,
            max: 3,
            ndv: NdvEstimate::exact(3.0),
            null_count: StatCount::exact(0),
            histogram: Some(histogram),
        });
    }

    #[test]
    fn test_all_null_column_stat_has_no_value_bounds() {
        let mut stat = ColumnStat::AllNull {
            null_count: StatCount::exact(3),
        };

        assert_eq!(stat.bounds(), None);
        assert!(
            stat.restrict_to_bounds(StatBounds::UInt { min: 1, max: 3 })
                .is_err()
        );
        assert_eq!(stat, ColumnStat::AllNull {
            null_count: StatCount::exact(3),
        });
    }

    #[test]
    fn test_column_stat_rejects_mismatched_histogram_type() {
        let histogram = TypedHistogram::new(
            vec![TypedHistogramBucket::new(
                F64::from(1.0),
                F64::from(3.0),
                3.0,
                3.0,
            )],
            true,
        );

        let stat = ColumnStat::new(
            StatBounds::UInt { min: 1, max: 3 },
            NdvEstimate::exact(3.0),
            StatCount::exact(0),
            Some(Histogram::Float(histogram.clone())),
        )
        .unwrap_err();

        assert!(stat.contains("UInt column statistic cannot carry"));
    }

    #[test]
    fn test_restrict_to_bounds_updates_typed_histogram_and_ndv() {
        let mut stat = ColumnStat::UInt {
            min: 0,
            max: 9,
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(2),
            histogram: Some(TypedHistogram::new(
                vec![
                    TypedHistogramBucket::new(0, 4, 5.0, 5.0),
                    TypedHistogramBucket::new(5, 9, 5.0, 5.0),
                ],
                true,
            )),
        };

        stat.restrict_to_bounds(StatBounds::UInt { min: 2, max: 6 })
            .unwrap();

        assert_eq!(stat, ColumnStat::UInt {
            min: 2,
            max: 6,
            ndv: NdvEstimate::exact(5.0),
            null_count: StatCount::exact(2),
            histogram: Some(TypedHistogram::new(
                vec![
                    TypedHistogramBucket::new(2, 4, 3.0, 3.0),
                    TypedHistogramBucket::new(5, 6, 2.0, 2.0),
                ],
                true,
            )),
        });
    }

    #[test]
    fn test_restrict_to_bounds_rejects_mismatched_type_without_mutation() {
        let mut stat = ColumnStat::Int {
            min: 0,
            max: 10,
            ndv: NdvEstimate::exact(11.0),
            null_count: StatCount::exact(0),
            histogram: None,
        };
        let original = stat.clone();

        let err = stat
            .restrict_to_bounds(StatBounds::UInt { min: 0, max: 10 })
            .unwrap_err();

        assert!(err.starts_with("column statistic bounds do not match its data type"));
        assert_eq!(stat, original);
    }
}
