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

use std::fmt;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result as ExceptionResult;

use crate::Datum;
use crate::F64;
use crate::JoinEstimation;
use crate::NdvEstimate;
use crate::NumericValue;
use crate::StatBounds;
use crate::TypedHistogram;
use crate::TypedHistogramBucket;

pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 100;

/// A column histogram used by optimizer statistics.
///
/// A histogram is always a probabilistic model and provides no strong
/// consistency guarantees. In particular, neither bucket bounds nor counts
/// prove value existence, complete value-set coverage, exact NDV, or confirmed
/// join matches.
///
/// `accuracy == true` only means the bucket statistics are expected to be
/// relatively more accurate because they still come directly from
/// `ANALYZE TABLE`. ANALYZE buckets are observed row-order tiles: for each
/// supported non-null column, ANALYZE runs a query equivalent to sorting rows
/// by the column, assigning `NTILE(DEFAULT_HISTOGRAM_BUCKETS)`, then grouping by
/// tile and collecting `MIN(col)`, `MAX(col)`, `COUNT()`, and
/// `COUNT(DISTINCT col)`. Each bucket is the modeled closed value envelope of
/// one row-order tile. The bucket list is not a value-domain partition:
/// adjacent buckets may share boundaries or overlap when duplicate values
/// cross tile boundaries.
///
/// Histograms synthesized from column NDV plus min/max bounds by
/// [`crate::HistogramBuilder::from_ndv`] use `accuracy == false`. Scaling a
/// histogram by an independent selectivity also marks it inaccurate: scaling
/// only aligns row mass after a filter whose surviving values are unknown, so
/// the original bucket distinct counts are expected to be less accurate. Range
/// clipping and join overlap estimation keep this relative-quality flag because
/// they do not by themselves perform that unknown-value alignment. Numeric
/// synthetic histograms keep `avg_spacing` separately so consumers can detect
/// distorted ranges.
///
/// Consumers may use this distinction to choose a confidence or refinement
/// policy, but must not interpret it as proof. The type variants preserve the
/// bucket value type for serialization, function selectivity, and type-specific
/// join estimation.
#[derive(Debug, Clone, PartialEq)]
pub enum Histogram {
    Int(TypedHistogram<i64>),
    UInt(TypedHistogram<u64>),
    Float(TypedHistogram<F64>),
    Bytes(TypedHistogram<Vec<u8>>),
}

#[derive(Debug, Clone, Copy)]
pub enum BorrowedHistogram<'a> {
    Int(&'a TypedHistogram<i64>),
    UInt(&'a TypedHistogram<u64>),
    Float(&'a TypedHistogram<F64>),
    Bytes(&'a TypedHistogram<Vec<u8>>),
}

impl<'a> BorrowedHistogram<'a> {
    pub fn to_owned(self) -> Histogram {
        match self {
            Self::Int(histogram) => Histogram::Int(histogram.clone()),
            Self::UInt(histogram) => Histogram::UInt(histogram.clone()),
            Self::Float(histogram) => Histogram::Float(histogram.clone()),
            Self::Bytes(histogram) => Histogram::Bytes(histogram.clone()),
        }
    }

    pub fn num_values(self) -> f64 {
        match self {
            Self::Int(histogram) => histogram.num_values(),
            Self::UInt(histogram) => histogram.num_values(),
            Self::Float(histogram) => histogram.num_values(),
            Self::Bytes(histogram) => histogram.num_values(),
        }
    }

    pub fn ndv(self) -> NdvEstimate {
        match self {
            Self::Int(histogram) => histogram.ndv(),
            Self::UInt(histogram) => histogram.ndv(),
            Self::Float(histogram) => histogram.ndv(),
            Self::Bytes(histogram) => histogram.ndv(),
        }
    }

    pub fn accuracy(self) -> bool {
        match self {
            Self::Int(histogram) => histogram.accuracy,
            Self::UInt(histogram) => histogram.accuracy,
            Self::Float(histogram) => histogram.accuracy,
            Self::Bytes(histogram) => histogram.accuracy,
        }
    }

    pub fn is_range_distorted(self) -> bool {
        match self {
            Self::Int(histogram) => histogram.avg_spacing,
            Self::UInt(histogram) => histogram.avg_spacing,
            Self::Float(histogram) => histogram.avg_spacing,
            Self::Bytes(histogram) => histogram.avg_spacing,
        }
        .is_some_and(|bucket_width| bucket_width > 1e12)
    }

    /// Estimate a numeric join in the comparison expression's return type.
    ///
    /// Mixed bucket variants are evaluated directly. The input histograms are
    /// not normalized or cloned before estimation.
    pub fn estimate_join_numeric(
        self,
        other: BorrowedHistogram<'a>,
        return_type: NumericHistogramType,
    ) -> ExceptionResult<Option<JoinEstimation>> {
        match (return_type, self, other) {
            (NumericHistogramType::Int, Self::Int(left), Self::Int(right)) => {
                Ok(Some(left.estimate_join(right)))
            }
            (NumericHistogramType::UInt, Self::UInt(left), Self::UInt(right)) => {
                Ok(Some(left.estimate_join(right)))
            }
            (NumericHistogramType::Float, Self::Float(left), Self::Float(right)) => {
                Ok(Some(left.estimate_join(right)))
            }
            (_, Self::Bytes(_), _) | (_, _, Self::Bytes(_)) => Ok(None),
            (_, Self::Int(left), Self::Int(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::Int(left), Self::UInt(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::Int(left), Self::Float(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::UInt(left), Self::Int(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::UInt(left), Self::UInt(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::UInt(left), Self::Float(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::Float(left), Self::Int(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::Float(left), Self::UInt(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
            (_, Self::Float(left), Self::Float(right)) => {
                Ok(Some(left.estimate_join_as(right, return_type)?))
            }
        }
    }

    /// Infer the computation type from histogram variants when the caller has no expression type.
    pub fn estimate_join_numeric_compatible(
        self,
        other: BorrowedHistogram<'a>,
    ) -> ExceptionResult<Option<JoinEstimation>> {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Ok(Some(left.estimate_join(right))),
            (Self::UInt(left), Self::UInt(right)) => Ok(Some(left.estimate_join(right))),
            (Self::Float(left), Self::Float(right)) => Ok(Some(left.estimate_join(right))),
            (Self::Bytes(left), Self::Bytes(right)) => Ok(Some(left.estimate_join(right))),
            (Self::Bytes(_), _) | (_, Self::Bytes(_)) => Ok(None),
            (left, right) => left.estimate_join_numeric(right, NumericHistogramType::Float),
        }
    }
}

impl<'a> From<&'a Histogram> for BorrowedHistogram<'a> {
    fn from(histogram: &'a Histogram) -> Self {
        match histogram {
            Histogram::Int(histogram) => Self::Int(histogram),
            Histogram::UInt(histogram) => Self::UInt(histogram),
            Histogram::Float(histogram) => Self::Float(histogram),
            Histogram::Bytes(histogram) => Self::Bytes(histogram),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericHistogramType {
    Int,
    UInt,
    Float,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NumericRange {
    Integer { min: i128, max: i128 },
    Float { min: F64, max: F64 },
}

impl NumericHistogramType {
    pub(crate) fn project_range<T: NumericValue>(self, min: &T, max: &T) -> NumericRange {
        match self {
            Self::Int | Self::UInt => NumericRange::Integer {
                min: min.as_wide_integer(),
                max: max.as_wide_integer(),
            },
            Self::Float => NumericRange::Float {
                min: F64::from(min.as_f64()),
                max: F64::from(max.as_f64()),
            },
        }
    }
}

impl NumericRange {
    pub(crate) fn intersection(self, other: Self) -> ExceptionResult<Option<Self>> {
        match (self, other) {
            (
                Self::Integer {
                    min: left_min,
                    max: left_max,
                },
                Self::Integer {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let min = left_min.max(right_min);
                let max = left_max.min(right_max);
                Ok((min <= max).then_some(Self::Integer { min, max }))
            }
            (
                Self::Float {
                    min: left_min,
                    max: left_max,
                },
                Self::Float {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let min = left_min.max(right_min);
                let max = left_max.min(right_max);
                Ok((min <= max).then_some(Self::Float { min, max }))
            }
            _ => Err(ErrorCode::Internal(
                "numeric ranges must use the same comparison type",
            )),
        }
    }

    pub(crate) fn is_singleton(self) -> bool {
        match self {
            Self::Integer { min, max } => min == max,
            Self::Float { min, max } => min == max,
        }
    }

    pub(crate) fn width(self) -> Option<f64> {
        match self {
            Self::Integer { min, max } => max.checked_sub(min).map(|width| width as f64),
            Self::Float { min, max } => Some(max.into_inner() - min.into_inner()),
        }
    }

    pub(crate) fn restrict_stat_bounds(
        self,
        original: &StatBounds,
    ) -> ExceptionResult<Option<StatBounds>> {
        match (self, original) {
            (
                Self::Integer { min, max },
                StatBounds::Int {
                    min: original_min,
                    max: original_max,
                },
            ) => {
                let min = min.max(*original_min as i128);
                let max = max.min(*original_max as i128);
                if min > max {
                    return Ok(None);
                }
                Ok(Some(StatBounds::Int {
                    min: min as i64,
                    max: max as i64,
                }))
            }
            (
                Self::Integer { min, max },
                StatBounds::UInt {
                    min: original_min,
                    max: original_max,
                },
            ) => {
                let min = min.max(*original_min as i128);
                let max = max.min(*original_max as i128);
                if min > max {
                    return Ok(None);
                }
                Ok(Some(StatBounds::UInt {
                    min: min as u64,
                    max: max as u64,
                }))
            }
            (
                Self::Float { min, max },
                StatBounds::Int {
                    min: original_min,
                    max: original_max,
                },
            ) => Ok(
                integer_float_preimage(*original_min as i128, *original_max as i128, min, max).map(
                    |(min, max)| StatBounds::Int {
                        min: min as i64,
                        max: max as i64,
                    },
                ),
            ),
            (
                Self::Float { min, max },
                StatBounds::UInt {
                    min: original_min,
                    max: original_max,
                },
            ) => Ok(
                integer_float_preimage(*original_min as i128, *original_max as i128, min, max).map(
                    |(min, max)| StatBounds::UInt {
                        min: min as u64,
                        max: max as u64,
                    },
                ),
            ),
            (
                Self::Float { min, max },
                StatBounds::Float {
                    min: original_min,
                    max: original_max,
                },
            ) => {
                let min = min.max(*original_min);
                let max = max.min(*original_max);
                Ok((min <= max).then_some(StatBounds::Float { min, max }))
            }
            _ => Err(ErrorCode::Internal(format!(
                "numeric comparison range cannot restrict statistics bounds: range={self:?}, bounds={original:?}"
            ))),
        }
    }
}

fn integer_float_preimage(
    original_min: i128,
    original_max: i128,
    comparison_min: F64,
    comparison_max: F64,
) -> Option<(i128, i128)> {
    let comparison_min = comparison_min.into_inner();
    let comparison_max = comparison_max.into_inner();
    let min = first_integer_satisfying(original_min, original_max, |value| {
        value as f64 >= comparison_min
    })?;
    let max = last_integer_satisfying(min, original_max, |value| value as f64 <= comparison_max)?;
    Some((min, max))
}

fn first_integer_satisfying(
    mut min: i128,
    mut max: i128,
    predicate: impl Fn(i128) -> bool,
) -> Option<i128> {
    if !predicate(max) {
        return None;
    }
    while min < max {
        let middle = min + (max - min) / 2;
        if predicate(middle) {
            max = middle;
        } else {
            min = middle + 1;
        }
    }
    Some(min)
}

fn last_integer_satisfying(
    mut min: i128,
    mut max: i128,
    predicate: impl Fn(i128) -> bool,
) -> Option<i128> {
    if !predicate(min) {
        return None;
    }
    while min < max {
        let middle = min + (max - min + 1) / 2;
        if predicate(middle) {
            min = middle;
        } else {
            max = middle - 1;
        }
    }
    Some(min)
}

impl Histogram {
    pub fn try_from_buckets(
        accuracy: bool,
        buckets: Vec<HistogramBucket>,
        avg_spacing: Option<f64>,
    ) -> Result<Self, &'static str> {
        let Some(first_bucket) = buckets.first() else {
            return Err("histogram must contain at least one bucket");
        };

        match first_bucket {
            HistogramBucket::Int(_) => Ok(Self::Int(TypedHistogram {
                accuracy,
                row_scale: 1.0,
                buckets: buckets
                    .into_iter()
                    .map(|bucket| match bucket {
                        HistogramBucket::Int(bucket) => Ok(bucket),
                        _ => Err("histogram bucket types must be consistent"),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                avg_spacing,
            })),
            HistogramBucket::UInt(_) => Ok(Self::UInt(TypedHistogram {
                accuracy,
                row_scale: 1.0,
                buckets: buckets
                    .into_iter()
                    .map(|bucket| match bucket {
                        HistogramBucket::UInt(bucket) => Ok(bucket),
                        _ => Err("histogram bucket types must be consistent"),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                avg_spacing,
            })),
            HistogramBucket::Float(_) => Ok(Self::Float(TypedHistogram {
                accuracy,
                row_scale: 1.0,
                buckets: buckets
                    .into_iter()
                    .map(|bucket| match bucket {
                        HistogramBucket::Float(bucket) => Ok(bucket),
                        _ => Err("histogram bucket types must be consistent"),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                avg_spacing,
            })),
            HistogramBucket::Bytes(_) => Ok(Self::Bytes(TypedHistogram {
                accuracy,
                row_scale: 1.0,
                buckets: buckets
                    .into_iter()
                    .map(|bucket| match bucket {
                        HistogramBucket::Bytes(bucket) => Ok(bucket),
                        _ => Err("histogram bucket types must be consistent"),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                avg_spacing,
            })),
        }
    }

    pub fn accuracy(&self) -> bool {
        match self {
            Self::Int(histogram) => histogram.accuracy,
            Self::UInt(histogram) => histogram.accuracy,
            Self::Float(histogram) => histogram.accuracy,
            Self::Bytes(histogram) => histogram.accuracy,
        }
    }

    pub fn avg_spacing(&self) -> Option<f64> {
        match self {
            Self::Int(histogram) => histogram.avg_spacing,
            Self::UInt(histogram) => histogram.avg_spacing,
            Self::Float(histogram) => histogram.avg_spacing,
            Self::Bytes(histogram) => histogram.avg_spacing,
        }
    }

    pub fn num_buckets(&self) -> usize {
        match self {
            Self::Int(histogram) => histogram.num_buckets(),
            Self::UInt(histogram) => histogram.num_buckets(),
            Self::Float(histogram) => histogram.num_buckets(),
            Self::Bytes(histogram) => histogram.num_buckets(),
        }
    }

    pub fn num_values(&self) -> f64 {
        match self {
            Self::Int(histogram) => histogram.num_values(),
            Self::UInt(histogram) => histogram.num_values(),
            Self::Float(histogram) => histogram.num_values(),
            Self::Bytes(histogram) => histogram.num_values(),
        }
    }

    pub fn ndv(&self) -> NdvEstimate {
        match self {
            Self::Int(histogram) => histogram.ndv(),
            Self::UInt(histogram) => histogram.ndv(),
            Self::Float(histogram) => histogram.ndv(),
            Self::Bytes(histogram) => histogram.ndv(),
        }
    }

    pub fn bucket_iter(&self) -> HistogramBucketIter<'_> {
        match self {
            Self::Int(histogram) => HistogramBucketIter::Int {
                iter: histogram.buckets.iter(),
                row_scale: histogram.row_scale,
            },
            Self::UInt(histogram) => HistogramBucketIter::UInt {
                iter: histogram.buckets.iter(),
                row_scale: histogram.row_scale,
            },
            Self::Float(histogram) => HistogramBucketIter::Float {
                iter: histogram.buckets.iter(),
                row_scale: histogram.row_scale,
            },
            Self::Bytes(histogram) => HistogramBucketIter::Bytes {
                iter: histogram.buckets.iter(),
                row_scale: histogram.row_scale,
            },
        }
    }

    pub fn scale_counts(&mut self, selectivity: f64) {
        match self {
            Self::Int(histogram) => histogram.scale_counts(selectivity),
            Self::UInt(histogram) => histogram.scale_counts(selectivity),
            Self::Float(histogram) => histogram.scale_counts(selectivity),
            Self::Bytes(histogram) => histogram.scale_counts(selectivity),
        }
    }

    pub fn collapse_counts_to_distinct(&mut self) {
        match self {
            Self::Int(histogram) => histogram.collapse_counts_to_distinct(),
            Self::UInt(histogram) => histogram.collapse_counts_to_distinct(),
            Self::Float(histogram) => histogram.collapse_counts_to_distinct(),
            Self::Bytes(histogram) => histogram.collapse_counts_to_distinct(),
        }
    }

    /// Estimate a join only when both histograms use the same typed bucket representation.
    pub fn estimate_join(&self, other: &Histogram) -> ExceptionResult<JoinEstimation> {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Ok(left.estimate_join(right)),
            (Self::UInt(left), Self::UInt(right)) => Ok(left.estimate_join(right)),
            (Self::Float(left), Self::Float(right)) => Ok(left.estimate_join(right)),
            (Self::Bytes(left), Self::Bytes(right)) => Ok(left.estimate_join(right)),
            _ => Err(ErrorCode::Internal(
                "cannot estimate join for histograms with different bucket types",
            )),
        }
    }

    /// Estimate a numeric join in the comparison expression's return type.
    ///
    /// Mixed histogram variants are evaluated bucket-by-bucket in that type. This avoids
    /// materializing a float histogram and preserves exact signed/unsigned integer boundaries.
    pub fn estimate_join_numeric(
        &self,
        other: &Histogram,
        return_type: NumericHistogramType,
    ) -> ExceptionResult<Option<JoinEstimation>> {
        BorrowedHistogram::from(self)
            .estimate_join_numeric(BorrowedHistogram::from(other), return_type)
    }

    /// Infer the computation type from histogram variants when the caller has no expression type.
    pub fn estimate_join_numeric_compatible(
        &self,
        other: &Histogram,
    ) -> ExceptionResult<Option<JoinEstimation>> {
        BorrowedHistogram::from(self)
            .estimate_join_numeric_compatible(BorrowedHistogram::from(other))
    }

    pub fn is_range_distorted(&self) -> bool {
        self.avg_spacing()
            .is_some_and(|bucket_width| bucket_width > 1e12)
    }
}

pub enum HistogramBucketIter<'a> {
    Int {
        iter: std::slice::Iter<'a, TypedHistogramBucket<i64>>,
        row_scale: f64,
    },
    UInt {
        iter: std::slice::Iter<'a, TypedHistogramBucket<u64>>,
        row_scale: f64,
    },
    Float {
        iter: std::slice::Iter<'a, TypedHistogramBucket<F64>>,
        row_scale: f64,
    },
    Bytes {
        iter: std::slice::Iter<'a, TypedHistogramBucket<Vec<u8>>>,
        row_scale: f64,
    },
}

impl<'a> Iterator for HistogramBucketIter<'a> {
    type Item = HistogramBucketView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HistogramBucketIter::Int { iter, row_scale } => {
                iter.next().map(|bucket| HistogramBucketView::Int {
                    bucket,
                    row_scale: *row_scale,
                })
            }
            HistogramBucketIter::UInt { iter, row_scale } => {
                iter.next().map(|bucket| HistogramBucketView::UInt {
                    bucket,
                    row_scale: *row_scale,
                })
            }
            HistogramBucketIter::Float { iter, row_scale } => {
                iter.next().map(|bucket| HistogramBucketView::Float {
                    bucket,
                    row_scale: *row_scale,
                })
            }
            HistogramBucketIter::Bytes { iter, row_scale } => {
                iter.next().map(|bucket| HistogramBucketView::Bytes {
                    bucket,
                    row_scale: *row_scale,
                })
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            HistogramBucketIter::Int { iter, .. } => iter.size_hint(),
            HistogramBucketIter::UInt { iter, .. } => iter.size_hint(),
            HistogramBucketIter::Float { iter, .. } => iter.size_hint(),
            HistogramBucketIter::Bytes { iter, .. } => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for HistogramBucketIter<'_> {}

#[derive(Debug, Clone, Copy)]
pub enum HistogramBucketView<'a> {
    Int {
        bucket: &'a TypedHistogramBucket<i64>,
        row_scale: f64,
    },
    UInt {
        bucket: &'a TypedHistogramBucket<u64>,
        row_scale: f64,
    },
    Float {
        bucket: &'a TypedHistogramBucket<F64>,
        row_scale: f64,
    },
    Bytes {
        bucket: &'a TypedHistogramBucket<Vec<u8>>,
        row_scale: f64,
    },
}

impl HistogramBucketView<'_> {
    pub fn lower_bound(&self) -> Datum {
        match self {
            HistogramBucketView::Int { bucket, .. } => Datum::Int(*bucket.lower_bound()),
            HistogramBucketView::UInt { bucket, .. } => Datum::UInt(*bucket.lower_bound()),
            HistogramBucketView::Float { bucket, .. } => Datum::Float(*bucket.lower_bound()),
            HistogramBucketView::Bytes { bucket, .. } => Datum::Bytes(bucket.lower_bound().clone()),
        }
    }

    pub fn upper_bound(&self) -> Datum {
        match self {
            HistogramBucketView::Int { bucket, .. } => Datum::Int(*bucket.upper_bound()),
            HistogramBucketView::UInt { bucket, .. } => Datum::UInt(*bucket.upper_bound()),
            HistogramBucketView::Float { bucket, .. } => Datum::Float(*bucket.upper_bound()),
            HistogramBucketView::Bytes { bucket, .. } => Datum::Bytes(bucket.upper_bound().clone()),
        }
    }

    pub fn num_values(&self) -> f64 {
        match self {
            HistogramBucketView::Int { bucket, row_scale } => bucket.num_values() * row_scale,
            HistogramBucketView::UInt { bucket, row_scale } => bucket.num_values() * row_scale,
            HistogramBucketView::Float { bucket, row_scale } => bucket.num_values() * row_scale,
            HistogramBucketView::Bytes { bucket, row_scale } => bucket.num_values() * row_scale,
        }
    }

    pub fn num_distinct(&self) -> f64 {
        match self {
            HistogramBucketView::Int { bucket, .. } => bucket.num_distinct(),
            HistogramBucketView::UInt { bucket, .. } => bucket.num_distinct(),
            HistogramBucketView::Float { bucket, .. } => bucket.num_distinct(),
            HistogramBucketView::Bytes { bucket, .. } => bucket.num_distinct(),
        }
    }

    pub fn owned(&self) -> HistogramBucket {
        match self {
            HistogramBucketView::Int { bucket, row_scale } => {
                HistogramBucket::Int(TypedHistogramBucket::new(
                    *bucket.lower_bound(),
                    *bucket.upper_bound(),
                    bucket.num_values() * row_scale,
                    bucket.num_distinct(),
                ))
            }
            HistogramBucketView::UInt { bucket, row_scale } => {
                HistogramBucket::UInt(TypedHistogramBucket::new(
                    *bucket.lower_bound(),
                    *bucket.upper_bound(),
                    bucket.num_values() * row_scale,
                    bucket.num_distinct(),
                ))
            }
            HistogramBucketView::Float { bucket, row_scale } => {
                HistogramBucket::Float(TypedHistogramBucket::new(
                    *bucket.lower_bound(),
                    *bucket.upper_bound(),
                    bucket.num_values() * row_scale,
                    bucket.num_distinct(),
                ))
            }
            HistogramBucketView::Bytes { bucket, row_scale } => {
                HistogramBucket::Bytes(TypedHistogramBucket::new(
                    bucket.lower_bound().clone(),
                    bucket.upper_bound().clone(),
                    bucket.num_values() * row_scale,
                    bucket.num_distinct(),
                ))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum HistogramBucket {
    Int(TypedHistogramBucket<i64>),
    UInt(TypedHistogramBucket<u64>),
    Float(TypedHistogramBucket<F64>),
    Bytes(TypedHistogramBucket<Vec<u8>>),
}

impl HistogramBucket {
    pub fn try_from_bounds(
        lower_bound: Datum,
        upper_bound: Datum,
        num_values: f64,
        num_distinct: f64,
    ) -> Result<Self, &'static str> {
        match (lower_bound, upper_bound) {
            (Datum::Int(lower_bound), Datum::Int(upper_bound)) => Ok(Self::Int(
                TypedHistogramBucket::new(lower_bound, upper_bound, num_values, num_distinct),
            )),
            (Datum::UInt(lower_bound), Datum::UInt(upper_bound)) => Ok(Self::UInt(
                TypedHistogramBucket::new(lower_bound, upper_bound, num_values, num_distinct),
            )),
            (Datum::Float(lower_bound), Datum::Float(upper_bound)) => Ok(Self::Float(
                TypedHistogramBucket::new(lower_bound, upper_bound, num_values, num_distinct),
            )),
            (Datum::Bytes(lower_bound), Datum::Bytes(upper_bound)) => Ok(Self::Bytes(
                TypedHistogramBucket::new(lower_bound, upper_bound, num_values, num_distinct),
            )),
            _ => Err("histogram bucket bounds must have the same supported type"),
        }
    }

    pub fn upper_bound(&self) -> Datum {
        match self {
            Self::Int(bucket) => Datum::Int(*bucket.upper_bound()),
            Self::UInt(bucket) => Datum::UInt(*bucket.upper_bound()),
            Self::Float(bucket) => Datum::Float(*bucket.upper_bound()),
            Self::Bytes(bucket) => Datum::Bytes(bucket.upper_bound().clone()),
        }
    }

    pub fn lower_bound(&self) -> Datum {
        match self {
            Self::Int(bucket) => Datum::Int(*bucket.lower_bound()),
            Self::UInt(bucket) => Datum::UInt(*bucket.lower_bound()),
            Self::Float(bucket) => Datum::Float(*bucket.lower_bound()),
            Self::Bytes(bucket) => Datum::Bytes(bucket.lower_bound().clone()),
        }
    }

    pub fn num_values(&self) -> f64 {
        match self {
            Self::Int(bucket) => bucket.num_values(),
            Self::UInt(bucket) => bucket.num_values(),
            Self::Float(bucket) => bucket.num_values(),
            Self::Bytes(bucket) => bucket.num_values(),
        }
    }

    pub fn num_distinct(&self) -> f64 {
        match self {
            Self::Int(bucket) => bucket.num_distinct(),
            Self::UInt(bucket) => bucket.num_distinct(),
            Self::Float(bucket) => bucket.num_distinct(),
            Self::Bytes(bucket) => bucket.num_distinct(),
        }
    }
}
impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for bucket in self.bucket_iter() {
            writeln!(
                f,
                "[lower: {}, upper: {}, ndv: {}, count: {}]",
                bucket.lower_bound(),
                bucket.upper_bound(),
                bucket.num_distinct(),
                bucket.num_values()
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::StatRangeBounds;

    #[test]
    fn test_range_constraint_bounds_use_discrete_exclusive_edges() -> ExceptionResult<()> {
        let input_bounds = StatBounds::new(Datum::UInt(0), Datum::UInt(19)).unwrap();
        let bounds =
            input_bounds.restrict_by_range(&Bound::Unbounded, &Bound::Excluded(Datum::UInt(15)));

        assert_eq!(
            bounds,
            StatRangeBounds::Bounds(StatBounds::new(Datum::UInt(0), Datum::UInt(14)).unwrap())
        );
        Ok(())
    }

    #[test]
    fn test_mixed_numeric_join_calculates_existing_buckets_directly() -> ExceptionResult<()> {
        let left = Histogram::Int(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(1, 1, 3.0, 1.0)],
            avg_spacing: None,
        });
        let right = Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(1, 1, 2.0, 1.0)],
            avg_spacing: None,
        });

        let estimation = left
            .estimate_join_numeric_compatible(&right)?
            .expect("mixed numeric histograms should be comparable");

        assert_eq!(estimation.cardinality.expected, 6.0);
        assert_eq!(estimation.ndv.expected, Some(1.0));
        Ok(())
    }

    #[test]
    fn test_direct_numeric_join_matches_typed_join_for_representable_ranges() -> ExceptionResult<()>
    {
        let left_int = Histogram::Int(TypedHistogram {
            accuracy: false,
            row_scale: 0.5,
            buckets: vec![
                TypedHistogramBucket::new(0, 10, 40.0, 10.0),
                TypedHistogramBucket::new(20, 20, 8.0, 1.0),
                TypedHistogramBucket::new(40, 50, 20.0, 8.0),
            ],
            avg_spacing: None,
        });
        let left_float = Histogram::Float(TypedHistogram {
            accuracy: false,
            row_scale: 0.5,
            buckets: vec![
                TypedHistogramBucket::new(F64::from(0.0), F64::from(10.0), 40.0, 10.0),
                TypedHistogramBucket::new(F64::from(20.0), F64::from(20.0), 8.0, 1.0),
                TypedHistogramBucket::new(F64::from(40.0), F64::from(50.0), 20.0, 8.0),
            ],
            avg_spacing: None,
        });
        let right_float = Histogram::Float(TypedHistogram {
            accuracy: false,
            row_scale: 0.25,
            buckets: vec![
                TypedHistogramBucket::new(F64::from(5.0), F64::from(15.0), 32.0, 9.0),
                TypedHistogramBucket::new(F64::from(20.0), F64::from(20.0), 12.0, 1.0),
                TypedHistogramBucket::new(F64::from(60.0), F64::from(70.0), 16.0, 7.0),
            ],
            avg_spacing: None,
        });

        let native = left_float.estimate_join(&right_float)?;
        let direct = left_int
            .estimate_join_numeric(&right_float, NumericHistogramType::Float)?
            .expect("numeric histograms should be comparable");

        assert_eq!(direct.cardinality, native.cardinality);
        assert_eq!(direct.ndv, native.ndv);
        assert!(direct.histogram.is_none());
        Ok(())
    }

    #[test]
    fn test_direct_mixed_integer_join_matches_typed_join() -> ExceptionResult<()> {
        let left = Histogram::Int(TypedHistogram {
            accuracy: false,
            row_scale: 0.5,
            buckets: vec![TypedHistogramBucket::new(-10, 20, 80.0, 24.0)],
            avg_spacing: None,
        });
        let right_int = Histogram::Int(TypedHistogram {
            accuracy: false,
            row_scale: 0.25,
            buckets: vec![TypedHistogramBucket::new(5, 30, 60.0, 20.0)],
            avg_spacing: None,
        });
        let right_uint = Histogram::UInt(TypedHistogram {
            accuracy: false,
            row_scale: 0.25,
            buckets: vec![TypedHistogramBucket::new(5, 30, 60.0, 20.0)],
            avg_spacing: None,
        });

        let native = left.estimate_join(&right_int)?;
        let direct = left
            .estimate_join_numeric(&right_uint, NumericHistogramType::Int)?
            .expect("integer histograms should be comparable as Int64");

        assert_eq!(direct.cardinality, native.cardinality);
        assert_eq!(direct.ndv, native.ndv);
        assert!(direct.histogram.is_none());
        Ok(())
    }

    #[test]
    fn test_mixed_integer_join_preserves_large_integer_boundaries() -> ExceptionResult<()> {
        let left = Histogram::Int(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                9_007_199_254_740_992,
                9_007_199_254_740_992,
                3.0,
                1.0,
            )],
            avg_spacing: None,
        });
        let right = Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                9_007_199_254_740_993,
                9_007_199_254_740_993,
                2.0,
                1.0,
            )],
            avg_spacing: None,
        });

        let estimation = left
            .estimate_join_numeric(&right, NumericHistogramType::Int)?
            .expect("integer histograms should be comparable as Int64");

        assert_eq!(estimation.cardinality.expected, 0.0);
        assert_eq!(estimation.ndv.expected, Some(0.0));
        Ok(())
    }

    #[test]
    fn test_numeric_join_uses_requested_return_type() -> ExceptionResult<()> {
        let left = Histogram::Int(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                9_007_199_254_740_992,
                9_007_199_254_740_992,
                3.0,
                1.0,
            )],
            avg_spacing: None,
        });
        let right = Histogram::Int(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                9_007_199_254_740_993,
                9_007_199_254_740_993,
                2.0,
                1.0,
            )],
            avg_spacing: None,
        });

        let integer = left
            .estimate_join_numeric(&right, NumericHistogramType::Int)?
            .expect("integer histograms should be comparable");
        let float = left
            .estimate_join_numeric(&right, NumericHistogramType::Float)?
            .expect("integer histograms should be comparable as float");

        assert_eq!(integer.cardinality.expected, 0.0);
        assert_eq!(float.cardinality.expected, 6.0);
        Ok(())
    }

    #[test]
    fn test_integer_join_compares_values_outside_return_type_range() -> ExceptionResult<()> {
        let left = Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(u64::MAX, u64::MAX, 1.0, 1.0)],
            avg_spacing: None,
        });
        let right = Histogram::Int(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0, 0, 1.0, 1.0)],
            avg_spacing: None,
        });

        let estimation = left
            .estimate_join_numeric(&right, NumericHistogramType::Int)
            .expect("integer statistics must not fail on a wide comparison")
            .expect("integer histograms should be comparable");
        assert_eq!(estimation.cardinality.expected, 0.0);
        Ok(())
    }
}
