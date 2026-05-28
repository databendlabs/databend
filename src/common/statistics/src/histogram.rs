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
use std::ops::Bound;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result as ExceptionResult;

use crate::Datum;
use crate::F64;
use crate::JoinEstimation;
use crate::NdvEstimate;
use crate::TypedHistogram;
use crate::TypedHistogramBucket;

pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 100;

/// A column histogram used by optimizer statistics.
///
/// `accuracy == true` means bucket `num_distinct` values still come directly
/// from `ANALYZE TABLE` and may be used as exact NDV facts for the remaining
/// bucket set. ANALYZE buckets are observed row-order tiles: for each supported
/// non-null column, ANALYZE runs a query equivalent to sorting rows by the
/// column, assigning `NTILE(DEFAULT_HISTOGRAM_BUCKETS)`, then grouping by tile
/// and collecting `MIN(col)`, `MAX(col)`, `COUNT()`, and `COUNT(DISTINCT col)`.
/// Each bucket is therefore the closed value envelope observed in one
/// row-order tile. The bucket list is not a value-domain partition: adjacent
/// buckets may share boundaries or overlap when duplicate values cross tile
/// boundaries.
///
/// Histograms synthesized from column NDV plus min/max bounds by
/// [`crate::HistogramBuilder::from_ndv`] use `accuracy == false`. Scaling a
/// histogram by an independent selectivity also marks it inaccurate: scaling
/// only aligns row mass after a filter whose surviving values are unknown, so
/// the original bucket distinct counts are no longer exact facts. Range
/// clipping and join overlap estimation keep the input accuracy because they do
/// not by themselves perform that unknown-value alignment. Numeric synthetic
/// histograms keep `avg_spacing` separately so consumers can detect distorted
/// ranges.
///
/// Consumers should preserve this distinction when deriving column NDV from
/// bucket distinct counts. The type variants preserve the bucket value type for
/// serialization, function selectivity, and type-specific join estimation.
#[derive(Debug, Clone, PartialEq)]
pub enum Histogram {
    Int(TypedHistogram<i64>),
    UInt(TypedHistogram<u64>),
    Float(TypedHistogram<F64>),
    Bytes(TypedHistogram<Vec<u8>>),
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

    /// Estimate a join for matching histogram types, or for mixed numeric histograms
    /// through a temporary float view. Non-numeric mixed types return `None`.
    pub fn estimate_join_numeric_compatible(
        &self,
        other: &Histogram,
    ) -> ExceptionResult<Option<JoinEstimation>> {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Ok(Some(left.estimate_join(right))),
            (Self::UInt(left), Self::UInt(right)) => Ok(Some(left.estimate_join(right))),
            (Self::Float(left), Self::Float(right)) => Ok(Some(left.estimate_join(right))),
            (Self::Bytes(left), Self::Bytes(right)) => Ok(Some(left.estimate_join(right))),
            (Self::Bytes(_), _) | (_, Self::Bytes(_)) => Ok(None),
            _ => estimate_mixed_numeric_histogram_join(self, other),
        }
    }

    pub fn restrict_to_bounds(&self, min: &Datum, max: &Datum) -> ExceptionResult<Option<Self>> {
        let buckets = self.restricted_buckets(min, max)?;

        if buckets.is_empty() {
            return Ok(None);
        }

        Self::try_from_buckets(self.accuracy(), buckets, self.avg_spacing())
            .map(Some)
            .map_err(|err| ErrorCode::Internal(err.to_string()))
    }

    fn restricted_buckets(
        &self,
        min: &Datum,
        max: &Datum,
    ) -> ExceptionResult<Vec<HistogramBucket>> {
        let mut buckets = Vec::new();

        for bucket in self.bucket_iter() {
            let bucket_min = bucket.lower_bound();
            let bucket_max = bucket.upper_bound();
            if bucket_min.compare(max)? == std::cmp::Ordering::Greater
                || bucket_max.compare(min)? == std::cmp::Ordering::Less
            {
                continue;
            }

            let Some(lower_bound) = max_datum_as_bucket_kind(&bucket_min, min) else {
                continue;
            };
            let Some(upper_bound) = min_datum_as_bucket_kind(&bucket_max, max) else {
                continue;
            };
            if lower_bound.compare(&upper_bound)? == std::cmp::Ordering::Greater {
                continue;
            }

            let (num_values, num_distinct) = bucket_overlap_counts(
                &bucket_min,
                &bucket_max,
                &lower_bound,
                &upper_bound,
                bucket.num_values(),
                bucket.num_distinct(),
            );
            buckets.push(
                HistogramBucket::try_from_bounds(
                    lower_bound,
                    upper_bound,
                    num_values,
                    num_distinct,
                )
                .map_err(|err| ErrorCode::Internal(err.to_string()))?,
            );
        }

        Ok(buckets)
    }

    pub fn is_range_distorted(&self) -> bool {
        self.avg_spacing()
            .is_some_and(|bucket_width| bucket_width > 1e12)
    }
}

fn estimate_mixed_numeric_histogram_join(
    left: &Histogram,
    right: &Histogram,
) -> ExceptionResult<Option<JoinEstimation>> {
    let Some(left) = numeric_histogram_as_float(left)? else {
        return Ok(None);
    };
    let Some(right) = numeric_histogram_as_float(right)? else {
        return Ok(None);
    };

    let mut estimation = left.estimate_join(&right);
    estimation.histogram = None;
    Ok(Some(estimation))
}

fn numeric_histogram_as_float(
    histogram: &Histogram,
) -> ExceptionResult<Option<TypedHistogram<F64>>> {
    if matches!(histogram, Histogram::Bytes(_)) {
        return Ok(None);
    }

    let buckets = histogram
        .bucket_iter()
        .map(|bucket| {
            let lower_bound = F64::from(bucket.lower_bound().as_double()?);
            let upper_bound = F64::from(bucket.upper_bound().as_double()?);
            Ok(TypedHistogramBucket::new(
                lower_bound,
                upper_bound,
                bucket.num_values(),
                bucket.num_distinct(),
            ))
        })
        .collect::<ExceptionResult<Vec<_>>>()?;

    Ok(Some(TypedHistogram {
        accuracy: histogram.accuracy(),
        row_scale: 1.0,
        buckets,
        avg_spacing: histogram.avg_spacing(),
    }))
}

fn max_datum_as_bucket_kind(bucket_value: &Datum, stat_value: &Datum) -> Option<Datum> {
    let selected = if bucket_value.compare(stat_value).ok()? == std::cmp::Ordering::Less {
        stat_value
    } else {
        bucket_value
    };
    selected.normalize_to_kind(bucket_value.kind()?)
}

fn min_datum_as_bucket_kind(bucket_value: &Datum, stat_value: &Datum) -> Option<Datum> {
    let selected = if bucket_value.compare(stat_value).ok()? == std::cmp::Ordering::Greater {
        stat_value
    } else {
        bucket_value
    };
    selected.normalize_to_kind(bucket_value.kind()?)
}

fn bucket_overlap_counts(
    bucket_min: &Datum,
    bucket_max: &Datum,
    new_min: &Datum,
    new_max: &Datum,
    num_values: f64,
    num_distinct: f64,
) -> (f64, f64) {
    match (bucket_min, bucket_max, new_min, new_max) {
        (
            Datum::Int(bucket_min),
            Datum::Int(bucket_max),
            Datum::Int(new_min),
            Datum::Int(new_max),
        ) => discrete_overlap_counts(
            *bucket_min as i128,
            *bucket_max as i128,
            *new_min as i128,
            *new_max as i128,
            num_values,
            num_distinct,
        ),
        (
            Datum::UInt(bucket_min),
            Datum::UInt(bucket_max),
            Datum::UInt(new_min),
            Datum::UInt(new_max),
        ) => discrete_overlap_counts(
            *bucket_min as i128,
            *bucket_max as i128,
            *new_min as i128,
            *new_max as i128,
            num_values,
            num_distinct,
        ),
        (
            Datum::Float(bucket_min),
            Datum::Float(bucket_max),
            Datum::Float(new_min),
            Datum::Float(new_max),
        ) => {
            let bucket_width = bucket_max.into_inner() - bucket_min.into_inner();
            if bucket_width <= 0.0 {
                return (num_values, num_distinct);
            }
            let overlap_width = new_max.into_inner() - new_min.into_inner();
            let selectivity = overlap_width / bucket_width;
            debug_assert!(
                (0.0..=1.0).contains(&selectivity),
                "invalid float bucket overlap selectivity: {selectivity:?}"
            );
            (num_values * selectivity, num_distinct * selectivity)
        }
        (Datum::Bytes(_), Datum::Bytes(_), Datum::Bytes(_), Datum::Bytes(_)) => {
            (num_values, num_distinct)
        }
        _ => (num_values, num_distinct),
    }
}

fn discrete_overlap_counts(
    bucket_min: i128,
    bucket_max: i128,
    new_min: i128,
    new_max: i128,
    num_values: f64,
    num_distinct: f64,
) -> (f64, f64) {
    let bucket_count = bucket_max - bucket_min + 1;
    if bucket_count <= 0 {
        return (num_values, num_distinct);
    }
    let overlap_count = new_max - new_min + 1;
    let bucket_count = bucket_count as f64;
    let overlap_count = overlap_count as f64;
    let selectivity = overlap_count / bucket_count;
    debug_assert!(
        (0.0..=1.0).contains(&selectivity),
        "invalid discrete bucket overlap selectivity: {selectivity:?}"
    );
    let scale_count = |count| overlap_count * (count / bucket_count);
    (scale_count(num_values), scale_count(num_distinct))
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
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBounds {
    lower_bound: Datum,
    upper_bound: Datum,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HistogramRangeBounds {
    Bounds(HistogramBounds),
    Empty,
    Imprecise,
}

impl HistogramBounds {
    pub fn new(lower_bound: Datum, upper_bound: Datum) -> Self {
        Self {
            lower_bound,
            upper_bound,
        }
    }

    pub fn from_range_constraint(
        min: &Datum,
        max: &Datum,
        lower: &Bound<Datum>,
        upper: &Bound<Datum>,
    ) -> ExceptionResult<HistogramRangeBounds> {
        let new_min = match lower {
            Bound::Unbounded => Some(min.clone()),
            Bound::Included(datum) => Datum::max(Some(min.clone()), Some(datum.clone())),
            Bound::Excluded(datum) => {
                if datum.compare(max)? != std::cmp::Ordering::Less {
                    return Ok(HistogramRangeBounds::Empty);
                }
                if datum.compare(min)? == std::cmp::Ordering::Less {
                    Some(min.clone())
                } else {
                    let datum = match datum {
                        Datum::Bool(false) => Some(Datum::Bool(true)),
                        Datum::Int(value) => value.checked_add(1).map(Datum::Int),
                        Datum::UInt(value) => value.checked_add(1).map(Datum::UInt),
                        // Column stats store closed bounds. For types without
                        // a representable adjacent value, keep the literal as
                        // a coarse bound for the strict predicate.
                        Datum::Float(_) | Datum::Bytes(_) => Some(datum.clone()),
                        Datum::Bool(true) => None,
                    };
                    if datum.is_none() {
                        return Ok(HistogramRangeBounds::Imprecise);
                    };
                    datum
                }
            }
        };
        let new_max = match upper {
            Bound::Unbounded => Some(max.clone()),
            Bound::Included(datum) => Datum::min(Some(max.clone()), Some(datum.clone())),
            Bound::Excluded(datum) => {
                if datum.compare(min)? != std::cmp::Ordering::Greater {
                    return Ok(HistogramRangeBounds::Empty);
                }
                if datum.compare(max)? == std::cmp::Ordering::Greater {
                    Some(max.clone())
                } else {
                    let datum = match datum {
                        Datum::Bool(false) => None,
                        Datum::Bool(true) => Some(Datum::Bool(false)),
                        Datum::Int(value) => value.checked_sub(1).map(Datum::Int),
                        Datum::UInt(value) => value.checked_sub(1).map(Datum::UInt),
                        // See the lower-bound case above.
                        Datum::Float(_) | Datum::Bytes(_) => Some(datum.clone()),
                    };
                    if datum.is_none() {
                        return Ok(HistogramRangeBounds::Imprecise);
                    };
                    datum
                }
            }
        };

        let (Some(new_min), Some(new_max)) = (new_min, new_max) else {
            return Ok(HistogramRangeBounds::Empty);
        };
        if new_min.compare(&new_max)? == std::cmp::Ordering::Greater {
            return Ok(HistogramRangeBounds::Empty);
        }

        Ok(HistogramRangeBounds::Bounds(Self {
            lower_bound: new_min,
            upper_bound: new_max,
        }))
    }

    pub fn lower_bound(&self) -> &Datum {
        &self.lower_bound
    }

    pub fn upper_bound(&self) -> &Datum {
        &self.upper_bound
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
    use super::*;

    #[test]
    fn test_restrict_to_bounds_uses_existing_buckets() -> ExceptionResult<()> {
        let histogram = Histogram::UInt(TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(0, 4, 5.0, 5.0),
                TypedHistogramBucket::new(5, 9, 5.0, 5.0),
            ],
            avg_spacing: None,
        });

        let restricted = histogram
            .restrict_to_bounds(&Datum::UInt(2), &Datum::UInt(6))?
            .expect("histogram should keep intersecting buckets");
        let buckets = restricted.bucket_iter().collect::<Vec<_>>();

        assert!(restricted.accuracy());
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].lower_bound(), Datum::UInt(2));
        assert_eq!(buckets[0].upper_bound(), Datum::UInt(4));
        assert_eq!(buckets[0].num_values(), 3.0);
        assert_eq!(buckets[0].num_distinct(), 3.0);
        assert_eq!(buckets[1].lower_bound(), Datum::UInt(5));
        assert_eq!(buckets[1].upper_bound(), Datum::UInt(6));
        assert_eq!(buckets[1].num_values(), 2.0);
        assert_eq!(buckets[1].num_distinct(), 2.0);
        Ok(())
    }

    #[test]
    fn test_range_constraint_bounds_use_discrete_exclusive_edges() -> ExceptionResult<()> {
        let bounds = HistogramBounds::from_range_constraint(
            &Datum::UInt(0),
            &Datum::UInt(19),
            &Bound::Unbounded,
            &Bound::Excluded(Datum::UInt(15)),
        )?;

        assert_eq!(
            bounds,
            HistogramRangeBounds::Bounds(HistogramBounds::new(Datum::UInt(0), Datum::UInt(14)))
        );
        Ok(())
    }

    #[test]
    fn test_mixed_numeric_join_uses_float_view_of_existing_buckets() -> ExceptionResult<()> {
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
            .expect("mixed numeric histograms should use a float view");

        assert_eq!(estimation.cardinality.expected, 6.0);
        assert_eq!(estimation.ndv.expected, Some(1.0));
        Ok(())
    }
}
