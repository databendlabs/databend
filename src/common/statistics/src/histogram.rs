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
use crate::TypedHistogram;
use crate::TypedHistogramBucket;

pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 100;

/// A column histogram used by optimizer statistics.
///
/// Histograms currently have two sources with different reliability:
/// - `accuracy == true`: buckets come from `ANALYZE TABLE`. For each supported
///   non-null column, ANALYZE runs a query equivalent to sorting rows by the
///   column, assigning `NTILE(DEFAULT_HISTOGRAM_BUCKETS)`, then grouping by tile
///   and collecting `MIN(col)`, `MAX(col)`, `COUNT()`, and
///   `COUNT(DISTINCT col)`. Each bucket is therefore the closed value envelope
///   observed in one row-order tile. The bucket list is not a value-domain
///   partition: adjacent buckets may share boundaries or overlap when duplicate
///   values cross tile boundaries.
/// - `accuracy == false`: buckets are synthesized from column NDV plus
///   min/max bounds by [`crate::HistogramBuilder::from_ndv`]. These buckets
///   assume a uniform distribution over the recorded bounds, and numeric
///   histograms keep `avg_spacing` so consumers can detect distorted ranges.
///
/// Consumers should preserve this distinction when updating or interpreting
/// bucket counts. The type variants preserve the bucket value type for
/// serialization, function selectivity, and type-specific join estimation.
#[derive(Debug, Clone)]
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

    pub fn num_distinct_values(&self) -> f64 {
        match self {
            Self::Int(histogram) => histogram.num_distinct_values(),
            Self::UInt(histogram) => histogram.num_distinct_values(),
            Self::Float(histogram) => histogram.num_distinct_values(),
            Self::Bytes(histogram) => histogram.num_distinct_values(),
        }
    }

    pub fn bucket_iter(&self) -> HistogramBucketIter<'_> {
        match self {
            Self::Int(histogram) => HistogramBucketIter::Int(histogram.buckets.iter()),
            Self::UInt(histogram) => HistogramBucketIter::UInt(histogram.buckets.iter()),
            Self::Float(histogram) => HistogramBucketIter::Float(histogram.buckets.iter()),
            Self::Bytes(histogram) => HistogramBucketIter::Bytes(histogram.buckets.iter()),
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
        let buckets = self
            .bucket_iter()
            .map(|bucket| {
                let bucket_min = bucket.lower_bound();
                let bucket_max = bucket.upper_bound();
                if bucket_min.compare(max)? == std::cmp::Ordering::Greater
                    || bucket_max.compare(min)? == std::cmp::Ordering::Less
                {
                    return Ok(None);
                }

                let Some(lower_bound) = max_datum_as_bucket_kind(&bucket_min, min) else {
                    return Ok(None);
                };
                let Some(upper_bound) = min_datum_as_bucket_kind(&bucket_max, max) else {
                    return Ok(None);
                };
                if lower_bound.compare(&upper_bound)? == std::cmp::Ordering::Greater {
                    return Ok(None);
                }

                let selectivity = bucket_overlap_selectivity(
                    &bucket_min,
                    &bucket_max,
                    &lower_bound,
                    &upper_bound,
                );
                HistogramBucket::try_from_bounds(
                    lower_bound,
                    upper_bound,
                    bucket.num_values() * selectivity,
                    bucket.num_distinct() * selectivity,
                )
                .map(Some)
                .map_err(|err| ErrorCode::Internal(err.to_string()))
            })
            .collect::<ExceptionResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        if buckets.is_empty() {
            return Ok(None);
        }

        Self::try_from_buckets(self.accuracy(), buckets, self.avg_spacing())
            .map(Some)
            .map_err(|err| ErrorCode::Internal(err.to_string()))
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

    Ok(Some(left.estimate_join(&right)))
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

fn bucket_overlap_selectivity(
    bucket_min: &Datum,
    bucket_max: &Datum,
    new_min: &Datum,
    new_max: &Datum,
) -> f64 {
    match (bucket_min, bucket_max, new_min, new_max) {
        (
            Datum::Int(bucket_min),
            Datum::Int(bucket_max),
            Datum::Int(new_min),
            Datum::Int(new_max),
        ) => discrete_overlap_selectivity(
            *bucket_min as i128,
            *bucket_max as i128,
            *new_min as i128,
            *new_max as i128,
        ),
        (
            Datum::UInt(bucket_min),
            Datum::UInt(bucket_max),
            Datum::UInt(new_min),
            Datum::UInt(new_max),
        ) => discrete_overlap_selectivity(
            *bucket_min as i128,
            *bucket_max as i128,
            *new_min as i128,
            *new_max as i128,
        ),
        (
            Datum::Float(bucket_min),
            Datum::Float(bucket_max),
            Datum::Float(new_min),
            Datum::Float(new_max),
        ) => {
            let bucket_width = bucket_max.into_inner() - bucket_min.into_inner();
            if bucket_width <= 0.0 {
                return 1.0;
            }
            let overlap_width = new_max.into_inner() - new_min.into_inner();
            let selectivity = overlap_width / bucket_width;
            debug_assert!(
                (0.0..=1.0).contains(&selectivity),
                "invalid float bucket overlap selectivity: {selectivity:?}"
            );
            selectivity
        }
        (Datum::Bytes(_), Datum::Bytes(_), Datum::Bytes(_), Datum::Bytes(_)) => 1.0,
        _ => 1.0,
    }
}

fn discrete_overlap_selectivity(
    bucket_min: i128,
    bucket_max: i128,
    new_min: i128,
    new_max: i128,
) -> f64 {
    let bucket_count = bucket_max - bucket_min + 1;
    if bucket_count <= 0 {
        return 1.0;
    }
    let overlap_count = new_max - new_min + 1;
    let selectivity = overlap_count as f64 / bucket_count as f64;
    debug_assert!(
        (0.0..=1.0).contains(&selectivity),
        "invalid discrete bucket overlap selectivity: {selectivity:?}"
    );
    selectivity
}

pub enum HistogramBucketIter<'a> {
    Int(std::slice::Iter<'a, TypedHistogramBucket<i64>>),
    UInt(std::slice::Iter<'a, TypedHistogramBucket<u64>>),
    Float(std::slice::Iter<'a, TypedHistogramBucket<F64>>),
    Bytes(std::slice::Iter<'a, TypedHistogramBucket<Vec<u8>>>),
}

impl<'a> Iterator for HistogramBucketIter<'a> {
    type Item = HistogramBucketView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HistogramBucketIter::Int(iter) => iter.next().map(HistogramBucketView::Int),
            HistogramBucketIter::UInt(iter) => iter.next().map(HistogramBucketView::UInt),
            HistogramBucketIter::Float(iter) => iter.next().map(HistogramBucketView::Float),
            HistogramBucketIter::Bytes(iter) => iter.next().map(HistogramBucketView::Bytes),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            HistogramBucketIter::Int(iter) => iter.size_hint(),
            HistogramBucketIter::UInt(iter) => iter.size_hint(),
            HistogramBucketIter::Float(iter) => iter.size_hint(),
            HistogramBucketIter::Bytes(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for HistogramBucketIter<'_> {}

#[derive(Debug, Clone, Copy)]
pub enum HistogramBucketView<'a> {
    Int(&'a TypedHistogramBucket<i64>),
    UInt(&'a TypedHistogramBucket<u64>),
    Float(&'a TypedHistogramBucket<F64>),
    Bytes(&'a TypedHistogramBucket<Vec<u8>>),
}

impl HistogramBucketView<'_> {
    pub fn lower_bound(&self) -> Datum {
        match self {
            HistogramBucketView::Int(bucket) => Datum::Int(*bucket.lower_bound()),
            HistogramBucketView::UInt(bucket) => Datum::UInt(*bucket.lower_bound()),
            HistogramBucketView::Float(bucket) => Datum::Float(*bucket.lower_bound()),
            HistogramBucketView::Bytes(bucket) => Datum::Bytes(bucket.lower_bound().clone()),
        }
    }

    pub fn upper_bound(&self) -> Datum {
        match self {
            HistogramBucketView::Int(bucket) => Datum::Int(*bucket.upper_bound()),
            HistogramBucketView::UInt(bucket) => Datum::UInt(*bucket.upper_bound()),
            HistogramBucketView::Float(bucket) => Datum::Float(*bucket.upper_bound()),
            HistogramBucketView::Bytes(bucket) => Datum::Bytes(bucket.upper_bound().clone()),
        }
    }

    pub fn num_values(&self) -> f64 {
        match self {
            HistogramBucketView::Int(bucket) => bucket.num_values(),
            HistogramBucketView::UInt(bucket) => bucket.num_values(),
            HistogramBucketView::Float(bucket) => bucket.num_values(),
            HistogramBucketView::Bytes(bucket) => bucket.num_values(),
        }
    }

    pub fn num_distinct(&self) -> f64 {
        match self {
            HistogramBucketView::Int(bucket) => bucket.num_distinct(),
            HistogramBucketView::UInt(bucket) => bucket.num_distinct(),
            HistogramBucketView::Float(bucket) => bucket.num_distinct(),
            HistogramBucketView::Bytes(bucket) => bucket.num_distinct(),
        }
    }

    pub fn owned(&self) -> HistogramBucket {
        match self {
            HistogramBucketView::Int(bucket) => HistogramBucket::Int((*bucket).clone()),
            HistogramBucketView::UInt(bucket) => HistogramBucket::UInt((*bucket).clone()),
            HistogramBucketView::Float(bucket) => HistogramBucket::Float((*bucket).clone()),
            HistogramBucketView::Bytes(bucket) => HistogramBucket::Bytes((*bucket).clone()),
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

impl HistogramBounds {
    pub fn new(lower_bound: Datum, upper_bound: Datum) -> Self {
        Self {
            lower_bound,
            upper_bound,
        }
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
    fn test_mixed_numeric_join_uses_float_view_of_existing_buckets() -> ExceptionResult<()> {
        let left = Histogram::Int(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(1, 1, 3.0, 1.0)],
            avg_spacing: None,
        });
        let right = Histogram::UInt(TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(1, 1, 2.0, 1.0)],
            avg_spacing: None,
        });

        let estimation = left
            .estimate_join_numeric_compatible(&right)?
            .expect("mixed numeric histograms should use a float view");

        assert_eq!(estimation.cardinality.expected, 6.0);
        assert_eq!(estimation.ndv.expected, 1.0);
        Ok(())
    }
}
