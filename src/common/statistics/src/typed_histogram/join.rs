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

use std::cmp::Ordering;

use databend_common_exception::Result;

use super::Intersection;
use super::NumericValue;
use super::OverlapCoverage;
use super::TypedHistogram;
use super::TypedHistogramBucket;
use super::Value;
use crate::Histogram;
use crate::NdvEstimate;
use crate::NumericHistogramType;
use crate::NumericRange;
use crate::StatEstimate;
use crate::TypedHistogramBounds;

#[must_use]
#[derive(Debug, Clone, PartialEq)]
pub struct JoinEstimation {
    pub cardinality: StatEstimate,
    pub ndv: NdvEstimate,
    /// Input rows whose values are expected to occur on the other side. These
    /// are side row counts, not output pairs, so join fanout is not included.
    pub left_matched_rows: f64,
    pub right_matched_rows: f64,
    /// Matched side rows inferred from bucket overlap rather than proven by
    /// matching exact singleton values on both sides.
    pub left_estimated_matched_rows: f64,
    pub right_estimated_matched_rows: f64,
    pub left_matched_histogram: Option<Histogram>,
    pub right_matched_histogram: Option<Histogram>,
    pub histogram: Option<Histogram>,
}

impl JoinEstimation {
    pub fn zero() -> Self {
        Self {
            cardinality: StatEstimate::exact(0.0),
            ndv: NdvEstimate::exact(0.0),
            left_matched_rows: 0.0,
            right_matched_rows: 0.0,
            left_estimated_matched_rows: 0.0,
            right_estimated_matched_rows: 0.0,
            left_matched_histogram: None,
            right_matched_histogram: None,
            histogram: None,
        }
    }
}

struct JoinCounts {
    cardinality: StatEstimate,
    ndv: NdvEstimate,
    left_matched_rows: f64,
    right_matched_rows: f64,
    left_estimated_matched_rows: f64,
    right_estimated_matched_rows: f64,
}

impl JoinCounts {
    fn zero() -> Self {
        Self {
            cardinality: StatEstimate::exact(0.0),
            ndv: NdvEstimate::exact(0.0),
            left_matched_rows: 0.0,
            right_matched_rows: 0.0,
            left_estimated_matched_rows: 0.0,
            right_estimated_matched_rows: 0.0,
        }
    }

    fn add(&mut self, contribution: Self) {
        self.cardinality = self.cardinality.add(contribution.cardinality);
        self.ndv = self.ndv.add(contribution.ndv);
        self.left_matched_rows += contribution.left_matched_rows;
        self.right_matched_rows += contribution.right_matched_rows;
        self.left_estimated_matched_rows += contribution.left_estimated_matched_rows;
        self.right_estimated_matched_rows += contribution.right_estimated_matched_rows;
    }

    fn into_estimation(self, left_num_values: f64, right_num_values: f64) -> JoinEstimation {
        JoinEstimation {
            cardinality: self.cardinality,
            ndv: self.ndv,
            left_matched_rows: self.left_matched_rows.min(left_num_values),
            right_matched_rows: self.right_matched_rows.min(right_num_values),
            left_estimated_matched_rows: self.left_estimated_matched_rows.min(left_num_values),
            right_estimated_matched_rows: self.right_estimated_matched_rows.min(right_num_values),
            left_matched_histogram: None,
            right_matched_histogram: None,
            histogram: None,
        }
    }
}

#[derive(Default)]
struct ExpectedJoinCounts {
    cardinality: f64,
    ndv: f64,
    left_matched_rows: f64,
    right_matched_rows: f64,
}

struct JoinContribution<T> {
    counts: JoinCounts,
    left_matched_bucket: Option<TypedHistogramBucket<T>>,
    right_matched_bucket: Option<TypedHistogramBucket<T>>,
    output_bucket: Option<TypedHistogramBucket<T>>,
}

impl<T: Clone> JoinContribution<T> {
    fn zero() -> Self {
        Self {
            counts: JoinCounts::zero(),
            left_matched_bucket: None,
            right_matched_bucket: None,
            output_bucket: None,
        }
    }

    fn from_counts(
        counts: JoinCounts,
        overlap_bounds: TypedHistogramBounds<T>,
        singleton_match: bool,
    ) -> Self {
        let expected_cardinality = counts.cardinality.expected;
        let expected_ndv = counts.ndv.expected.unwrap_or(0.0);
        let TypedHistogramBounds {
            lower_bound,
            upper_bound,
        } = overlap_bounds;

        let matched_bucket = |num_values: f64| {
            (num_values > 0.0 && expected_ndv > 0.0).then(|| {
                TypedHistogramBucket::new(
                    lower_bound.clone(),
                    upper_bound.clone(),
                    num_values.max(expected_ndv),
                    expected_ndv,
                )
            })
        };
        let output_bucket = if singleton_match {
            Some(TypedHistogramBucket::new(
                lower_bound.clone(),
                upper_bound.clone(),
                expected_cardinality,
                1.0,
            ))
        } else {
            (expected_cardinality > 0.0 && expected_ndv > 0.0).then(|| {
                TypedHistogramBucket::new(
                    lower_bound.clone(),
                    upper_bound.clone(),
                    expected_cardinality.max(expected_ndv),
                    expected_ndv,
                )
            })
        };

        Self {
            left_matched_bucket: matched_bucket(counts.left_matched_rows),
            right_matched_bucket: matched_bucket(counts.right_matched_rows),
            output_bucket,
            counts,
        }
    }
}

struct JoinAccumulator<T> {
    counts: JoinCounts,
    left_matched_buckets: Vec<TypedHistogramBucket<T>>,
    right_matched_buckets: Vec<TypedHistogramBucket<T>>,
    output_buckets: Vec<TypedHistogramBucket<T>>,
}

impl<T: Value> JoinAccumulator<T> {
    fn new() -> Self {
        Self {
            counts: JoinCounts::zero(),
            left_matched_buckets: Vec::new(),
            right_matched_buckets: Vec::new(),
            output_buckets: Vec::new(),
        }
    }

    fn add(&mut self, contribution: JoinContribution<T>) {
        let JoinContribution {
            counts,
            left_matched_bucket,
            right_matched_bucket,
            output_bucket,
        } = contribution;

        self.counts.add(counts);
        self.left_matched_buckets.extend(left_matched_bucket);
        self.right_matched_buckets.extend(right_matched_bucket);
        self.output_buckets.extend(output_bucket);
    }

    fn finish(self, left: &TypedHistogram<T>, right: &TypedHistogram<T>) -> JoinEstimation {
        let Self {
            counts,
            left_matched_buckets,
            right_matched_buckets,
            output_buckets,
        } = self;
        let accuracy = left.accuracy && right.accuracy;
        let avg_spacing = left.avg_spacing.or(right.avg_spacing);

        JoinEstimation {
            cardinality: counts.cardinality,
            ndv: counts.ndv,
            left_matched_rows: counts.left_matched_rows.min(left.num_values()),
            right_matched_rows: counts.right_matched_rows.min(right.num_values()),
            left_estimated_matched_rows: counts.left_estimated_matched_rows.min(left.num_values()),
            right_estimated_matched_rows: counts
                .right_estimated_matched_rows
                .min(right.num_values()),
            left_matched_histogram: build_histogram(left_matched_buckets, accuracy, avg_spacing),
            right_matched_histogram: build_histogram(right_matched_buckets, accuracy, avg_spacing),
            histogram: build_histogram(output_buckets, accuracy, avg_spacing),
        }
    }
}

fn build_histogram<T: Value>(
    buckets: Vec<TypedHistogramBucket<T>>,
    accuracy: bool,
    avg_spacing: Option<f64>,
) -> Option<Histogram> {
    (!buckets.is_empty()).then(|| {
        T::into_histogram(TypedHistogram {
            accuracy,
            row_scale: 1.0,
            buckets,
            avg_spacing,
        })
    })
}

impl<T: Value> TypedHistogram<T> {
    pub fn estimate_join(&self, other: &TypedHistogram<T>) -> JoinEstimation {
        let mut accumulator = JoinAccumulator::new();

        for left_bucket in &self.buckets {
            for right_bucket in &other.buckets {
                accumulator.add(left_bucket.estimate_join_contribution(
                    right_bucket,
                    self.row_scale,
                    other.row_scale,
                ));
            }
        }

        accumulator.finish(self, other)
    }
}

impl<L> TypedHistogram<L> {
    pub(crate) fn estimate_join_as<R>(
        &self,
        other: &TypedHistogram<R>,
        return_type: NumericHistogramType,
    ) -> Result<JoinEstimation>
    where
        L: NumericValue,
        R: NumericValue,
    {
        let mut counts = JoinCounts::zero();

        for left_bucket in &self.buckets {
            for right_bucket in &other.buckets {
                counts.add(estimate_join_contribution_from_ranges(
                    left_bucket,
                    right_bucket,
                    self.row_scale,
                    other.row_scale,
                    return_type.project_range(&left_bucket.lower_bound, &left_bucket.upper_bound),
                    return_type.project_range(&right_bucket.lower_bound, &right_bucket.upper_bound),
                )?);
            }
        }

        Ok(counts.into_estimation(self.num_values(), other.num_values()))
    }
}

fn estimate_join_contribution_from_ranges<L, R>(
    left: &TypedHistogramBucket<L>,
    right: &TypedHistogramBucket<R>,
    left_row_scale: f64,
    right_row_scale: f64,
    left_bounds: NumericRange,
    right_bounds: NumericRange,
) -> Result<JoinCounts> {
    let Some(overlap_bounds) = left_bounds.intersection(right_bounds)? else {
        return Ok(JoinCounts::zero());
    };
    let intersection = if overlap_bounds.is_singleton() {
        Intersection::Point
    } else {
        Intersection::Range
    };
    let singleton_match =
        left_bounds.is_singleton() && right_bounds.is_singleton() && left_bounds == right_bounds;
    let coverage = match intersection {
        Intersection::None => None,
        Intersection::Point => OverlapCoverage::point(left.num_distinct, right.num_distinct),
        Intersection::Range => {
            match (
                overlap_bounds.width(),
                left_bounds.width(),
                right_bounds.width(),
            ) {
                (Some(overlap_width), Some(left_width), Some(right_width))
                    if overlap_width > 0.0 && left_width > 0.0 && right_width > 0.0 =>
                {
                    let left = overlap_width / left_width;
                    let right = overlap_width / right_width;
                    debug_assert!(
                        (0.0..=1.0).contains(&left),
                        "invalid left overlap coverage: {left:?}"
                    );
                    debug_assert!(
                        (0.0..=1.0).contains(&right),
                        "invalid right overlap coverage: {right:?}"
                    );
                    Some(OverlapCoverage { left, right })
                }
                _ => None,
            }
        }
    };
    Ok(estimate_join_counts(
        left,
        right,
        left_row_scale,
        right_row_scale,
        intersection,
        singleton_match,
        coverage,
    ))
}

impl<T: Value> TypedHistogramBucket<T> {
    fn estimate_join_contribution(
        &self,
        other: &TypedHistogramBucket<T>,
        left_row_scale: f64,
        right_row_scale: f64,
    ) -> JoinContribution<T> {
        let intersection = self.intersection_kind(other);
        if intersection == Intersection::None {
            return JoinContribution::zero();
        }

        let Some(overlap_bounds) = self.bounds().intersection(&other.bounds()) else {
            return JoinContribution::zero();
        };
        let singleton_match = self.is_singleton_value()
            && other.is_singleton_value()
            && T::compare(&self.lower_bound, &other.lower_bound) == Ordering::Equal;
        let coverage = T::estimate_overlap_coverages(self, other);
        let counts = estimate_join_counts(
            self,
            other,
            left_row_scale,
            right_row_scale,
            intersection,
            singleton_match,
            coverage,
        );
        JoinContribution::from_counts(counts, overlap_bounds, singleton_match)
    }
}

fn estimate_join_counts<L, R>(
    left: &TypedHistogramBucket<L>,
    right: &TypedHistogramBucket<R>,
    left_row_scale: f64,
    right_row_scale: f64,
    intersection: Intersection,
    singleton_match: bool,
    coverage: Option<OverlapCoverage>,
) -> JoinCounts {
    if intersection == Intersection::None {
        return JoinCounts::zero();
    }

    let upper_cardinality =
        (left.num_values * left_row_scale) * (right.num_values * right_row_scale);
    if singleton_match {
        let left_matched_rows = left.num_values * left_row_scale;
        let right_matched_rows = right.num_values * right_row_scale;
        return JoinCounts {
            cardinality: StatEstimate::exact(upper_cardinality),
            ndv: NdvEstimate::exact(1.0),
            left_matched_rows,
            right_matched_rows,
            left_estimated_matched_rows: left_matched_rows,
            right_estimated_matched_rows: right_matched_rows,
        };
    }

    let expected = coverage
        .and_then(|coverage| {
            estimate_expected_join_counts(
                left,
                right,
                coverage,
                left_row_scale,
                right_row_scale,
                upper_cardinality,
            )
        })
        .unwrap_or_default();
    let upper_ndv = match intersection {
        Intersection::None => 0.0,
        Intersection::Point => 1.0,
        Intersection::Range => coverage
            .map(|coverage| {
                let left_rows = left.num_values * left_row_scale * coverage.left;
                let right_rows = right.num_values * right_row_scale * coverage.right;
                let left_distinct = left.num_distinct * coverage.left;
                let right_distinct = right.num_distinct * coverage.right;
                left_distinct
                    .min(right_distinct)
                    .min(left_rows)
                    .min(right_rows)
            })
            .unwrap_or_else(|| left.num_distinct.min(right.num_distinct)),
    };

    debug_assert!(
        expected.cardinality <= upper_cardinality,
        "join expected cardinality exceeds cartesian upper: {:?} > {upper_cardinality:?}",
        expected.cardinality
    );
    debug_assert!(
        expected.ndv <= upper_ndv,
        "join expected ndv exceeds intersection upper: {:?} > {upper_ndv:?}",
        expected.ndv
    );

    JoinCounts {
        cardinality: StatEstimate::new(0.0, expected.cardinality, upper_cardinality),
        ndv: NdvEstimate::new(expected.ndv, upper_ndv),
        left_matched_rows: expected.left_matched_rows,
        right_matched_rows: expected.right_matched_rows,
        left_estimated_matched_rows: expected.left_matched_rows,
        right_estimated_matched_rows: expected.right_matched_rows,
    }
}

fn estimate_expected_join_counts<L, R>(
    left: &TypedHistogramBucket<L>,
    right: &TypedHistogramBucket<R>,
    coverage: OverlapCoverage,
    left_row_scale: f64,
    right_row_scale: f64,
    upper_cardinality: f64,
) -> Option<ExpectedJoinCounts> {
    let left_rows = left.num_values * left_row_scale * coverage.left;
    let right_rows = right.num_values * right_row_scale * coverage.right;
    let left_ndv =
        (left.expected_distinct_after_row_scale(left_row_scale) * coverage.left).min(left_rows);
    let right_ndv =
        (right.expected_distinct_after_row_scale(right_row_scale) * coverage.right).min(right_rows);
    let matched_ndv = left_ndv.min(right_ndv);
    let max_ndv = left_ndv.max(right_ndv);
    if max_ndv <= 0.0 {
        return None;
    }
    // The equality denominator is a value count. Fractional NDV estimates
    // below one would otherwise produce more rows than the cartesian upper.
    let effective_max_ndv = max_ndv.max(1.0);
    let match_factor = coverage.left * coverage.right / effective_max_ndv;
    debug_assert!(
        (0.0..=1.0).contains(&match_factor),
        "invalid join match factor: {match_factor:?}"
    );
    let expected_cardinality =
        (upper_cardinality / effective_max_ndv) * coverage.left * coverage.right;
    let matched_rows = |rows: f64, ndv: f64| {
        if ndv <= 0.0 {
            0.0
        } else {
            rows * matched_ndv / ndv
        }
    };
    Some(ExpectedJoinCounts {
        cardinality: expected_cardinality,
        ndv: matched_ndv,
        // Range overlap only identifies the rows whose values may match. Scale
        // those rows by the overlapping value-set fraction as well: otherwise
        // a sparse bucket is treated as covering every value in its range.
        left_matched_rows: matched_rows(left_rows, left_ndv),
        right_matched_rows: matched_rows(right_rows, right_ndv),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typed_histogram_estimate_join_keeps_point_overlap() {
        let left = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0_u64, 10_u64, 10.0, 10.0)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(10_u64, 20_u64, 10.0, 10.0)],
            avg_spacing: None,
        };

        assert_eq!(left.estimate_join(&right), JoinEstimation {
            cardinality: StatEstimate::new(0.0, 1.0, 100.0),
            ndv: NdvEstimate::new(1.0, 1.0),
            left_matched_rows: 1.0,
            right_matched_rows: 1.0,
            left_estimated_matched_rows: 1.0,
            right_estimated_matched_rows: 1.0,
            left_matched_histogram: Some(crate::Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 1.0, 1.0)],
                avg_spacing: None,
            })),
            right_matched_histogram: Some(crate::Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 1.0, 1.0)],
                avg_spacing: None,
            })),
            histogram: Some(crate::Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 1.0, 1.0)],
                avg_spacing: None,
            })),
        });
    }

    #[test]
    fn test_typed_histogram_estimate_join_models_singleton_bucket_matches() {
        let left = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(10_i64, 10_i64, 4.0, 1.0)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(10_i64, 10_i64, 3.0, 1.0)],
            avg_spacing: None,
        };

        assert_eq!(left.estimate_join(&right), JoinEstimation {
            cardinality: StatEstimate::exact(12.0),
            ndv: NdvEstimate::exact(1.0),
            left_matched_rows: 4.0,
            right_matched_rows: 3.0,
            left_estimated_matched_rows: 4.0,
            right_estimated_matched_rows: 3.0,
            left_matched_histogram: Some(crate::Histogram::Int(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 4.0, 1.0)],
                avg_spacing: None,
            })),
            right_matched_histogram: Some(crate::Histogram::Int(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 3.0, 1.0)],
                avg_spacing: None,
            })),
            histogram: Some(crate::Histogram::Int(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 12.0, 1.0)],
                avg_spacing: None,
            })),
        });
    }

    #[test]
    fn test_typed_histogram_estimate_join_applies_row_scale_to_counts() {
        let mut left = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(10_i64, 10_i64, 4.0, 1.0)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(10_i64, 10_i64, 3.0, 1.0)],
            avg_spacing: None,
        };
        left.scale_counts(0.5);

        let estimation = left.estimate_join(&right);

        assert_eq!(estimation.cardinality, StatEstimate::exact(6.0));
        assert_eq!(estimation.ndv, NdvEstimate::exact(1.0));
        let histogram = estimation
            .histogram
            .expect("singleton overlap should produce output histogram");
        assert_eq!(histogram.num_values(), 6.0);
        assert_eq!(histogram.ndv().expected, Some(1.0));
    }

    #[test]
    fn test_typed_histogram_estimate_join_caps_range_ndv_by_scaled_rows() {
        let mut filtered = TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0_i64, 999_i64, 2000.0, 1800.0)],
            avg_spacing: None,
        };
        let grouped = TypedHistogram {
            accuracy: false,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0_i64, 999_i64, 1800.0, 1800.0)],
            avg_spacing: None,
        };
        filtered.scale_counts(0.025);

        let estimation = filtered.estimate_join(&grouped);

        assert!((estimation.cardinality.expected - 50.0).abs() < 1e-9);
        assert!((estimation.ndv.expected.unwrap() - 49.930034990281634).abs() < 1e-9);
        let histogram = estimation
            .histogram
            .expect("range overlap should produce output histogram");
        assert_eq!(histogram.num_values(), 50.0);
        assert!((histogram.ndv().expected.unwrap() - 49.930034990281634).abs() < 1e-9);
    }

    #[test]
    fn test_typed_histogram_estimate_join_converts_string_output_to_bytes_histogram() {
        let left = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                "a".to_string(),
                "a".to_string(),
                2.0,
                1.0,
            )],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(
                "a".to_string(),
                "a".to_string(),
                3.0,
                1.0,
            )],
            avg_spacing: None,
        };

        assert_eq!(left.estimate_join(&right), JoinEstimation {
            cardinality: StatEstimate::exact(6.0),
            ndv: NdvEstimate::exact(1.0),
            left_matched_rows: 2.0,
            right_matched_rows: 3.0,
            left_estimated_matched_rows: 2.0,
            right_estimated_matched_rows: 3.0,
            left_matched_histogram: Some(crate::Histogram::Bytes(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(
                    b"a".to_vec(),
                    b"a".to_vec(),
                    2.0,
                    1.0,
                )],
                avg_spacing: None,
            })),
            right_matched_histogram: Some(crate::Histogram::Bytes(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(
                    b"a".to_vec(),
                    b"a".to_vec(),
                    3.0,
                    1.0,
                )],
                avg_spacing: None,
            })),
            histogram: Some(crate::Histogram::Bytes(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(
                    b"a".to_vec(),
                    b"a".to_vec(),
                    6.0,
                    1.0,
                )],
                avg_spacing: None,
            })),
        });
    }

    #[test]
    fn test_typed_histogram_estimate_join_caps_scaled_bucket_expected_count() {
        let left = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0_i64, 10_i64, 0.984, 0.93)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![TypedHistogramBucket::new(0_i64, 10_i64, 0.984, 0.93)],
            avg_spacing: None,
        };

        let raw_expected =
            left.buckets[0].num_values * right.buckets[0].num_values / left.buckets[0].num_distinct;
        let cartesian_upper = left.buckets[0].num_values * right.buckets[0].num_values;
        assert!(raw_expected > cartesian_upper);

        let estimation = left.estimate_join(&right);
        estimation.cardinality.check_consistency().unwrap();
        estimation.ndv.check_consistency().unwrap();
        assert_eq!(estimation.cardinality.upper, cartesian_upper);
        assert_eq!(
            estimation.cardinality.expected,
            estimation.cardinality.upper
        );
    }

    #[test]
    fn test_typed_histogram_estimate_join_uses_same_scaled_upper_for_expected_count() {
        let left = TypedHistogram {
            accuracy: true,
            row_scale: 0.1,
            buckets: vec![TypedHistogramBucket::new(0_i64, 10_i64, 0.1, 0.1)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 0.1,
            buckets: vec![TypedHistogramBucket::new(0_i64, 10_i64, 0.1, 0.1)],
            avg_spacing: None,
        };

        let estimation = left.estimate_join(&right);
        estimation.cardinality.check_consistency().unwrap();
        assert_eq!(
            estimation.cardinality.expected,
            estimation.cardinality.upper
        );
    }
}
