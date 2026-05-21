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

use super::Intersection;
use super::OverlapCoverage;
use super::TypedHistogram;
use super::TypedHistogramBucket;
use super::Value;
use crate::Histogram;
use crate::StatEstimate;

#[must_use]
#[derive(Debug, Clone, PartialEq)]
pub struct JoinEstimation {
    pub cardinality: StatEstimate,
    pub ndv: StatEstimate,
    pub histogram: Option<Histogram>,
}

impl JoinEstimation {
    pub fn zero() -> Self {
        Self {
            cardinality: StatEstimate::exact(0.0),
            ndv: StatEstimate::exact(0.0),
            histogram: None,
        }
    }
}

impl<T: Value> TypedHistogram<T> {
    pub fn estimate_join(&self, other: &TypedHistogram<T>) -> JoinEstimation {
        let mut cardinality = StatEstimate::exact(0.0);
        let mut ndv = StatEstimate::exact(0.0);
        let mut output_buckets = Vec::new();

        for left_bucket in &self.buckets {
            for right_bucket in &other.buckets {
                let contribution = left_bucket.estimate_join_contribution(
                    right_bucket,
                    self.row_scale,
                    other.row_scale,
                );
                cardinality = cardinality.add(contribution.cardinality);
                ndv = ndv.add(contribution.ndv);
                if let Some(bucket) = contribution.bucket {
                    output_buckets.push(bucket);
                }
            }
        }

        let histogram = (!output_buckets.is_empty()).then(|| {
            T::into_histogram(TypedHistogram {
                accuracy: self.accuracy && other.accuracy,
                row_scale: 1.0,
                buckets: output_buckets,
                avg_spacing: self.avg_spacing.or(other.avg_spacing),
            })
        });

        JoinEstimation {
            cardinality,
            ndv,
            histogram,
        }
    }
}

struct JoinContribution<T> {
    cardinality: StatEstimate,
    ndv: StatEstimate,
    bucket: Option<TypedHistogramBucket<T>>,
}

impl<T> JoinContribution<T> {
    fn zero() -> Self {
        Self {
            cardinality: StatEstimate::exact(0.0),
            ndv: StatEstimate::exact(0.0),
            bucket: None,
        }
    }
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

        let (Some(lower_bound), Some(upper_bound)) = self.bounds().intersection(&other.bounds())
        else {
            return JoinContribution::zero();
        };
        if self.is_singleton_value()
            && other.is_singleton_value()
            && T::compare(self.lower_bound(), other.lower_bound()) == Ordering::Equal
        {
            let cardinality =
                self.num_values() * left_row_scale * other.num_values() * right_row_scale;
            return JoinContribution {
                cardinality: StatEstimate::exact(cardinality),
                ndv: StatEstimate::exact(1.0),
                bucket: Some(TypedHistogramBucket::new(
                    lower_bound.clone(),
                    upper_bound.clone(),
                    cardinality,
                    1.0,
                )),
            };
        }

        let (expected_cardinality, expected_ndv) = T::estimate_overlap_coverages(self, other)
            .and_then(|coverage| {
                self.estimate_expected_join_counts(other, coverage, left_row_scale, right_row_scale)
            })
            .unwrap_or((0.0, 0.0));

        let upper_ndv = match intersection {
            Intersection::None => 0.0,
            Intersection::Point => 1.0,
            Intersection::Range => self.num_distinct().min(other.num_distinct()),
        };

        let upper_cardinality =
            self.num_values() * left_row_scale * other.num_values() * right_row_scale;
        debug_assert!(
            expected_cardinality <= upper_cardinality,
            "join expected cardinality exceeds cartesian upper: {expected_cardinality:?} > {upper_cardinality:?}"
        );
        debug_assert!(
            expected_ndv <= upper_ndv,
            "join expected ndv exceeds intersection upper: {expected_ndv:?} > {upper_ndv:?}"
        );

        let bucket = (expected_cardinality > 0.0 && expected_ndv > 0.0).then(|| {
            TypedHistogramBucket::new(
                lower_bound.clone(),
                upper_bound.clone(),
                expected_cardinality.max(expected_ndv),
                expected_ndv,
            )
        });

        JoinContribution {
            cardinality: StatEstimate::new(0.0, expected_cardinality, upper_cardinality),
            ndv: StatEstimate::new(0.0, expected_ndv, upper_ndv),
            bucket,
        }
    }

    fn estimate_expected_join_counts(
        &self,
        other: &TypedHistogramBucket<T>,
        coverage: OverlapCoverage,
        left_row_scale: f64,
        right_row_scale: f64,
    ) -> Option<(f64, f64)> {
        let left_num_rows = self.num_values() * left_row_scale * coverage.left;
        let left_ndv = self.num_distinct() * coverage.left;
        let right_num_rows = other.num_values() * right_row_scale * coverage.right;
        let right_ndv = other.num_distinct() * coverage.right;
        let max_ndv = left_ndv.max(right_ndv);
        if max_ndv <= 0.0 {
            return None;
        }
        // The equality denominator is a value count. Fractional NDV estimates
        // below one would otherwise produce more rows than the cartesian upper.
        let effective_max_ndv = if max_ndv < 1.0 { 1.0 } else { max_ndv };

        Some((
            left_num_rows * right_num_rows / effective_max_ndv,
            left_ndv.min(right_ndv),
        ))
    }
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
            ndv: StatEstimate::new(0.0, 1.0, 1.0),
            histogram: Some(crate::Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![TypedHistogramBucket::new(10, 10, 1.0, 1.0)],
                avg_spacing: None,
            })),
        });
    }

    #[test]
    fn test_typed_histogram_estimate_join_is_exact_for_singleton_buckets() {
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
            ndv: StatEstimate::exact(1.0),
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
        assert_eq!(estimation.ndv, StatEstimate::exact(1.0));
        let histogram = estimation
            .histogram
            .expect("singleton overlap should produce output histogram");
        assert_eq!(histogram.num_values(), 6.0);
        assert_eq!(histogram.ndv().expected, 1.0);
    }

    #[test]
    fn test_typed_histogram_estimate_join_builds_output_buckets_from_contributions() {
        let left = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(1_u64, 1_u64, 2.0, 1.0),
                TypedHistogramBucket::new(3_u64, 3_u64, 4.0, 1.0),
            ],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            row_scale: 1.0,
            buckets: vec![
                TypedHistogramBucket::new(1_u64, 1_u64, 5.0, 1.0),
                TypedHistogramBucket::new(2_u64, 2_u64, 7.0, 1.0),
                TypedHistogramBucket::new(3_u64, 3_u64, 6.0, 1.0),
            ],
            avg_spacing: None,
        };

        assert_eq!(left.estimate_join(&right), JoinEstimation {
            cardinality: StatEstimate::exact(34.0),
            ndv: StatEstimate::exact(2.0),
            histogram: Some(crate::Histogram::UInt(TypedHistogram {
                accuracy: true,
                row_scale: 1.0,
                buckets: vec![
                    TypedHistogramBucket::new(1, 1, 10.0, 1.0),
                    TypedHistogramBucket::new(3, 3, 24.0, 1.0),
                ],
                avg_spacing: None,
            })),
        });
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
            ndv: StatEstimate::exact(1.0),
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

        let raw_expected = left.buckets[0].num_values() * right.buckets[0].num_values()
            / left.buckets[0].num_distinct();
        let cartesian_upper = left.buckets[0].num_values() * right.buckets[0].num_values();
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
}
