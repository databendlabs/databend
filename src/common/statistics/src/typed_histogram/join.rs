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
use crate::StatEstimate;

#[derive(Debug, Clone, PartialEq)]
pub struct JoinEstimation {
    pub cardinality: StatEstimate,
    pub ndv: StatEstimate,
}

impl JoinEstimation {
    pub fn zero() -> Self {
        Self {
            cardinality: StatEstimate::exact(0.0),
            ndv: StatEstimate::exact(0.0),
        }
    }

    pub(super) fn add(self, other: Self) -> Self {
        Self {
            cardinality: self.cardinality.add(other.cardinality),
            ndv: self.ndv.add(other.ndv),
        }
    }
}

impl<T: Value> TypedHistogram<T> {
    pub fn estimate_join(&self, other: &TypedHistogram<T>) -> JoinEstimation {
        let mut estimation = JoinEstimation::zero();

        for left_bucket in &self.buckets {
            for right_bucket in &other.buckets {
                estimation = estimation.add(left_bucket.estimate_join_contribution(right_bucket));
            }
        }

        estimation
    }
}

impl<T: Value> TypedHistogramBucket<T> {
    fn estimate_join_contribution(&self, other: &TypedHistogramBucket<T>) -> JoinEstimation {
        let intersection = self.intersection_kind(other);
        if intersection == Intersection::None {
            return JoinEstimation::zero();
        }

        if self.is_singleton_value()
            && other.is_singleton_value()
            && T::compare(self.lower_bound(), other.lower_bound()) == Ordering::Equal
        {
            return JoinEstimation {
                cardinality: StatEstimate::exact(self.num_values() * other.num_values()),
                ndv: StatEstimate::exact(1.0),
            };
        }

        let expected = T::estimate_overlap_coverages(self, other)
            .and_then(|coverage| self.build_expected_join_estimation(other, coverage))
            .unwrap_or_else(JoinEstimation::zero);

        let upper_ndv = match intersection {
            Intersection::None => 0.0,
            Intersection::Point => 1.0,
            Intersection::Range => self.num_distinct().min(other.num_distinct()),
        };

        let upper_cardinality = self.num_values() * other.num_values();
        let expected_cardinality = expected.cardinality.expected.min(upper_cardinality);
        let expected_ndv = expected.ndv.expected.min(upper_ndv);

        JoinEstimation {
            cardinality: StatEstimate::new(0.0, expected_cardinality, upper_cardinality),
            ndv: StatEstimate::new(0.0, expected_ndv, upper_ndv),
        }
    }

    fn build_expected_join_estimation(
        &self,
        other: &TypedHistogramBucket<T>,
        coverage: OverlapCoverage,
    ) -> Option<JoinEstimation> {
        let left_num_rows = self.num_values() * coverage.left;
        let left_ndv = self.num_distinct() * coverage.left;
        let right_num_rows = other.num_values() * coverage.right;
        let right_ndv = other.num_distinct() * coverage.right;
        let max_ndv = left_ndv.max(right_ndv);
        if max_ndv <= 0.0 {
            return None;
        }

        Some(JoinEstimation {
            cardinality: StatEstimate::exact(left_num_rows * right_num_rows / max_ndv),
            ndv: StatEstimate::exact(left_ndv.min(right_ndv)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typed_histogram_estimate_join_keeps_point_overlap() {
        let left = TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(0_u64, 10_u64, 10.0, 10.0)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(10_u64, 20_u64, 10.0, 10.0)],
            avg_spacing: None,
        };

        assert_eq!(left.estimate_join(&right), JoinEstimation {
            cardinality: StatEstimate::new(0.0, 1.0, 100.0),
            ndv: StatEstimate::new(0.0, 1.0, 1.0),
        });
    }

    #[test]
    fn test_typed_histogram_estimate_join_is_exact_for_singleton_buckets() {
        let left = TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(10_i64, 10_i64, 4.0, 1.0)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(10_i64, 10_i64, 3.0, 1.0)],
            avg_spacing: None,
        };

        assert_eq!(left.estimate_join(&right), JoinEstimation {
            cardinality: StatEstimate::exact(12.0),
            ndv: StatEstimate::exact(1.0),
        });
    }

    #[test]
    fn test_typed_histogram_estimate_join_caps_scaled_bucket_expected_count() {
        let left = TypedHistogram {
            accuracy: true,
            buckets: vec![TypedHistogramBucket::new(0_i64, 10_i64, 0.984, 0.93)],
            avg_spacing: None,
        };
        let right = TypedHistogram {
            accuracy: true,
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
