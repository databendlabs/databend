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

use databend_common_base::base::OrderedFloat;

mod join;

pub use join::JoinEstimation;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TypedHistogramEstimate {
    pub lower: f64,
    pub expected: f64,
    pub upper: f64,
}

impl TypedHistogramEstimate {
    pub fn new(lower: f64, expected: f64, upper: f64) -> Self {
        let lower = lower.max(0.0);
        let upper = upper.max(lower);
        let expected = expected.clamp(lower, upper);
        Self {
            lower,
            expected,
            upper,
        }
    }

    pub fn exact(value: f64) -> Self {
        let value = value.max(0.0);
        Self {
            lower: value,
            expected: value,
            upper: value,
        }
    }

    fn add(self, other: Self) -> Self {
        Self::new(
            self.lower + other.lower,
            self.expected + other.expected,
            self.upper + other.upper,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedHistogram<T> {
    pub accuracy: bool,
    pub buckets: Vec<TypedHistogramBucket<T>>,
    pub avg_spacing: Option<f64>,
}

impl<T> TypedHistogram<T> {
    pub fn new(buckets: Vec<TypedHistogramBucket<T>>, accuracy: bool) -> Self {
        Self {
            accuracy,
            buckets,
            avg_spacing: None,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    pub fn num_values(&self) -> f64 {
        self.buckets
            .iter()
            .fold(0.0, |acc, bucket| acc + bucket.num_values)
    }

    pub fn num_distinct_values(&self) -> f64 {
        self.buckets
            .iter()
            .fold(0.0, |acc, bucket| acc + bucket.num_distinct)
    }

    pub fn buckets_iter(&self) -> std::slice::Iter<'_, TypedHistogramBucket<T>> {
        self.buckets.iter()
    }

    pub fn scale_counts(&mut self, selectivity: f64) {
        for bucket in &mut self.buckets {
            bucket.num_values *= selectivity;
            bucket.num_distinct *= selectivity;
        }
    }

    pub fn collapse_counts_to_distinct(&mut self) {
        for bucket in &mut self.buckets {
            bucket.num_values = bucket.num_distinct;
        }
    }

    pub fn is_range_distorted(&self) -> bool {
        self.avg_spacing
            .is_some_and(|bucket_width| bucket_width > 1e12)
    }
}

impl<T: Clone> TypedHistogram<T> {
    pub fn bounds(&self) -> Option<TypedHistogramBounds<T>> {
        let first = self.buckets.first()?;
        let last = self.buckets.last()?;
        Some(TypedHistogramBounds::new(
            first.lower_bound().clone(),
            last.upper_bound().clone(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedHistogramBucket<T> {
    lower_bound: T,
    upper_bound: T,
    num_values: f64,
    num_distinct: f64,
}

impl<T> TypedHistogramBucket<T> {
    pub fn new(lower_bound: T, upper_bound: T, num_values: f64, num_distinct: f64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            num_values,
            num_distinct,
        }
    }

    pub fn lower_bound(&self) -> &T {
        &self.lower_bound
    }

    pub fn upper_bound(&self) -> &T {
        &self.upper_bound
    }

    pub fn num_values(&self) -> f64 {
        self.num_values
    }

    pub fn num_distinct(&self) -> f64 {
        self.num_distinct
    }
}

impl<T: Clone> TypedHistogramBucket<T> {
    pub fn bounds(&self) -> TypedHistogramBounds<T> {
        TypedHistogramBounds::new(self.lower_bound.clone(), self.upper_bound.clone())
    }
}

impl<T: Value> TypedHistogramBucket<T> {
    fn intersection_kind(&self, other: &TypedHistogramBucket<T>) -> Intersection {
        let lower_bound = if matches!(
            T::compare(&self.lower_bound, &other.lower_bound),
            Ordering::Less
        ) {
            &other.lower_bound
        } else {
            &self.lower_bound
        };

        let upper_bound = if matches!(
            T::compare(&self.upper_bound, &other.upper_bound),
            Ordering::Greater
        ) {
            &other.upper_bound
        } else {
            &self.upper_bound
        };

        match T::compare(lower_bound, upper_bound) {
            Ordering::Greater => Intersection::None,
            Ordering::Equal => Intersection::Point,
            Ordering::Less => Intersection::Range,
        }
    }

    fn is_singleton_value(&self) -> bool {
        matches!(
            T::compare(self.lower_bound(), self.upper_bound()),
            Ordering::Equal
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedHistogramBounds<T> {
    lower_bound: T,
    upper_bound: T,
}

impl<T> TypedHistogramBounds<T> {
    pub fn new(lower_bound: T, upper_bound: T) -> Self {
        Self {
            lower_bound,
            upper_bound,
        }
    }

    pub fn lower_bound(&self) -> &T {
        &self.lower_bound
    }

    pub fn upper_bound(&self) -> &T {
        &self.upper_bound
    }
}

impl<T: Value> TypedHistogramBounds<T> {
    pub fn has_intersection(&self, other: &Self) -> bool {
        !matches!(
            (
                T::compare(self.lower_bound(), other.upper_bound()),
                T::compare(self.upper_bound(), other.lower_bound()),
            ),
            (Ordering::Greater, _) | (_, Ordering::Less)
        )
    }

    pub fn intersection(&self, other: &Self) -> (Option<T>, Option<T>) {
        if !self.has_intersection(other) {
            return (None, None);
        }

        let lower_bound = if matches!(
            T::compare(self.lower_bound(), other.lower_bound()),
            Ordering::Less
        ) {
            other.lower_bound().clone()
        } else {
            self.lower_bound().clone()
        };

        let upper_bound = if matches!(
            T::compare(self.upper_bound(), other.upper_bound()),
            Ordering::Greater
        ) {
            other.upper_bound().clone()
        } else {
            self.upper_bound().clone()
        };

        (Some(lower_bound), Some(upper_bound))
    }

    pub fn get_upper_bound(&self, num_buckets: usize, bucket_index: usize) -> Result<T, String> {
        T::bucket_upper_bound(
            self.lower_bound(),
            self.upper_bound(),
            num_buckets,
            bucket_index,
        )
    }
}

pub struct TypedHistogramBuilder;

impl TypedHistogramBuilder {
    pub fn from_ndv<T: Value>(
        ndv: u64,
        num_rows: u64,
        bounds: Option<(T, T)>,
        num_buckets: usize,
    ) -> Result<TypedHistogram<T>, String> {
        if ndv <= 2 {
            return if num_rows != 0 {
                Err(format!(
                    "NDV must be greater than 0 when the number of rows is greater than 0, got NDV: {ndv}, num_rows: {num_rows}"
                ))
            } else {
                Ok(TypedHistogram {
                    accuracy: false,
                    buckets: vec![],
                    avg_spacing: None,
                })
            };
        }

        if num_buckets < 2 {
            return Err(format!("Must have at least 2 buckets, got {num_buckets}"));
        }

        if ndv > num_rows {
            return Err(format!(
                "NDV must be less than or equal to the number of rows, got NDV: {ndv}, num_rows: {num_rows}"
            ));
        }

        let (min, max) = match bounds {
            Some(bounds) => bounds,
            None => {
                return Err(format!(
                    "Must have min and max value when NDV is greater than 0, got NDV: {ndv}"
                ));
            }
        };

        let avg_spacing = T::avg_spacing(&min, &max, num_buckets);
        let adjusted_num_buckets = num_buckets.min(ndv as usize);
        let sample_set = TypedHistogramBounds::new(min, max);
        let mut buckets: Vec<TypedHistogramBucket<T>> = Vec::with_capacity(adjusted_num_buckets);

        for idx in 0..adjusted_num_buckets {
            let lower_bound = if idx == 0 {
                sample_set.lower_bound().clone()
            } else {
                buckets[idx - 1].upper_bound().clone()
            };

            let upper_bound = sample_set.get_upper_bound(adjusted_num_buckets, idx + 1)?;
            buckets.push(TypedHistogramBucket::new(
                lower_bound,
                upper_bound,
                (num_rows / adjusted_num_buckets as u64) as f64,
                (ndv / adjusted_num_buckets as u64) as f64,
            ));
        }

        Ok(TypedHistogram {
            accuracy: false,
            buckets,
            avg_spacing,
        })
    }
}

pub trait Value: Clone + PartialEq {
    fn compare(left: &Self, right: &Self) -> Ordering;

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String>;

    fn avg_spacing(min: &Self, max: &Self, num_buckets: usize) -> Option<f64> {
        let _ = (min, max, num_buckets);
        None
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage>;
}

trait NumericValue: Value {
    fn distance(start: &Self, end: &Self) -> Option<f64>;

    fn estimate_numeric_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        match left.intersection_kind(right) {
            Intersection::None => None,
            Intersection::Point => OverlapCoverage::point(left.num_distinct, right.num_distinct),
            Intersection::Range => {
                let left_bounds = left.bounds();
                let right_bounds = right.bounds();
                let (Some(lower_bound), Some(upper_bound)) =
                    left_bounds.intersection(&right_bounds)
                else {
                    return None;
                };

                let overlap_width = Self::distance(&lower_bound, &upper_bound)?;
                let left_width = Self::distance(left.lower_bound(), left.upper_bound())?;
                let right_width = Self::distance(right.lower_bound(), right.upper_bound())?;
                if overlap_width <= 0.0 || left_width <= 0.0 || right_width <= 0.0 {
                    return None;
                }

                Some({
                    OverlapCoverage {
                        left: (overlap_width / left_width).clamp(0.0, 1.0),
                        right: (overlap_width / right_width).clamp(0.0, 1.0),
                    }
                })
            }
        }
    }
}

impl Value for u64 {
    fn compare(left: &Self, right: &Self) -> Ordering {
        left.cmp(right)
    }

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String> {
        let bucket_range = (max.saturating_add(1) - min) / num_buckets as u64;
        Ok(min + bucket_range * bucket_index as u64)
    }

    fn avg_spacing(min: &Self, max: &Self, num_buckets: usize) -> Option<f64> {
        (max > min && num_buckets > 0).then(|| (*max - *min) as f64 / num_buckets as f64)
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        Self::estimate_numeric_overlap_coverages(left, right)
    }
}

impl NumericValue for u64 {
    fn distance(start: &Self, end: &Self) -> Option<f64> {
        end.checked_sub(*start).map(|distance| distance as f64)
    }
}

impl Value for i64 {
    fn compare(left: &Self, right: &Self) -> Ordering {
        left.cmp(right)
    }

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String> {
        let bucket_range = max
            .saturating_add(1)
            .checked_sub(*min)
            .ok_or("overflowed")?
            / num_buckets as i64;
        Ok(min + bucket_range * bucket_index as i64)
    }

    fn avg_spacing(min: &Self, max: &Self, num_buckets: usize) -> Option<f64> {
        (max > min && num_buckets > 0)
            .then(|| {
                max.checked_sub(*min)
                    .map(|range| range as f64 / num_buckets as f64)
            })
            .flatten()
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        Self::estimate_numeric_overlap_coverages(left, right)
    }
}

impl NumericValue for i64 {
    fn distance(start: &Self, end: &Self) -> Option<f64> {
        end.checked_sub(*start).map(|distance| distance as f64)
    }
}

impl Value for f64 {
    fn compare(left: &Self, right: &Self) -> Ordering {
        left.total_cmp(right)
    }

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String> {
        let max = *max + 1.0;
        let bucket_range = (max - min) / num_buckets as f64;
        Ok(min + bucket_range * bucket_index as f64)
    }

    fn avg_spacing(min: &Self, max: &Self, num_buckets: usize) -> Option<f64> {
        (*max > *min && num_buckets > 0).then(|| (*max - *min) / num_buckets as f64)
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        Self::estimate_numeric_overlap_coverages(left, right)
    }
}

impl NumericValue for f64 {
    fn distance(start: &Self, end: &Self) -> Option<f64> {
        Some(end - start)
    }
}

impl Value for OrderedFloat<f64> {
    fn compare(left: &Self, right: &Self) -> Ordering {
        left.cmp(right)
    }

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String> {
        let min = min.into_inner();
        let max = max.into_inner() + 1.0;
        let bucket_range = (max - min) / num_buckets as f64;
        Ok(OrderedFloat(min + bucket_range * bucket_index as f64))
    }

    fn avg_spacing(min: &Self, max: &Self, num_buckets: usize) -> Option<f64> {
        (max > min && num_buckets > 0)
            .then(|| (max.into_inner() - min.into_inner()) / num_buckets as f64)
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        Self::estimate_numeric_overlap_coverages(left, right)
    }
}

impl NumericValue for OrderedFloat<f64> {
    fn distance(start: &Self, end: &Self) -> Option<f64> {
        Some(end.into_inner() - start.into_inner())
    }
}

impl Value for String {
    fn compare(left: &Self, right: &Self) -> Ordering {
        left.cmp(right)
    }

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String> {
        if min == max {
            return Ok(min.clone());
        }

        if bucket_index == 0 {
            Ok(min.clone())
        } else if bucket_index >= num_buckets {
            Ok(max.clone())
        } else {
            let mid_bucket = num_buckets / 2;
            if bucket_index <= mid_bucket {
                Ok(min.clone())
            } else {
                Ok(max.clone())
            }
        }
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        match left.intersection_kind(right) {
            Intersection::None => None,
            Intersection::Point => OverlapCoverage::point(left.num_distinct, right.num_distinct),
            Intersection::Range => Some(OverlapCoverage {
                left: 1.0,
                right: 1.0,
            }),
        }
    }
}

impl Value for Vec<u8> {
    fn compare(left: &Self, right: &Self) -> Ordering {
        left.cmp(right)
    }

    fn bucket_upper_bound(
        min: &Self,
        max: &Self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> Result<Self, String> {
        if min == max {
            return Ok(min.clone());
        }

        if bucket_index == 0 {
            Ok(min.clone())
        } else if bucket_index >= num_buckets {
            Ok(max.clone())
        } else {
            let mid_bucket = num_buckets / 2;
            if bucket_index <= mid_bucket {
                Ok(min.clone())
            } else {
                Ok(max.clone())
            }
        }
    }

    fn estimate_overlap_coverages(
        left: &TypedHistogramBucket<Self>,
        right: &TypedHistogramBucket<Self>,
    ) -> Option<OverlapCoverage> {
        match left.intersection_kind(right) {
            Intersection::None => None,
            Intersection::Point => {
                OverlapCoverage::point(left.num_distinct(), right.num_distinct())
            }
            Intersection::Range => Some(OverlapCoverage {
                left: 1.0,
                right: 1.0,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Intersection {
    None,
    Point,
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OverlapCoverage {
    left: f64,
    right: f64,
}

impl OverlapCoverage {
    fn point(left_ndv: f64, right_ndv: f64) -> Option<Self> {
        if left_ndv <= 0.0 || right_ndv <= 0.0 {
            return None;
        }

        Some(Self {
            left: (1.0 / left_ndv).clamp(0.0, 1.0),
            right: (1.0 / right_ndv).clamp(0.0, 1.0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_typed_histogram_bucket_bounds_keep_same_type() {
        let bucket = TypedHistogramBucket::new(1_u64, 10_u64, 8.0, 5.0);

        assert_eq!(bucket.bounds(), TypedHistogramBounds::new(1_u64, 10_u64));
    }

    #[test]
    fn test_typed_histogram_bounds_use_bucket_type() {
        let histogram = TypedHistogram::new(
            vec![
                TypedHistogramBucket::new("a".to_string(), "m".to_string(), 3.0, 2.0),
                TypedHistogramBucket::new("m".to_string(), "z".to_string(), 4.0, 3.0),
            ],
            true,
        );

        assert_eq!(
            histogram.bounds(),
            Some(TypedHistogramBounds::new("a".to_string(), "z".to_string()))
        );
    }

    #[test]
    fn test_typed_histogram_builder_and_intersection() {
        let histogram = TypedHistogramBuilder::from_ndv(8, 16, Some((0_u64, 80_u64)), 4).unwrap();
        let left = TypedHistogramBounds::new(0_u64, 10_u64);
        let right = TypedHistogramBounds::new(5_u64, 15_u64);

        assert!(!histogram.accuracy);
        assert_eq!(histogram.num_buckets(), 4);
        assert_eq!(histogram.avg_spacing, Some(20.0));
        assert!(left.has_intersection(&right));
        assert_eq!(left.intersection(&right), (Some(5_u64), Some(10_u64)));
    }

    #[test]
    fn test_typed_histogram_bytes_bounds_use_closed_interval_semantics() {
        let left = TypedHistogramBounds::new(b"a".to_vec(), b"c".to_vec());
        let right = TypedHistogramBounds::new(b"x".to_vec(), b"z".to_vec());

        assert!(!left.has_intersection(&right));
        assert_eq!(left.intersection(&right), (None, None));
    }
}
