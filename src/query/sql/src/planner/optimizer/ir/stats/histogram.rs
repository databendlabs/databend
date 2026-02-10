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
use databend_common_exception::Result;
use databend_common_expression::arithmetics_type::ResultTypeOfUnary;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_common_statistics::HistogramBucket;

pub type F64 = OrderedFloat<f64>;

/// Builder for creating histograms from statistical information
pub struct HistogramBuilder;

impl HistogramBuilder {
    /// Construct a histogram from NDV and total number of rows.
    ///
    /// # Arguments
    ///  * `ndv` - number of distinct values
    ///  * `num_rows` - total number of rows
    ///  * `bound` - min and max value of the column
    ///  * `num_buckets` - number of buckets
    pub fn from_ndv(
        ndv: u64,
        num_rows: u64,
        bound: Option<(Datum, Datum)>,
        num_buckets: usize,
    ) -> std::result::Result<Histogram, String> {
        // Handle special case for empty or very small datasets
        if ndv <= 2 {
            return if num_rows != 0 {
                Err(format!(
                    "NDV must be greater than 0 when the number of rows is greater than 0, got NDV: {}, num_rows: {}",
                    ndv, num_rows
                ))
            } else {
                Ok(Histogram {
                    buckets: vec![],
                    accuracy: false,
                })
            };
        }

        // Validate input parameters
        Self::validate_inputs(ndv, num_rows, num_buckets)?;

        // Extract min and max values
        let (min, max) = match bound {
            Some((min, max)) => (min.cast_float(), max.cast_float()),
            None => {
                return Err(format!(
                    "Must have min and max value when NDV is greater than 0, got NDV: {}",
                    ndv
                ));
            }
        };

        // Adjust number of buckets if needed
        let adjusted_num_buckets = if num_buckets > ndv as usize {
            ndv as usize
        } else {
            num_buckets
        };

        // Create uniform sample set and generate buckets
        let sample_set = UniformSampleSet::new(min, max);
        let buckets = Self::generate_buckets(&sample_set, adjusted_num_buckets, ndv, num_rows)?;

        Ok(Histogram {
            buckets,
            accuracy: false,
        })
    }

    /// Validate input parameters for histogram creation
    fn validate_inputs(
        ndv: u64,
        num_rows: u64,
        num_buckets: usize,
    ) -> std::result::Result<(), String> {
        if num_buckets < 2 {
            return Err(format!("Must have at least 2 buckets, got {}", num_buckets));
        }

        if ndv > num_rows {
            return Err(format!(
                "NDV must be less than or equal to the number of rows, got NDV: {}, num_rows: {}",
                ndv, num_rows
            ));
        }

        Ok(())
    }

    /// Generate histogram buckets based on the sample set
    fn generate_buckets(
        sample_set: &UniformSampleSet,
        num_buckets: usize,
        ndv: u64,
        num_rows: u64,
    ) -> std::result::Result<Vec<HistogramBucket>, String> {
        let mut buckets: Vec<HistogramBucket> = Vec::with_capacity(num_buckets);

        for idx in 0..num_buckets {
            let lower_bound = if idx == 0 {
                sample_set.min().clone()
            } else {
                buckets[idx - 1].upper_bound().clone()
            };

            let upper_bound = sample_set.get_upper_bound(num_buckets, idx + 1)?;

            let bucket = HistogramBucket::new(
                lower_bound,
                upper_bound,
                (num_rows / num_buckets as u64) as f64,
                (ndv / num_buckets as u64) as f64,
            );

            buckets.push(bucket);
        }

        Ok(buckets)
    }
}

/// Trait defining methods for sample sets used in histogram generation
pub trait SampleSet {
    /// Get the upper bound for a specific bucket
    fn get_upper_bound(
        &self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> std::result::Result<Datum, String>;
}

/// A sample set with uniformly distributed values
pub struct UniformSampleSet {
    min: Datum,
    max: Datum,
}

impl UniformSampleSet {
    /// Create a new uniform sample set with given min and max values
    pub fn new(min: Datum, max: Datum) -> Self {
        UniformSampleSet { min, max }
    }

    /// Get the minimum value of the sample set
    pub fn min(&self) -> &Datum {
        &self.min
    }

    /// Get the maximum value of the sample set
    pub fn max(&self) -> &Datum {
        &self.max
    }

    /// Check if two UniformSampleSet have intersection
    pub fn has_intersection(&self, other: &UniformSampleSet) -> Result<bool> {
        // If data type is string, we don't need to compare, just return true
        if let Datum::Bytes(_) = self.min {
            return Ok(true);
        }
        let r1 = self.min.compare(&other.max)?;
        let r2 = self.max.compare(&other.min)?;
        Ok(!matches!(
            (r1, r2),
            (Ordering::Greater, _) | (_, Ordering::Less)
        ))
    }

    /// Calculate the intersection of two sample sets
    pub fn intersection(&self, other: &UniformSampleSet) -> Result<(Option<Datum>, Option<Datum>)> {
        match (&self.min, &other.min) {
            (Datum::Bytes(_), Datum::Bytes(_)) => Ok((None, None)),
            _ => {
                let left_min = self.min.as_double()?;
                let left_max = self.max.as_double()?;
                let right_min = other.min.as_double()?;
                let right_max = other.max.as_double()?;
                let new_min = if left_min <= right_min {
                    other.min.clone()
                } else {
                    self.min.clone()
                };
                let new_max = if left_max >= right_max {
                    other.max.clone()
                } else {
                    self.max.clone()
                };
                Ok((Some(new_min), Some(new_max)))
            }
        }
    }
}

impl SampleSet for UniformSampleSet {
    fn get_upper_bound(
        &self,
        num_buckets: usize,
        bucket_index: usize,
    ) -> std::result::Result<Datum, String> {
        match (&self.min, &self.max) {
            (Datum::Int(min), Datum::Int(max)) => {
                let min = *min;
                let max = *max;
                // TODO(xudong): better histogram computation.
                let bucket_range = max.saturating_add(1).checked_sub(min).ok_or("overflowed")?
                    / num_buckets as i64;
                let upper_bound = min + bucket_range * bucket_index as i64;
                Ok(Datum::Int(upper_bound))
            }

            (Datum::UInt(min), Datum::UInt(max)) => {
                let min = *min;
                let max = *max;
                let bucket_range = (max.saturating_add(1) - min) / num_buckets as u64;
                let upper_bound = min + bucket_range * bucket_index as u64;
                Ok(Datum::UInt(upper_bound))
            }

            (Datum::Float(min), Datum::Float(max)) => {
                let min = *min;
                let max = (*max).checked_add(F64::from(1.0)).ok_or("overflowed")?;
                // TODO(xudong): better histogram computation.
                let bucket_range = max.checked_sub(min).ok_or("overflowed")? / num_buckets as f64;
                let upper_bound = min + bucket_range * bucket_index as f64;
                Ok(Datum::Float(upper_bound))
            }

            // Handle Bytes type for histogram calculation by converting to strings first
            (Datum::Bytes(min_bytes), Datum::Bytes(max_bytes)) => {
                // Convert bytes to strings for comparison
                let min_str = String::from_utf8_lossy(min_bytes);
                let max_str = String::from_utf8_lossy(max_bytes);

                // For boundary cases, return the exact values
                if min_str == max_str {
                    return Ok(Datum::Bytes(min_bytes.clone()));
                }

                if bucket_index == 0 {
                    return Ok(Datum::Bytes(min_bytes.clone()));
                } else if bucket_index >= num_buckets {
                    return Ok(Datum::Bytes(max_bytes.clone()));
                }

                // For intermediate buckets, use a simple approach based on string comparison
                // Just divide the range into equal parts based on bucket_index

                // If bucket_index is in the first half, return min
                // If bucket_index is in the second half, return max
                // This preserves the string ordering semantics
                let mid_bucket = num_buckets / 2;

                if bucket_index <= mid_bucket {
                    Ok(Datum::Bytes(min_bytes.clone()))
                } else {
                    Ok(Datum::Bytes(max_bytes.clone()))
                }
            }

            _ => Err(format!(
                "Unsupported datum type for histogram calculation: {} (type: {}), {} (type: {}). Only numeric types are supported.",
                self.min,
                self.min.type_name(),
                self.max,
                self.max.type_name()
            )),
        }
    }
}
