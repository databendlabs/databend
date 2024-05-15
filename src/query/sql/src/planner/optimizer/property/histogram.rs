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
use std::fmt;
use std::fmt::Debug;

use databend_common_exception::Result;
use databend_common_expression::arithmetics_type::ResultTypeOfUnary;
use databend_common_storage::Datum;

pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 100;

/// A histogram is a representation of the distribution of a column.
///
/// We are constructing this in an "Equi-height" fashion, which means
/// every bucket has roughly the same number of rows.
///
/// Real-world data distribution is often skewed,
/// so an equal-height histogram is better than an equal-width histogram,
/// the former can use multiple buckets to show the skew data, but for the latter,
/// it is difficult to give the exact frequency of the skew data
/// when the skew data and other data fall into the same bucket
///
/// We choose this approach because so far the histogram is originally
/// constructed from NDV(number of distinct values) and the total number
/// of rows instead of maintaining a real histogram for each column,
/// which brings the assumption that the data is uniformly distributed.
#[derive(Debug, Clone)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

impl Histogram {
    pub fn new(buckets: Vec<HistogramBucket>) -> Self {
        Self { buckets }
    }

    /// Get number of buckets
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Get number of values
    pub fn num_values(&self) -> f64 {
        self.buckets
            .iter()
            .fold(0.0, |acc, bucket| acc + bucket.num_values())
    }

    /// Get number of distinct values
    /// TODO(leiysky): this is not accurate, find a better way to calculate NDV
    pub fn num_distinct_values(&self) -> f64 {
        self.buckets
            .iter()
            .fold(0.0, |acc, bucket| acc + bucket.num_distinct())
    }

    /// Get iterator of buckets
    pub fn buckets_iter(&self) -> impl DoubleEndedIterator<Item = &HistogramBucket> {
        self.buckets.iter()
    }
}

/// Construct a histogram from NDV and total number of rows.
///
/// # Arguments
///  * `ndv` - number of distinct values
///  * `num_rows` - total number of rows
///  * `bound` - min and max value of the column
///  * `buckets` - number of buckets
pub fn histogram_from_ndv(
    ndv: u64,
    num_rows: u64,
    bound: Option<(Datum, Datum)>,
    num_buckets: usize,
) -> Result<Histogram, String> {
    if ndv <= 2 {
        return if num_rows != 0 {
            Err(format!(
                "NDV must be greater than 0 when the number of rows is greater than 0, got NDV: {}, num_rows: {}",
                ndv, num_rows
            ))
        } else {
            Ok(Histogram { buckets: vec![] })
        };
    }

    if num_buckets < 2 {
        return Err(format!("Must have at least 2 buckets, got {}", num_buckets));
    }

    if ndv > num_rows {
        return Err(format!(
            "NDV must be less than or equal to the number of rows, got NDV: {}, num_rows: {}",
            ndv, num_rows
        ));
    }

    let (min, max) = match bound {
        Some((min, max)) => (min, max),
        None => {
            return Err(format!(
                "Must have min and max value when NDV is greater than 0, got NDV: {}",
                ndv
            ));
        }
    };

    let num_buckets = if num_buckets > ndv as usize {
        ndv as usize
    } else {
        num_buckets
    };

    let mut buckets: Vec<HistogramBucket> = Vec::with_capacity(num_buckets);
    let sample_set = UniformSampleSet { min, max };

    for idx in 0..num_buckets + 1 {
        let upper_bound = sample_set.get_upper_bound(num_buckets, idx)?;
        if idx == 0 {
            // The first bucket is a dummy bucket
            // which is used to record the min value of the column
            // So we don't need to record the min value for each bucket
            buckets.push(HistogramBucket {
                upper_bound,
                num_values: 1.0,
                num_distinct: 1.0,
            });
            continue;
        }
        let bucket = HistogramBucket {
            upper_bound,
            num_values: (num_rows / num_buckets as u64) as f64,
            num_distinct: (ndv / num_buckets as u64) as f64,
        };
        buckets.push(bucket);
    }

    Ok(Histogram { buckets })
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Upper bound value of the bucket.
    upper_bound: Datum,
    /// Estimated number of values in the bucket.
    num_values: f64,
    /// Estimated number of distinct values in the bucket.
    num_distinct: f64,
}

impl HistogramBucket {
    pub fn new(upper_bound: Datum, num_values: f64, num_distinct: f64) -> Self {
        Self {
            upper_bound,
            num_values,
            num_distinct,
        }
    }

    pub fn upper_bound(&self) -> &Datum {
        &self.upper_bound
    }

    pub fn num_values(&self) -> f64 {
        self.num_values
    }

    pub fn num_distinct(&self) -> f64 {
        self.num_distinct
    }

    pub fn aggregate_values(&mut self) {
        self.num_values = self.num_distinct;
    }

    pub fn update(&mut self, selectivity: f64) {
        self.num_values *= selectivity;
        self.num_distinct *= selectivity
    }
}

trait SampleSet {
    fn get_upper_bound(&self, num_buckets: usize, bucket_index: usize) -> Result<Datum, String>;
}

pub struct UniformSampleSet {
    min: Datum,
    max: Datum,
}

impl UniformSampleSet {
    pub fn new(min: Datum, max: Datum) -> Self {
        UniformSampleSet { min, max }
    }

    // Check if two `UniformSampleSet` have intersection
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

    pub fn intersection(&self, other: &UniformSampleSet) -> Result<(Option<Datum>, Option<Datum>)> {
        match (&self.min, &other.min) {
            (Datum::Bytes(_), Datum::Bytes(_)) => Ok((None, None)),
            _ => {
                let left_min = self.min.to_double()?;
                let left_max = self.max.to_double()?;
                let right_min = other.min.to_double()?;
                let right_max = other.max.to_double()?;
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
    fn get_upper_bound(&self, num_buckets: usize, bucket_index: usize) -> Result<Datum, String> {
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
                let max = *max;
                // TODO(xudong): better histogram computation.
                let bucket_range = max.checked_sub(min).ok_or("overflowed")? / num_buckets as f64;
                let upper_bound = min + bucket_range * bucket_index as f64;
                Ok(Datum::Float(upper_bound))
            }

            _ => Err(format!(
                "Unsupported datum type: {:?}, {:?}",
                self.min, self.max
            )),
        }
    }
}

pub struct InterleavedBucket {
    pub left_ndv: f64,
    pub right_ndv: f64,
    pub left_num_rows: f64,
    pub right_num_rows: f64,
    pub max_val: f64,
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for bucket in &self.buckets {
            writeln!(
                f,
                "{}: {} values, {} distinct values",
                bucket.upper_bound(),
                bucket.num_values,
                bucket.num_distinct
            )?;
        }
        Ok(())
    }
}
