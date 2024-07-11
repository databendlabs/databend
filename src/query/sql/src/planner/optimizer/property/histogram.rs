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
use databend_common_storage::Histogram;
use databend_common_storage::HistogramBucket;

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
            buckets.push(HistogramBucket::new(upper_bound, 1.0, 1.0));
            continue;
        }
        let bucket = HistogramBucket::new(
            upper_bound,
            (num_rows / num_buckets as u64) as f64,
            (ndv / num_buckets as u64) as f64,
        );
        buckets.push(bucket);
    }

    Ok(Histogram { buckets })
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
