// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;

use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 100;

pub trait Datum: Debug + Clone + PartialEq + Eq {
    type Type: Debug + Clone + PartialEq + Eq;

    fn data_type(&self) -> Self::Type;
}

impl Datum for DataValue {
    type Type = DataTypeImpl;

    fn data_type(&self) -> Self::Type {
        self.data_type()
    }
}

/// A histogram is a representation of the distribution of a column.
///
/// We are constructing this in an "Equi-depth" fashion, which means
/// every bucket has roughly the same number of rows.
///
/// We choose this approach because so far the histogram is originally
/// constructed from NDV(number of distinct values) and the total number
/// of rows instead of maintaining a real histogram for each column,
/// which brings the assumption that the data is uniformly distributed.
#[derive(Debug, Clone)]
pub struct Histogram<DatumType: Datum = DataValue> {
    pub buckets: Vec<HistogramBucket<DatumType>>,
}

impl<DatumType: Datum> Histogram<DatumType> {
    pub fn new(buckets: Vec<HistogramBucket<DatumType>>) -> Self {
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
            .fold(0.0, |acc, bucket| acc + bucket.num_values)
    }

    /// Get number of distinct values
    /// TODO(leiysky): this is not accurate, find a better way to calculate NDV
    pub fn num_distinct_values(&self) -> f64 {
        self.buckets
            .iter()
            .fold(0.0, |acc, bucket| acc + bucket.num_distinct)
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
    bound: Option<(DataValue, DataValue)>,
    num_buckets: usize,
) -> Result<Histogram<DataValue>> {
    if ndv == 0 {
        if num_rows != 0 {
            return Err(ErrorCode::Internal(format!(
                "NDV must be greater than 0 when the number of rows is greater than 0, got NDV: {}, num_rows: {}",
                ndv, num_rows
            )));
        } else {
            return Ok(Histogram::<DataValue> { buckets: vec![] });
        }
    }

    if num_buckets < 2 {
        return Err(ErrorCode::Internal(format!(
            "Must have at least 2 buckets, got {}",
            num_buckets
        )));
    }

    if ndv > num_rows {
        return Err(ErrorCode::Internal(format!(
            "NDV must be less than or equal to the number of rows, got NDV: {}, num_rows: {}",
            ndv, num_rows
        )));
    }

    let (min, max) = match bound {
        Some((min, max)) => (min, max),
        None => {
            return Err(ErrorCode::Internal(format!(
                "Must have min and max value when NDV is greater than 0, got NDV: {}",
                ndv
            )));
        }
    };

    let num_buckets = if num_buckets > ndv as usize {
        ndv as usize
    } else {
        num_buckets
    };

    let mut buckets: Vec<HistogramBucket<DataValue>> = Vec::with_capacity(num_buckets);
    let sample_set = UniformSampleSet::<DataValue> { min, max };

    for idx in 0..num_buckets {
        let upper_bound = sample_set.get_upper_bound(num_buckets, idx)?;
        let bucket = HistogramBucket::<DataValue> {
            upper_bound,
            num_values: (num_rows / num_buckets as u64) as f64,
            num_distinct: (ndv / num_buckets as u64) as f64,
        };
        buckets.push(bucket);
    }

    Ok(Histogram::<DataValue> { buckets })
}

#[derive(Debug, Clone)]
pub struct HistogramBucket<DatumType: Datum = DataValue> {
    /// Upper bound value of the bucket.
    upper_bound: DatumType,
    /// Estimated number of values in the bucket.
    num_values: f64,
    /// Estimated number of distinct values in the bucket.
    num_distinct: f64,
}

impl<DatumType: Datum> HistogramBucket<DatumType> {
    pub fn new(upper_bound: DatumType, num_values: f64, num_distinct: f64) -> Self {
        Self {
            upper_bound,
            num_values,
            num_distinct,
        }
    }

    #[allow(dead_code)]
    fn upper_bound(&self) -> &DatumType {
        &self.upper_bound
    }

    #[allow(dead_code)]
    fn num_values(&self) -> f64 {
        self.num_values
    }

    #[allow(dead_code)]
    fn num_distinct(&self) -> f64 {
        self.num_distinct
    }
}

trait SampleSet<DatumType: Datum> {
    fn get_upper_bound(&self, num_buckets: usize, bucket_index: usize) -> Result<DatumType>;
}

struct UniformSampleSet<DatumType: Datum> {
    min: DatumType,
    max: DatumType,
}

impl SampleSet<DataValue> for UniformSampleSet<DataValue> {
    fn get_upper_bound(&self, num_buckets: usize, bucket_index: usize) -> Result<DataValue> {
        match (&self.min, &self.max) {
            (DataValue::UInt64(min), DataValue::UInt64(max)) => {
                let min = *min;
                let max = *max;
                let bucket_range = (max - min) / num_buckets as u64;
                let upper_bound = match bucket_index {
                    0 => min,
                    _ if bucket_index == num_buckets - 1 => max,
                    _ => min + bucket_range * bucket_index as u64,
                };
                Ok(DataValue::UInt64(upper_bound))
            }
            _ => Err(ErrorCode::Unimplemented(format!(
                "Unsupported data type: {:?}",
                self.min.data_type()
            ))),
        }
    }
}
