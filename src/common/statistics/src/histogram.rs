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
use std::fmt::Debug;

use crate::Datum;

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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Histogram {
    pub accuracy: bool,
    pub buckets: Vec<HistogramBucket>,
}

impl Histogram {
    pub fn new(buckets: Vec<HistogramBucket>, accuracy: bool) -> Self {
        Self { accuracy, buckets }
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
    pub fn num_distinct_values(&self) -> f64 {
        self.buckets
            .iter()
            .fold(0.0, |acc, bucket| acc + bucket.num_distinct())
    }

    /// Get iterator of buckets
    pub fn buckets_iter(&self) -> impl DoubleEndedIterator<Item = &HistogramBucket> {
        self.buckets.iter()
    }

    pub fn add_bucket(&mut self, bucket: HistogramBucket) {
        self.buckets.push(bucket);
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistogramBucket {
    /// Lower bound value of the bucket.
    lower_bound: Datum,
    /// Upper bound value of the bucket.
    upper_bound: Datum,
    /// Estimated number of values in the bucket.
    num_values: f64,
    /// Estimated number of distinct values in the bucket.
    num_distinct: f64,
}

impl HistogramBucket {
    pub fn new(lower_bound: Datum, upper_bound: Datum, num_values: f64, num_distinct: f64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            num_values,
            num_distinct,
        }
    }

    pub fn upper_bound(&self) -> &Datum {
        &self.upper_bound
    }

    pub fn lower_bound(&self) -> &Datum {
        &self.lower_bound
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

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for bucket in &self.buckets {
            writeln!(
                f,
                "{} ~ {}: {} values, {} distinct values",
                bucket.lower_bound(),
                bucket.upper_bound(),
                bucket.num_values,
                bucket.num_distinct
            )?;
        }
        Ok(())
    }
}
