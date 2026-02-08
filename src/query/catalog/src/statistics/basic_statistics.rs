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

use databend_common_statistics::Datum;
use databend_storages_common_table_meta::meta::ColumnStatistics;

// #[derive(Debug, Clone)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
/// Basic statistics information of a column
pub struct BasicColumnStatistics {
    /// Min value of the column
    pub min: Option<Datum>,
    /// Max value of the column
    pub max: Option<Datum>,
    // Number of Distinct Value
    pub ndv: Option<u64>,
    // Count of null values
    pub null_count: u64,
    // Memory size of the column
    pub in_memory_size: u64,
}

impl From<ColumnStatistics> for BasicColumnStatistics {
    fn from(value: ColumnStatistics) -> Self {
        Self {
            min: value.min.to_datum(),
            max: value.max.to_datum(),
            ndv: value.distinct_of_values,
            null_count: value.null_count,
            in_memory_size: value.in_memory_size,
        }
    }
}

impl BasicColumnStatistics {
    pub fn new_null() -> Self {
        Self {
            min: None,
            max: None,
            ndv: None,
            null_count: 0,
            in_memory_size: 0,
        }
    }

    pub fn merge(&mut self, other: BasicColumnStatistics) {
        self.min = Datum::min(self.min.clone(), other.min);
        self.max = Datum::max(self.max.clone(), other.max);
        self.ndv = match (self.ndv, other.ndv) {
            (Some(x), Some(y)) => Some(x + y),
            (Some(x), None) | (None, Some(x)) => Some(x),
            _ => None,
        };
        self.null_count += other.null_count;
        self.in_memory_size += other.in_memory_size;
    }

    // If the data type is int and max - min + 1 < ndv, then adjust ndv to max - min + 1.
    fn adjust_ndv_by_min_max(ndv: Option<u64>, mut min: Datum, mut max: Datum) -> Option<u64> {
        let mut range = match (&mut min, &mut max) {
            (Datum::Bytes(min), Datum::Bytes(max)) => {
                // There are 128 characters in ASCII code and 128^4 = 268435456 < 2^32 < 128^5.
                if min.is_empty() || max.is_empty() || min.len() > 4 || max.len() > 4 {
                    return ndv;
                }
                let mut min_value: u32 = 0;
                let mut max_value: u32 = 0;
                while min.len() != max.len() {
                    if min.len() < max.len() {
                        min.push(0);
                    } else {
                        max.push(0);
                    }
                }
                for idx in 0..min.len() {
                    min_value = min_value * 128 + min[idx] as u32;
                    max_value = max_value * 128 + max[idx] as u32;
                }
                (max_value - min_value) as u64
            }
            _ => {
                // Safe to unwrap: min and max are either both Datum::Bytes or neither
                let min = min.as_double().unwrap();
                let max = max.as_double().unwrap();
                (max - min) as u64
            }
        };
        range = range.saturating_add(1);
        let ndv = match ndv {
            Some(ndv) if range > ndv && ndv != 0 => ndv,
            _ => range,
        };
        Some(ndv)
    }

    // Get useful statistics: min, max and ndv are all `Some(_)`.
    pub fn get_useful_stat(&self, num_rows: u64, stats_row_count: u64) -> Option<Self> {
        if self.min.is_none() || self.max.is_none() {
            return None;
        }
        // min and max are either both Datum::Bytes or neither
        if self.min.as_ref().unwrap().is_bytes() ^ self.max.as_ref().unwrap().is_bytes() {
            return None;
        }
        let ndv = Self::adjust_ndv_by_min_max(
            self.ndv,
            self.min.clone().unwrap(),
            self.max.clone().unwrap(),
        );
        let ndv = match ndv {
            None => Some(num_rows),
            Some(v) => Some(Self::estimate_ndv(v, stats_row_count, num_rows)),
        };
        Some(Self {
            min: self.min.clone(),
            max: self.max.clone(),
            ndv,
            null_count: self.null_count,
            in_memory_size: self.in_memory_size,
        })
    }

    // Inspired by duckdb (https://github.com/duckdb/duckdb/blob/main/src/storage/statistics/distinct_statistics.cpp#L55-L69)
    fn estimate_ndv(ndv: u64, stats_row_count: u64, num_rows: u64) -> u64 {
        if stats_row_count == 0 || ndv == 0 {
            return num_rows;
        }

        if stats_row_count >= num_rows {
            return ndv.min(num_rows);
        }

        let s = stats_row_count as f64;
        let n = num_rows as f64;
        let u = ndv.min(stats_row_count) as f64;

        let u1 = (u / s).powi(2) * u;
        // Good–Turing Estimation
        let estimate = u + u1 / s * (n - s);

        estimate.round().clamp(0.0, n) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::BasicColumnStatistics;

    #[test]
    fn test_estimate_ndv() {
        assert_eq!(BasicColumnStatistics::estimate_ndv(0, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(1, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(12, 100, 3000), 17);
        assert_eq!(
            BasicColumnStatistics::estimate_ndv(6000, 10000, 1000000),
            219840
        );
    }
}
