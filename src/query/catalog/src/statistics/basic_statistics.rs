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

    // Existing min/max-based NDV bound; unavailable for empty or long strings.
    fn domain_size(mut min: Datum, mut max: Datum) -> Option<u64> {
        let range = match (&mut min, &mut max) {
            (Datum::Bytes(min), Datum::Bytes(max)) => {
                // There are 128 characters in ASCII code and 128^4 = 268435456 < 2^32 < 128^5.
                if min.is_empty() || max.is_empty() || min.len() > 4 || max.len() > 4 {
                    return None;
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
        Some(range.saturating_add(1))
    }

    // If the data type is int and max - min + 1 < ndv, then adjust ndv to max - min + 1.
    fn adjust_ndv_by_min_max(ndv: Option<u64>, min: Datum, max: Datum) -> Option<u64> {
        let Some(range) = Self::domain_size(min, max) else {
            return ndv;
        };
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
        let min = self.min.clone().unwrap();
        let max = self.max.clone().unwrap();
        let ndv = Self::adjust_ndv_by_min_max(self.ndv, min.clone(), max.clone());
        let ndv = match ndv {
            None => num_rows,
            Some(v) => Self::estimate_ndv(v, stats_row_count, num_rows),
        };
        // A tiny sample can inflate the Good–Turing estimate beyond the same
        // min/max bound applied above. Preserve that bound after extrapolation.
        let ndv = match Self::domain_size(min, max) {
            Some(range) => ndv.min(range),
            None => ndv,
        };
        Some(Self {
            min: self.min.clone(),
            max: self.max.clone(),
            ndv: Some(ndv),
            null_count: self.null_count,
            in_memory_size: self.in_memory_size,
        })
    }

    // Inspired by duckdb (https://github.com/duckdb/duckdb/blob/main/src/storage/statistics/distinct_statistics.cpp#L55-L69)
    fn estimate_ndv(ndv: u64, stats_row_count: u64, num_rows: u64) -> u64 {
        if ndv == 0 {
            return num_rows;
        }

        // Without a sample, retain the available NDV (already min/max-bounded
        // by the caller) rather than replacing it with the table's row count.
        if stats_row_count == 0 || stats_row_count >= num_rows {
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
    use super::Datum;

    #[test]
    fn test_estimate_ndv() {
        assert_eq!(BasicColumnStatistics::estimate_ndv(0, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(1, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(12, 100, 3000), 17);
        assert_eq!(
            BasicColumnStatistics::estimate_ndv(6000, 10000, 1000000),
            219840
        );
        for (ndv, stats_row_count, num_rows, expected) in [
            (9, 0, 1_000_000, 9),
            (0, 0, 100, 100),
            (500, 0, 100, 100),
            (9, 0, 0, 0),
            (0, 0, 0, 0),
            (0, 100, 0, 0),
            (9, 100, 100, 9),
            (9, 200, 100, 9),
            (500, 100, 100, 100),
            (500, 200, 100, 100),
        ] {
            assert_eq!(
                BasicColumnStatistics::estimate_ndv(ndv, stats_row_count, num_rows),
                expected,
                "ndv={ndv}, stats_row_count={stats_row_count}, num_rows={num_rows}"
            );
        }
    }

    #[test]
    fn test_useful_stat_without_sample_keeps_domain_bounded_ndv() {
        let num_rows = 1_000_000;
        // Fuse defaults the NDV to row count when no HLL/ANALYZE NDV exists.
        // Also cover absent/zero NDV and a known NDV smaller than the domain.
        for (ndv, expected) in [(Some(num_rows), 9), (None, 9), (Some(0), 9), (Some(3), 3)] {
            let stat = BasicColumnStatistics {
                min: Some(Datum::Int(0)),
                max: Some(Datum::Int(8)),
                ndv,
                null_count: 17,
                in_memory_size: 1234,
            };
            let useful = stat.get_useful_stat(num_rows, 0).unwrap();
            assert_eq!(useful.ndv, Some(expected), "input ndv={ndv:?}");
            assert_eq!(useful.min, stat.min);
            assert_eq!(useful.max, stat.max);
            assert_eq!(useful.null_count, stat.null_count);
            assert_eq!(useful.in_memory_size, stat.in_memory_size);
        }
    }

    #[test]
    fn test_useful_stat_tiny_sample_cannot_exceed_domain() {
        let num_rows = 1_000_000;
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(8)),
            ndv: Some(3),
            ..Default::default()
        };
        assert_eq!(BasicColumnStatistics::estimate_ndv(3, 100, num_rows), 30);
        assert_eq!(stat.get_useful_stat(num_rows, 100).unwrap().ndv, Some(9));
    }

    #[test]
    fn test_useful_stat_sampling_and_row_count_bounds() {
        for (ndv, stats_row_count, num_rows, expected) in [
            (6000, 10_000, 1_000_000, 219_840),
            (6000, 1_000_000, 1_000_000, 6000),
            (6000, 2_000_000, 1_000_000, 6000),
            (500, 0, 100, 100),
            (500, 100, 100, 100),
            (500, 200, 100, 100),
            (9, 0, 0, 0),
            (9, 100, 0, 0),
        ] {
            let stat = BasicColumnStatistics {
                min: Some(Datum::Int(0)),
                max: Some(Datum::Int(1_000_000)),
                ndv: Some(ndv),
                ..Default::default()
            };
            assert_eq!(
                stat.get_useful_stat(num_rows, stats_row_count).unwrap().ndv,
                Some(expected),
                "ndv={ndv}, stats_row_count={stats_row_count}, num_rows={num_rows}"
            );
        }

        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(1_000)),
            max: Some(Datum::Int(50_000)),
            ndv: Some(1_000_000),
            ..Default::default()
        };
        assert_eq!(
            stat.get_useful_stat(1_000_000, 0).unwrap().ndv,
            Some(49_001)
        );
    }

    #[test]
    fn test_useful_stat_without_derivable_string_domain() {
        for (min, max) in [
            (b"".to_vec(), b"z".to_vec()),
            (b"alpha".to_vec(), b"omega".to_vec()),
            (b"a".to_vec(), b"abcde".to_vec()),
        ] {
            for (ndv, stats_row_count, expected) in [
                (Some(6000), 0, 6000),
                (Some(6000), 10_000, 219_840),
                (Some(6000), 1_000_000, 6000),
                (Some(2_000_000), 0, 1_000_000),
                (Some(0), 0, 1_000_000),
                (Some(0), 10_000, 1_000_000),
                (None, 0, 1_000_000),
                (None, 10_000, 1_000_000),
            ] {
                let stat = BasicColumnStatistics {
                    min: Some(Datum::Bytes(min.clone())),
                    max: Some(Datum::Bytes(max.clone())),
                    ndv,
                    ..Default::default()
                };
                assert_eq!(
                    stat.get_useful_stat(1_000_000, stats_row_count)
                        .unwrap()
                        .ndv,
                    Some(expected),
                    "min={min:?}, max={max:?}, ndv={ndv:?}, stats_row_count={stats_row_count}"
                );
            }
        }
    }

    #[test]
    fn test_useful_stat_preserves_existing_domain_calculation() {
        // Compatibility checks, not new assumptions about non-integer domains.
        for (min, max, expected) in [
            (Datum::Int(-4), Datum::Int(4), 9),
            (Datum::UInt(0), Datum::UInt(8), 9),
            (Datum::Bool(false), Datum::Bool(true), 2),
            (Datum::Float(1.25.into()), Datum::Float(3.75.into()), 3),
            (Datum::Bytes(b"a".to_vec()), Datum::Bytes(b"c".to_vec()), 3),
        ] {
            let stat = BasicColumnStatistics {
                min: Some(min),
                max: Some(max),
                ndv: Some(100),
                ..Default::default()
            };
            assert_eq!(stat.get_useful_stat(100, 100).unwrap().ndv, Some(expected));
        }
    }

    #[test]
    fn test_useful_stat_requires_compatible_min_max() {
        for (min, max) in [
            (None, None),
            (None, Some(Datum::Int(8))),
            (Some(Datum::Int(0)), None),
            (Some(Datum::Bytes(b"a".to_vec())), Some(Datum::Int(8))),
            (Some(Datum::Int(0)), Some(Datum::Bytes(b"z".to_vec()))),
        ] {
            let stat = BasicColumnStatistics {
                min,
                max,
                ndv: Some(9),
                ..Default::default()
            };
            assert!(stat.get_useful_stat(100, 0).is_none());
            assert!(stat.get_useful_stat(100, 10).is_none());
        }
    }
}
