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

    // Only exactly computed discrete domains are valid NDV upper bounds.
    // Float also represents Decimal here; interval width does not bound its
    // cardinality. Variable-length byte strings have no such bound either.
    fn domain_size(min: Datum, max: Datum) -> Option<u64> {
        match (min, max) {
            (Datum::Int(min), Datum::Int(max)) if min <= max => {
                u64::try_from(i128::from(max) - i128::from(min) + 1).ok()
            }
            (Datum::UInt(min), Datum::UInt(max)) => max.checked_sub(min)?.checked_add(1),
            (Datum::Bool(min), Datum::Bool(max)) if min <= max => {
                Some(u64::from(max) - u64::from(min) + 1)
            }
            // A domain containing 2^64 values cannot be represented by u64.
            // Falling back to the row count is already tighter in that case.
            _ => None,
        }
    }

    // Apply an exact discrete-domain bound when one is available.
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
        // Extrapolation (or the unknown-sample row-count fallback) can exceed
        // the exact discrete-domain bound. Apply it at the provider boundary.
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
        // Zero also means an unknown sample size in upgraded legacy HLL
        // metadata. Do not assume that its NDV covers the current rows.
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
            (9, 0, 1_000_000, 1_000_000),
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
        // Zero sample rows can also mean an unknown legacy sample size, so
        // do not trust a smaller input NDV without coverage information.
        for (ndv, expected) in [(Some(num_rows), 9), (None, 9), (Some(0), 9), (Some(3), 9)] {
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
                (Some(6000), 0, 1_000_000),
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
    fn test_useful_stat_applies_only_exact_domain_bounds() {
        for (min, max, expected) in [
            (Datum::Int(-4), Datum::Int(4), 9),
            (Datum::UInt(0), Datum::UInt(8), 9),
            (Datum::Bool(false), Datum::Bool(true), 2),
            (Datum::Float(1.25.into()), Datum::Float(3.75.into()), 100),
            (
                Datum::Bytes(b"a".to_vec()),
                Datum::Bytes(b"c".to_vec()),
                100,
            ),
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
    fn test_domain_size_requires_exact_cardinality() {
        let cases = [
            (Datum::Int(i64::MAX - 1), Datum::Int(i64::MAX), Some(2)),
            (
                Datum::UInt(1_u64 << 53),
                Datum::UInt((1_u64 << 53) + 1),
                Some(2),
            ),
            (Datum::Int(-1), Datum::Int(1), Some(3)),
            (Datum::Bool(false), Datum::Bool(true), Some(2)),
            (Datum::Int(i64::MIN), Datum::Int(i64::MAX), None),
            (Datum::UInt(0), Datum::UInt(u64::MAX), None),
            (Datum::Int(1), Datum::Int(0), None),
            (Datum::Float(0.0.into()), Datum::Float(2.5.into()), None),
            (
                Datum::Bytes(b"a".to_vec()),
                Datum::Bytes(b"b".to_vec()),
                None,
            ),
        ];
        for (min, max, expected) in cases {
            assert_eq!(BasicColumnStatistics::domain_size(min, max), expected);
        }
    }

    #[test]
    fn test_useful_stat_does_not_cap_fractional_or_string_domains() {
        // Decimal statistics also use Datum::Float. Neither interval width nor
        // a positional encoding of variable-length strings bounds their NDV.
        for (min, max) in [
            (Datum::Float(0.0.into()), Datum::Float(2.5.into())),
            (Datum::Bytes(b"a".to_vec()), Datum::Bytes(b"b".to_vec())),
        ] {
            let stat = BasicColumnStatistics {
                min: Some(min),
                max: Some(max),
                ndv: Some(80),
                ..Default::default()
            };
            for (sample_rows, expected) in [(0, 1_000), (100, 541), (1_000, 80)] {
                assert_eq!(
                    stat.get_useful_stat(1_000, sample_rows).unwrap().ndv,
                    Some(expected)
                );
            }
        }
    }

    #[test]
    fn test_zero_stats_rows_does_not_trust_legacy_ndv() {
        // Legacy HLL metadata can have an unknown (zero) sample row count.
        // An old count must not be treated as covering newly appended rows.
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(999)),
            ndv: Some(3),
            ..Default::default()
        };
        assert_eq!(stat.get_useful_stat(1_000, 0).unwrap().ndv, Some(1_000));
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
