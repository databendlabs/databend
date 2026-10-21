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
    }

    #[test]
    fn test_domain_bound_after_estimation() {
        // Zero coverage also represents old HLL metadata, not a fresh NDV.
        for (ndv, sampled_rows, rows, max, expected) in [
            (Some(1_000_000), 0, 1_000_000, 8, 9),
            (Some(3), 100, 1_000_000, 8, 9),
            (None, 0, 1_000_000, 8, 9),
            (Some(0), 0, 1_000_000, 8, 9),
            (Some(3), 0, 1_000, 999, 1_000),
        ] {
            let stat = BasicColumnStatistics {
                min: Some(Datum::Int(0)),
                max: Some(Datum::Int(max)),
                ndv,
                ..Default::default()
            };
            assert_eq!(
                stat.get_useful_stat(rows, sampled_rows).unwrap().ndv,
                Some(expected),
                "ndv={ndv:?}, sampled_rows={sampled_rows}, rows={rows}, max={max}"
            );
        }
    }

    #[test]
    fn test_exact_domain_size() {
        for (min, max, expected) in [
            (Datum::Int(-4), Datum::Int(4), Some(9)),
            (Datum::Int(i64::MAX - 1), Datum::Int(i64::MAX), Some(2)),
            (
                Datum::UInt(1_u64 << 53),
                Datum::UInt((1_u64 << 53) + 1),
                Some(2),
            ),
            (Datum::Bool(false), Datum::Bool(true), Some(2)),
            (Datum::Bool(true), Datum::Bool(true), Some(1)),
            (Datum::Int(i64::MIN), Datum::Int(i64::MAX), None),
            (Datum::UInt(0), Datum::UInt(u64::MAX), None),
            (Datum::Int(1), Datum::Int(0), None),
        ] {
            assert_eq!(BasicColumnStatistics::domain_size(min, max), expected);
        }
    }

    #[test]
    fn test_fractional_and_string_domains_are_not_capped() {
        // Datum::Float also stores Decimal statistics. Neither fractional
        // interval width nor a short-string encoding bounds cardinality.
        for (min, max) in [
            (Datum::Float(0.0.into()), Datum::Float(2.5.into())),
            (Datum::Bytes(b"a".to_vec()), Datum::Bytes(b"b".to_vec())),
        ] {
            assert_eq!(
                BasicColumnStatistics::domain_size(min.clone(), max.clone()),
                None
            );
            let stat = BasicColumnStatistics {
                min: Some(min),
                max: Some(max),
                ndv: Some(80),
                ..Default::default()
            };
            for (sampled_rows, expected) in [(0, 1_000), (100, 541), (1_000, 80)] {
                assert_eq!(
                    stat.get_useful_stat(1_000, sampled_rows).unwrap().ndv,
                    Some(expected)
                );
            }
        }
    }
}
