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
use databend_common_statistics::NdvEstimate;
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
    #[serde(default, with = "ndv_estimate_serde")]
    pub ndv: Option<NdvEstimate>,
    // Count of null values
    pub null_count: u64,
    // Memory size of the column
    pub in_memory_size: u64,
}

mod ndv_estimate_serde {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    use super::NdvEstimate;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NdvEstimateCompat {
        Legacy(u64),
        Current(NdvEstimate),
    }

    pub fn serialize<S>(ndv: &Option<NdvEstimate>, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        ndv.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NdvEstimate>, D::Error>
    where D: Deserializer<'de> {
        Option::<NdvEstimateCompat>::deserialize(deserializer).map(|ndv| {
            ndv.map(|ndv| match ndv {
                NdvEstimateCompat::Legacy(ndv) => NdvEstimate::exact(ndv as f64),
                NdvEstimateCompat::Current(ndv) => ndv,
            })
        })
    }
}

impl From<ColumnStatistics> for BasicColumnStatistics {
    fn from(value: ColumnStatistics) -> Self {
        Self {
            min: value.min.to_datum(),
            max: value.max.to_datum(),
            ndv: value
                .distinct_of_values
                .map(|ndv| NdvEstimate::exact(ndv as f64)),
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
        if self.min.is_none()
            && self.max.is_none()
            && self.ndv.is_none()
            && self.null_count == 0
            && self.in_memory_size == 0
        {
            *self = other;
            return;
        }

        self.min = Datum::min(self.min.clone(), other.min);
        self.max = Datum::max(self.max.clone(), other.max);
        self.ndv = match (self.ndv, other.ndv) {
            (Some(x), Some(y)) => Some(Self::add_ndv(x, y)),
            (Some(x), None) | (None, Some(x)) => Some(NdvEstimate::upper_bound(x.upper)),
            _ => None,
        };
        self.null_count += other.null_count;
        self.in_memory_size += other.in_memory_size;
    }

    // If the data type is int and max - min + 1 < ndv, then adjust ndv to max - min + 1.
    fn adjust_ndv_by_min_max(
        ndv: Option<NdvEstimate>,
        mut min: Datum,
        mut max: Datum,
    ) -> Option<NdvEstimate> {
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
        match ndv {
            Some(ndv) if range > ndv.upper as u64 && ndv.upper != 0.0 => Some(ndv),
            _ if range == 1 => Some(NdvEstimate::exact(1.0)),
            Some(ndv) => Some(ndv.reduce(range as f64)),
            None => Some(NdvEstimate::upper_bound(range as f64)),
        }
    }

    pub fn ndv_estimate(&self, num_rows: Option<u64>) -> NdvEstimate {
        let max_non_null_count = num_rows
            .map(|num_rows| num_rows.saturating_sub(self.null_count) as f64)
            .or_else(|| self.ndv.map(|ndv| ndv.upper))
            .unwrap_or(u64::MAX as f64);

        match self.ndv {
            Some(ndv) => ndv.reduce(max_non_null_count),
            None => NdvEstimate::upper_bound(max_non_null_count),
        }
    }

    pub fn ndv_for_synthetic_histogram(&self) -> Option<u64> {
        self.ndv.and_then(|ndv| match ndv.expected {
            Some(expected) if expected == ndv.upper => Some(expected.round() as u64),
            _ => None,
        })
    }

    pub fn ndv_value(&self) -> Option<u64> {
        self.ndv
            .map(|ndv| ndv.expected.unwrap_or(ndv.upper).round() as u64)
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
        let non_null_rows = num_rows.saturating_sub(self.null_count);
        let ndv = match ndv {
            None => Some(NdvEstimate::upper_bound(non_null_rows as f64)),
            Some(ndv) if ndv.expected.is_some() && stats_row_count < num_rows => {
                let expected = Self::estimate_ndv(
                    ndv.expected.unwrap().round() as u64,
                    stats_row_count,
                    non_null_rows,
                ) as f64;
                Some(NdvEstimate::new(expected, non_null_rows as f64))
            }
            Some(ndv) => Some(ndv.reduce(non_null_rows as f64)),
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

    fn add_ndv(left: NdvEstimate, right: NdvEstimate) -> NdvEstimate {
        NdvEstimate::upper_bound(left.upper + right.upper)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_statistics::Datum;
    use databend_common_statistics::NdvEstimate;

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

    #[test]
    fn test_missing_ndv_from_range_stays_upper_only() {
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(10)),
            ndv: None,
            null_count: 0,
            in_memory_size: 0,
        }
        .get_useful_stat(100, 100)
        .unwrap();

        assert_eq!(stat.ndv, Some(NdvEstimate::upper_bound(10.0)));
        assert_eq!(stat.ndv_estimate(Some(100)), NdvEstimate::upper_bound(10.0));
        assert_eq!(stat.ndv_for_synthetic_histogram(), None);
    }

    #[test]
    fn test_single_value_range_produces_exact_ndv() {
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(1)),
            ndv: None,
            null_count: 0,
            in_memory_size: 0,
        }
        .get_useful_stat(100, 100)
        .unwrap();

        assert_eq!(stat.ndv, Some(NdvEstimate::exact(1.0)));
        assert_eq!(stat.ndv_estimate(Some(100)), NdvEstimate::exact(1.0));
        assert_eq!(stat.ndv_for_synthetic_histogram(), Some(1));
    }

    #[test]
    fn test_sampled_statistics_ndv_is_expected_but_not_histogram_source() {
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(100)),
            ndv: Some(NdvEstimate::exact(12.0)),
            null_count: 0,
            in_memory_size: 0,
        }
        .get_useful_stat(3000, 100)
        .unwrap();

        assert_eq!(stat.ndv, Some(NdvEstimate::new(17.0, 3000.0)));
        assert_eq!(
            stat.ndv_estimate(Some(3000)),
            NdvEstimate::new(17.0, 3000.0)
        );
        assert_eq!(stat.ndv_for_synthetic_histogram(), None);
    }

    #[test]
    fn test_merged_ndv_is_upper_bound() {
        let mut left = BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(10)),
            ndv: Some(NdvEstimate::exact(10.0)),
            null_count: 0,
            in_memory_size: 0,
        };
        left.merge(BasicColumnStatistics {
            min: Some(Datum::Int(1)),
            max: Some(Datum::Int(10)),
            ndv: Some(NdvEstimate::exact(10.0)),
            null_count: 0,
            in_memory_size: 0,
        });

        assert_eq!(left.ndv, Some(NdvEstimate::upper_bound(20.0)));
        assert_eq!(left.ndv_estimate(Some(100)), NdvEstimate::upper_bound(20.0));
    }

    #[test]
    fn test_deserialize_legacy_ndv_value() {
        let stat: BasicColumnStatistics = serde_json::from_str(
            r#"{"min":null,"max":null,"ndv":12,"null_count":0,"in_memory_size":0}"#,
        )
        .unwrap();

        assert_eq!(stat.ndv, Some(NdvEstimate::exact(12.0)));
    }
}
