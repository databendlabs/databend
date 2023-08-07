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

use std::collections::HashMap;

use common_expression::converts::from_scalar;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_storage::Datum;

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
}

impl From<ColumnStatistics> for BasicColumnStatistics {
    fn from(value: ColumnStatistics) -> Self {
        Self {
            min: Datum::from_scalar(value.min),
            max: Datum::from_scalar(value.max),
            ndv: value.distinct_of_values,
            null_count: value.null_count,
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
                let min = min.to_double().unwrap();
                let max = max.to_double().unwrap();
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
    pub fn get_useful_stat(&self, num_rows: u64) -> Option<Self> {
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
            Some(ndv) if ndv == 0 => Some(num_rows),
            None => Some(num_rows),
            _ => ndv,
        };
        Some(Self {
            min: self.min.clone(),
            max: self.max.clone(),
            ndv,
            null_count: self.null_count,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub min: Scalar,
    pub max: Scalar,

    pub null_count: u64,
    pub in_memory_size: u64,
    pub distinct_of_values: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClusterStatistics {
    pub cluster_key_id: u32,
    pub min: Vec<Scalar>,
    pub max: Vec<Scalar>,
    pub level: i32,

    // currently it's only used in native engine
    pub pages: Option<Vec<Scalar>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,
    pub perfect_block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub index_size: u64,

    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub cluster_stats: Option<ClusterStatistics>,
}

// conversions from old meta data
// ----------------------------------------------------------------
// ----------------------------------------------------------------
impl ColumnStatistics {
    pub fn new(
        min: Scalar,
        max: Scalar,
        null_count: u64,
        in_memory_size: u64,
        distinct_of_values: Option<u64>,
    ) -> Self {
        Self {
            min,
            max,
            null_count,
            in_memory_size,
            distinct_of_values,
        }
    }

    pub fn min(&self) -> &Scalar {
        &self.min
    }

    pub fn max(&self) -> &Scalar {
        &self.max
    }

    pub fn from_v0(
        v0: &crate::meta::v0::statistics::ColumnStatistics,
        data_type: &TableDataType,
    ) -> Self {
        let data_type = data_type.into();
        Self {
            min: from_scalar(&v0.min, &data_type),
            max: from_scalar(&v0.max, &data_type),
            null_count: v0.null_count,
            in_memory_size: v0.in_memory_size,
            distinct_of_values: None,
        }
    }
}

impl ClusterStatistics {
    pub fn new(
        cluster_key_id: u32,
        min: Vec<Scalar>,
        max: Vec<Scalar>,
        level: i32,
        pages: Option<Vec<Scalar>>,
    ) -> Self {
        Self {
            cluster_key_id,
            min,
            max,
            level,
            pages,
        }
    }

    pub fn min(&self) -> Vec<Scalar> {
        self.min.clone()
    }

    pub fn max(&self) -> Vec<Scalar> {
        self.max.clone()
    }

    pub fn is_const(&self) -> bool {
        self.min.eq(&self.max)
    }

    pub fn from_v0(
        v0: crate::meta::v0::statistics::ClusterStatistics,
        data_type: &TableDataType,
    ) -> Self {
        let data_type = data_type.into();
        Self {
            cluster_key_id: v0.cluster_key_id,
            min: v0
                .min
                .into_iter()
                .map(|s| from_scalar(&s, &data_type))
                .collect(),
            max: v0
                .max
                .into_iter()
                .map(|s| from_scalar(&s, &data_type))
                .collect(),
            level: v0.level,
            pages: None,
        }
    }
}

impl Statistics {
    pub fn from_v0(v0: crate::meta::v0::statistics::Statistics, fields: &[TableField]) -> Self {
        let col_stats = v0
            .col_stats
            .into_iter()
            .map(|(k, v)| {
                let t = fields[k as usize].data_type();
                (k, ColumnStatistics::from_v0(&v, t))
            })
            .collect();
        Self {
            row_count: v0.row_count,
            block_count: v0.block_count,
            perfect_block_count: v0.perfect_block_count,
            uncompressed_byte_size: v0.uncompressed_byte_size,
            compressed_byte_size: v0.compressed_byte_size,
            index_size: v0.index_size,
            col_stats,
            cluster_stats: None,
        }
    }
}
