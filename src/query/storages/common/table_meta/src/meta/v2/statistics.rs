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
