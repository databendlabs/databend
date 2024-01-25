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

use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub min: Scalar,
    pub max: Scalar,
    // A non-backward compatible change has been introduced by [PR#6067](https://github.com/datafuselabs/databend/pull/6067/files#diff-20030750809780d6492d2fe215a8eb80294aa6a8a5af2cf1bebe17eb740cae35)
    // , please also see [issue#6556](https://github.com/datafuselabs/databend/issues/6556)
    // therefore, we alias `null_count` with `unset_bits`, to make subsequent versions backward compatible again
    #[serde(alias = "unset_bits")]
    pub null_count: u64,
    pub in_memory_size: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClusterStatistics {
    #[serde(default = "default_cluster_key_id")]
    pub cluster_key_id: u32,
    pub min: Vec<Scalar>,
    pub max: Vec<Scalar>,
    // The number of times the data in that block has been clustered. New blocks has zero level.
    #[serde(default = "default_level")]
    pub level: i32,
}

fn default_cluster_key_id() -> u32 {
    0
}

fn default_level() -> i32 {
    0
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    #[serde(default)]
    pub index_size: u64,

    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
}
