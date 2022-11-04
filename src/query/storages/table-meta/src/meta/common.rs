//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::ops::AddAssign;
use std::sync::Arc;

use common_base::base::uuid::Uuid;
use common_datavalues::Column;
use common_datavalues::DataValue;
use serde::Deserialize;
use serde::Serialize;
use streaming_algorithms::HyperLogLogMagnitude;
use streaming_algorithms::New;
use streaming_algorithms::UnionAssign;
pub type ColumnId = u32;
pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);

pub type StatisticsOfColumns = HashMap<u32, ColumnStatistics>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub min: DataValue,
    pub max: DataValue,
    // A non-backward compatible change has been introduced by [PR#6067](https://github.com/datafuselabs/databend/pull/6067/files#diff-20030750809780d6492d2fe215a8eb80294aa6a8a5af2cf1bebe17eb740cae35)
    // , please also see [issue#6556](https://github.com/datafuselabs/databend/issues/6556)
    // therefore, we alias `null_count` with `unset_bits`, to make subsequent versions backward compatible again
    #[serde(alias = "unset_bits")]
    pub null_count: u64,
    pub in_memory_size: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ClusterStatistics {
    #[serde(default = "default_cluster_key_id")]
    pub cluster_key_id: u32,
    pub min: Vec<DataValue>,
    pub max: Vec<DataValue>,
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

/// Thing has a u64 version nubmer
pub trait Versioned<const V: u64>
where Self: Sized
{
    const VERSION: u64 = V;
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Copy, Clone, Debug)]
pub enum Compression {
    Lz4,
    Lz4Raw,
}

impl Compression {
    pub fn legacy() -> Self {
        Compression::Lz4
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnNDVs {
    pub column_hll: HashMap<ColumnId, HyperLogLogMagnitude<DataValue>>,
}

impl ColumnNDVs {
    // The error rate of hll, the less error rate, the more hll size
    // Error rate  hll size(bytes)
    // 0.04         1057
    // 0.09         289
    // 0.11         161
    const HLL_ERROR_RATE: &'static f64 = &0.11;

    pub fn merge_number_of_distinct_values(&mut self, other: &ColumnNDVs) {
        for (idx, other_hll) in other.column_hll.iter() {
            match self.column_hll.get_mut(idx) {
                Some(hll) => hll.union_assign(other_hll),
                None => {
                    let mut hll = HyperLogLogMagnitude::new(ColumnNDVs::HLL_ERROR_RATE);
                    hll.union_assign(other_hll);
                    self.column_hll.insert(*idx, hll);
                }
            }
        }
    }

    pub fn calc_number_of_distinct_values(&mut self, idx: u32, col: &Arc<dyn Column>) {
        match self.column_hll.get_mut(&idx) {
            Some(hll) => {
                col.to_values().iter().for_each(|value| {
                    hll.add_assign(value);
                });
            }
            None => {
                let mut hll = HyperLogLogMagnitude::new(ColumnNDVs::HLL_ERROR_RATE);
                col.to_values().iter().for_each(|value| {
                    hll.add_assign(value);
                });
                self.column_hll.insert(idx, hll);
            }
        }
    }

    pub fn get_number_of_distinct_values(&self) -> HashMap<ColumnId, u64> {
        let mut map = HashMap::new();
        for (idx, hll) in self.column_hll.iter() {
            map.insert(*idx, hll.0.len() as u64);
        }

        map
    }
}
