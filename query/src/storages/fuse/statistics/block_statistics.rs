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

use common_datablocks::DataBlock;
use common_datavalues::DataValue;
use common_fuse_meta::meta::ClusterStatistics;
use common_fuse_meta::meta::ColumnId;
use common_fuse_meta::meta::ColumnStatistics;

use crate::storages::fuse::statistics::column_statistic;

pub struct BlockStatistics {
    pub block_rows_size: u64,
    pub block_bytes_size: u64,
    pub block_file_location: String,
    pub block_column_statistics: HashMap<ColumnId, ColumnStatistics>,
    pub block_cluster_statistics: Option<ClusterStatistics>,
}

impl BlockStatistics {
    pub fn from(
        data_block: &DataBlock,
        location: String,
        cluster_stats: Option<ClusterStatistics>,
    ) -> common_exception::Result<BlockStatistics> {
        Ok(BlockStatistics {
            block_file_location: location,
            block_rows_size: data_block.num_rows() as u64,
            block_bytes_size: data_block.memory_size() as u64,
            block_column_statistics: column_statistic::gen_columns_statistics(data_block)?,
            block_cluster_statistics: cluster_stats,
        })
    }

    pub fn clusters_statistics(
        cluster_key_id: u32,
        cluster_key_index: &[usize],
        block: &DataBlock,
    ) -> common_exception::Result<Option<ClusterStatistics>> {
        if cluster_key_index.is_empty() {
            return Ok(None);
        }

        let mut min = Vec::with_capacity(cluster_key_index.len());
        let mut max = Vec::with_capacity(cluster_key_index.len());

        for key in cluster_key_index.iter() {
            let col = block.column(*key);

            let mut left = col.get_checked(0)?;
            // To avoid high cardinality, for the string column,
            // cluster statistics uses only the first 5 bytes.
            if let DataValue::String(v) = &left {
                let l = v.len() as usize;
                let e = if l < 5 { l } else { 5 };
                left = DataValue::from(&v[0..e]);
            }
            min.push(left);

            let mut right = col.get_checked(col.len() - 1)?;
            if let DataValue::String(v) = &right {
                let l = v.len() as usize;
                let e = if l < 5 { l } else { 5 };
                right = DataValue::from(&v[0..e]);
            }
            max.push(right);
        }

        Ok(Some(ClusterStatistics {
            cluster_key_id,
            min,
            max,
        }))
    }
}
