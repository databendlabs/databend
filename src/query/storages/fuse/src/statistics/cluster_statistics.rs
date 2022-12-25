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

use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_sql::evaluator::ChunkOperator;
use common_storages_table_meta::meta::ClusterStatistics;

#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,
    cluster_key_index: Vec<usize>,
    extra_key_index: Vec<usize>,
    level: i32,
    block_compact_thresholds: BlockCompactThresholds,
    operators: Vec<ChunkOperator>,
}

impl ClusterStatsGenerator {
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        extra_key_index: Vec<usize>,
        level: i32,
        block_compact_thresholds: BlockCompactThresholds,
        operators: Vec<ChunkOperator>,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            extra_key_index,
            level,
            block_compact_thresholds,
            operators,
        }
    }

    pub fn is_cluster(&self) -> bool {
        !self.cluster_key_index.is_empty()
    }

    pub fn block_compact_thresholds(&self) -> BlockCompactThresholds {
        self.block_compact_thresholds
    }

    // This can be used in block append.
    // The input block contains the cluster key block.
    pub fn gen_stats_for_append(
        &self,
        data_block: &DataBlock,
    ) -> Result<(Option<ClusterStatistics>, DataBlock)> {
        let cluster_stats = self.clusters_statistics(data_block, self.level)?;
        let mut block = data_block.clone();

        for id in self.extra_key_index.iter() {
            block = block.remove_column_index(*id)?;
        }

        Ok((cluster_stats, block))
    }

    // This can be used in deletion, for an existing block.
    pub fn gen_with_origin_stats(
        &self,
        data_block: &DataBlock,
        origin_stats: Option<ClusterStatistics>,
    ) -> Result<Option<ClusterStatistics>> {
        if origin_stats.is_none() {
            return Ok(None);
        }

        let origin_stats = origin_stats.unwrap();
        if origin_stats.cluster_key_id != self.cluster_key_id {
            return Ok(None);
        }

        let mut block = data_block.clone();

        if !self.cluster_key_index.is_empty() {
            let indices = vec![0u32, block.num_rows() as u32 - 1];
            block = DataBlock::block_take_by_indices(&block, &indices)?;
        }

        let func_ctx = FunctionContext::default();
        block = self
            .operators
            .iter()
            .try_fold(block, |input, op| op.execute(&func_ctx, input))?;

        self.clusters_statistics(&block, origin_stats.level)
    }

    fn clusters_statistics(
        &self,
        data_block: &DataBlock,
        level: i32,
    ) -> Result<Option<ClusterStatistics>> {
        if self.cluster_key_index.is_empty() {
            return Ok(None);
        }

        let mut min = Vec::with_capacity(self.cluster_key_index.len());
        let mut max = Vec::with_capacity(self.cluster_key_index.len());

        for key in self.cluster_key_index.iter() {
            let col = data_block.column(*key);

            let mut left = col.get_checked(0)?;
            // To avoid high cardinality, for the string column,
            // cluster statistics uses only the first 5 bytes.
            if let DataValue::String(v) = &left {
                let l = v.len();
                let e = if l < 5 { l } else { 5 };
                left = DataValue::from(&v[0..e]);
            }
            min.push(left);

            let mut right = col.get_checked(col.len() - 1)?;
            if let DataValue::String(v) = &right {
                let l = v.len();
                let e = if l < 5 { l } else { 5 };
                right = DataValue::from(&v[0..e]);
            }
            max.push(right);
        }

        let level = if min == max
            && self
                .block_compact_thresholds
                .check_perfect_block(data_block.num_rows(), data_block.memory_size())
        {
            -1
        } else {
            level
        };

        Ok(Some(ClusterStatistics {
            cluster_key_id: self.cluster_key_id,
            min,
            max,
            level,
        }))
    }
}
