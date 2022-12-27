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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::BlockCompactThresholds;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::FunctionContext;
use common_expression::ScalarRef;
use common_sql::evaluator::BlockOperator;
use common_storages_table_meta::meta::ClusterStatistics;

#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,

    pub(crate) cluster_key_index: Vec<usize>,
    pub(crate) extra_key_index: Vec<usize>,

    level: i32,
    block_compact_thresholds: BlockCompactThresholds,
    operators: Vec<BlockOperator>,
    pub(crate) out_fields: Vec<DataField>,
}

impl ClusterStatsGenerator {
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        extra_key_index: Vec<usize>,
        level: i32,
        block_compact_thresholds: BlockCompactThresholds,
        operators: Vec<BlockOperator>,
        out_fields: Vec<DataField>,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            extra_key_index,
            level,
            block_compact_thresholds,
            operators,
            out_fields,
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
            block = block.remove_column(*id)?;
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

        for id in self.extra_key_index.iter() {
            block = block.remove_column(*id)?;
        }

        if !self.cluster_key_index.is_empty() {
            let indices = vec![0u32, block.num_rows() as u32 - 1];
            block = block.take(&indices)?;
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
            let val = data_block.get_by_offset(*key);
            let val_ref = val.value.as_ref();
            let mut left = unsafe { val_ref.index_unchecked(0) };
            // To avoid high cardinality, for the string column,
            // cluster statistics uses only the first 5 bytes.
            if val.data_type == DataType::String {
                let v = left.into_string().unwrap();
                let l = v.len();
                let e = if l < 5 { l } else { 5 };
                left = ScalarRef::String(&v[0..e]);
            }
            min.push(left.to_owned());

            let mut right = unsafe { val_ref.index_unchecked(val_ref.len() - 1) };
            if val.data_type == DataType::String {
                let v = right.into_string().unwrap();
                let l = v.len();
                let e = if l < 5 { l } else { 5 };
                right = ScalarRef::String(&v[0..e]);
            }
            max.push(right.to_owned());
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
