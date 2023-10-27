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

use std::cmp::Ordering;

use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_sql::evaluator::BlockOperator;
use storages_common_table_meta::meta::ClusterStatistics;

use crate::statistics::column_statistic::Trim;
use crate::table_functions::cmp_with_null;

pub const CLUSTER_STATS_STRING_PREFIX_LEN: usize = 8;

#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,

    pub(crate) cluster_key_index: Vec<usize>,
    pub(crate) extra_key_num: usize,

    max_page_size: Option<usize>,

    level: i32,
    block_thresholds: BlockThresholds,

    pub(crate) operators: Vec<BlockOperator>,
    pub(crate) out_fields: Vec<DataField>,
    pub(crate) func_ctx: FunctionContext,
}

impl ClusterStatsGenerator {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        extra_key_num: usize,
        max_page_size: Option<usize>,
        level: i32,
        block_thresholds: BlockThresholds,
        operators: Vec<BlockOperator>,
        out_fields: Vec<DataField>,
        func_ctx: FunctionContext,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            extra_key_num,
            max_page_size,
            level,
            block_thresholds,
            operators,
            out_fields,
            func_ctx,
        }
    }

    pub fn is_cluster(&self) -> bool {
        !self.cluster_key_index.is_empty()
    }

    pub fn block_thresholds(&self) -> BlockThresholds {
        self.block_thresholds
    }

    // This can be used in block append.
    // The input block contains the cluster key block.
    pub fn gen_stats_for_append(
        &self,
        data_block: DataBlock,
    ) -> Result<(Option<ClusterStatistics>, DataBlock)> {
        let cluster_stats = self.clusters_statistics(&data_block, self.level)?;
        let block = data_block.pop_columns(self.extra_key_num)?;
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
            block = block.take(&indices, &mut None)?;
        }

        block = self
            .operators
            .iter()
            .try_fold(block, |input, op| op.execute(&self.func_ctx, input))?;

        self.clusters_statistics(&block, origin_stats.level)
    }

    /// for string value, only use the first 8 bytes.
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
            let left = unsafe { val_ref.index_unchecked(0) }.to_owned();
            min.push(
                left.clone()
                    .trim_min(CLUSTER_STATS_STRING_PREFIX_LEN)
                    .unwrap_or(left),
            );

            // The maximum in cluster statistics neednot larger than the non-trimmed one.
            // So we use trim_min directly.
            let right = unsafe { val_ref.index_unchecked(val_ref.len() - 1) }.to_owned();
            max.push(
                right
                    .clone()
                    .trim_min(CLUSTER_STATS_STRING_PREFIX_LEN)
                    .unwrap_or(right),
            );
        }

        let level = if min == max
            && self
                .block_thresholds
                .check_large_enough(data_block.num_rows(), data_block.memory_size())
        {
            -1
        } else {
            level
        };

        let pages = if let Some(max_page_size) = self.max_page_size {
            let mut values = Vec::with_capacity(data_block.num_rows() / max_page_size + 1);
            for start in (0..data_block.num_rows()).step_by(max_page_size) {
                let mut tuple_values = Vec::with_capacity(self.cluster_key_index.len());
                for key in self.cluster_key_index.iter() {
                    let val = data_block.get_by_offset(*key);
                    let val_ref = val.value.as_ref();
                    let left = unsafe { val_ref.index_unchecked(start) };
                    tuple_values.push(left.to_owned());
                }
                values.push(Scalar::Tuple(tuple_values));
            }
            Some(values)
        } else {
            None
        };

        Ok(Some(ClusterStatistics::new(
            self.cluster_key_id,
            min,
            max,
            level,
            pages,
        )))
    }
}

pub fn sort_by_cluster_stats(
    v1: &Option<ClusterStatistics>,
    v2: &Option<ClusterStatistics>,
    default_cluster_key: u32,
) -> Ordering {
    match (v1.as_ref(), v2.as_ref()) {
        (Some(a), Some(b)) => {
            if a.cluster_key_id != default_cluster_key && b.cluster_key_id != default_cluster_key {
                return Ordering::Equal;
            }

            match a.min().iter().cmp_by(b.min().iter(), cmp_with_null) {
                Ordering::Equal => a.max().iter().cmp_by(b.max().iter(), cmp_with_null),
                ord => ord,
            }
        }
        _ => Ordering::Equal,
    }
}
