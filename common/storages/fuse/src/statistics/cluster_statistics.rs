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

use common_datablocks::DataBlock;
use common_datavalues::DataValue;
use common_exception::Result;
use common_fuse_meta::meta::ClusterStatistics;
use common_pipeline_transforms::processors::ExpressionExecutor;

#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,
    cluster_key_index: Vec<usize>,
    expression_executor: Option<ExpressionExecutor>,
}

impl ClusterStatsGenerator {
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        expression_executor: Option<ExpressionExecutor>,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            expression_executor,
        }
    }

    pub fn gen_stats_for_append(
        &self,
        data_block: &DataBlock,
    ) -> Result<(Option<ClusterStatistics>, DataBlock)> {
        let cluster_stats = self.clusters_statistics(data_block)?;

        let mut block = data_block.clone();
        // Remove unused columns.
        if let Some(executor) = &self.expression_executor {
            block = executor.execute(&block)?;
        }

        Ok((cluster_stats, block))
    }

    pub fn gen_stats_with_origin(
        &self,
        sorted_block: &DataBlock,
        origin_stats: Option<ClusterStatistics>,
    ) -> Result<Option<ClusterStatistics>> {
        if origin_stats.is_none() {
            return Ok(None);
        }

        let origin_stats = origin_stats.unwrap();
        if origin_stats.cluster_key_id != self.cluster_key_id {
            return Ok(None);
        }

        let block = if let Some(executor) = &self.expression_executor {
            let indices = vec![0u32, sorted_block.num_rows() as u32 - 1];
            let input = DataBlock::block_take_by_indices(sorted_block, &indices)?;
            executor.execute(&input)?
        } else {
            sorted_block.clone()
        };

        self.clusters_statistics(&block)
    }

    fn clusters_statistics(&self, data_block: &DataBlock) -> Result<Option<ClusterStatistics>> {
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
            cluster_key_id: self.cluster_key_id,
            min,
            max,
        }))
    }
}
