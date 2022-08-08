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

#[derive(Clone)]
pub struct ClusterStatsGenerator {
    pub cluster_key_id: u32,
    pub cluster_key_index: Vec<usize>,
    pub expression_executor: Option<ExpressionExecutor>,
}

impl ClusterStatsGenerator {
    pub fn gen_stats_with_block(
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
