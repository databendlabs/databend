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

use std::sync::Arc;

use databend_common_expression::BlockThresholds;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Statistics;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<Arc<BlockMeta>>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
}

impl StatisticsAccumulator {
    pub fn add_with_block_meta(&mut self, block_meta: BlockMeta) {
        self.summary_row_count += block_meta.row_count;
        self.summary_block_count += 1;
        self.blocks_metas.push(Arc::new(block_meta));
    }

    pub fn summary(
        &self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Statistics {
        super::reduce_block_metas(&self.blocks_metas, thresholds, default_cluster_key_id)
    }
}
