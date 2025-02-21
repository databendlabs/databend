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

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;

use crate::column_oriented_segment::SegmentBuilder;

#[derive(Default)]
pub struct RowOrientedSegmentBuilder {
    pub blocks_metas: Vec<Arc<BlockMeta>>,
}

impl SegmentBuilder for RowOrientedSegmentBuilder {
    type Segment = SegmentInfo;
    fn block_count(&self) -> usize {
        self.blocks_metas.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        self.blocks_metas.push(Arc::new(block_meta));
        Ok(())
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Self::Segment> {
        let builder = std::mem::take(self);
        let stat =
            super::reduce_block_metas(&builder.blocks_metas, thresholds, default_cluster_key_id);
        Ok(SegmentInfo::new(builder.blocks_metas, stat))
    }
}
