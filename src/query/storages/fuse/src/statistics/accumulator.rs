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
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;

use crate::io::build_virtual_segment_schema;

#[derive(Default)]
pub struct RowOrientedSegmentBuilder {
    pub blocks_metas: Vec<Arc<BlockMeta>>,
    pub virtual_inputs: Vec<VirtualBlockInput>,
}

impl SegmentBuilder for RowOrientedSegmentBuilder {
    type Segment = SegmentInfo;
    fn block_count(&self) -> usize {
        self.blocks_metas.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta, virtual_input: VirtualBlockInput) -> Result<()> {
        self.blocks_metas.push(Arc::new(block_meta));
        self.virtual_inputs.push(virtual_input);
        Ok(())
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        cluster_key_info: Option<&ClusterKeyInfo>,
        additional_stats_meta: Option<AdditionalStatsMeta>,
    ) -> Result<Self::Segment> {
        let mut builder = std::mem::take(self);
        let virtual_schema =
            build_virtual_segment_schema(&mut builder.blocks_metas, &mut builder.virtual_inputs)?;
        let mut stat =
            super::reduce_block_metas(&builder.blocks_metas, thresholds, cluster_key_info)?;
        stat.additional_stats_meta = additional_stats_meta;
        stat.virtual_segment_schema = virtual_schema;
        let segment = SegmentInfo::new(builder.blocks_metas, stat);
        Ok(segment)
    }

    fn new(_table_schema: TableSchemaRef, _block_per_segment: usize) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct ColumnHLLAccumulator {
    pub hlls: Vec<RawBlockHLL>,
    pub summary: BlockHLL,
}

impl ColumnHLLAccumulator {
    pub fn add_hll(&mut self, hll: BlockHLLState) -> Result<()> {
        match hll {
            BlockHLLState::Deserialized(v) => {
                let data = encode_column_hll(&v)?;
                self.hlls.push(data);
                merge_column_hll_mut(&mut self.summary, &v);
            }
            BlockHLLState::Serialized(v) => self.hlls.push(v),
        }
        Ok(())
    }

    pub fn build_segment_statistics(&mut self, block_top_ns: Vec<BlockTopN>) -> SegmentStatistics {
        SegmentStatistics::new(std::mem::take(&mut self.hlls), block_top_ns)
    }

    pub fn is_empty(&self) -> bool {
        self.hlls.is_empty()
    }

    pub fn take_summary(&mut self) -> BlockHLL {
        std::mem::take(&mut self.summary)
    }
}
