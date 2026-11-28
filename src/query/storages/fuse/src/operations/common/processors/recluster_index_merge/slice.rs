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
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;
use databend_common_pipeline_transforms::Transform;

use super::partition::IndexPartition;
use super::state::ReclusterIndexMergeState;
use crate::io::block_index::BlockIndexSlice;

/// The slices of one partition. Out of the reader they cover every output in output order;
/// the scatter then splits them by the writer owning each output.
#[derive(Debug)]
pub struct IndexPartitionSlices {
    pub id: u32,
    pub index: u16,
    pub slices: Vec<BlockIndexSlice>,
}

local_block_meta_serde!(IndexPartitionSlices);
#[typetag::serde(name = "recluster_index_partition_slices")]
impl BlockMetaInfo for IndexPartitionSlices {}

/// Reads one partition across every source into its per-output slices.
pub struct TransformReclusterIndexSlice {
    state: Arc<ReclusterIndexMergeState>,
}

impl TransformReclusterIndexSlice {
    pub fn new(state: Arc<ReclusterIndexMergeState>) -> Self {
        Self { state }
    }
}

impl Transform for TransformReclusterIndexSlice {
    const NAME: &'static str = "ReclusterIndexSlice";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let Some(meta) = block.take_meta() else {
            return Err(ErrorCode::Internal("missing recluster index partition"));
        };
        let Some(partition) = IndexPartition::downcast_from(meta) else {
            return Err(ErrorCode::Internal("unexpected recluster index partition"));
        };
        let start = Instant::now();
        let merger = self.state.merger(partition.index as usize)?;
        let mapping = self.state.mapping()?;
        let slices = merger.partition(&mapping, partition.partition as usize)?;
        log::debug!(
            "recluster index merge read partition {} of index {} in {:?}",
            partition.partition,
            partition.index,
            start.elapsed()
        );
        Ok(DataBlock::empty_with_meta(Box::new(IndexPartitionSlices {
            id: partition.id,
            index: partition.index,
            slices,
        })))
    }
}
