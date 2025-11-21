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

use databend_common_catalog::plan::StealablePartitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::SyncSource;
use databend_common_pipeline::sources::SyncSourcer;

use crate::operations::read::block_partition_meta::BlockPartitionMeta;

pub struct BlockPartitionSource {
    id: usize,
    partitions: StealablePartitions,
    max_batch_size: usize,
}

impl BlockPartitionSource {
    pub fn create(
        id: usize,
        partitions: StealablePartitions,
        max_batch_size: usize,
        ctx: Arc<dyn TableContext>,
        output_port: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        SyncSourcer::create(ctx.get_scan_progress(), output_port, BlockPartitionSource {
            id,
            partitions,
            max_batch_size,
        })
    }
}

impl SyncSource for BlockPartitionSource {
    const NAME: &'static str = "BlockPartitionSource";

    fn generate(&mut self) -> databend_common_exception::Result<Option<DataBlock>> {
        match self.partitions.steal(self.id, self.max_batch_size) {
            None => Ok(None),
            Some(parts) => Ok(Some(DataBlock::empty_with_meta(
                BlockPartitionMeta::create(parts),
            ))),
        }
    }
}
