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

use std::collections::VecDeque;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

pub struct FinalAggregateSpiller {
    // bucket lifecycle:
    working_bucket: VecDeque<AggregateMeta>,
    pending_buckets: Vec<Vec<AggregateMeta>>,

    // spill and restore:
    partition_stream: BlockPartitionStream,
    ready_blocks: Vec<(usize, DataBlock)>,
    // repartition_level: usize,
}

impl FinalAggregateSpiller {
    pub fn new(partition_stream: BlockPartitionStream) -> Self {
        Self {
            working_bucket: VecDeque::new(),
            pending_buckets: Vec::new(),
            partition_stream,
            ready_blocks: Vec::new(),
            // repartition_level: 0,
        }
    }

    pub fn add_ready_blocks(&mut self, partition_ids: Vec<usize>, blocks: Vec<DataBlock>) {
        debug_assert_eq!(partition_ids.len(), blocks.len());

        for (pid, block) in partition_ids.into_iter().zip(blocks.into_iter()) {
            self.ready_blocks.push((pid, block));
        }
    }

    pub fn get_ready_block(&mut self) -> Option<(usize, DataBlock)> {
        self.ready_blocks.pop()
    }

    pub fn add_pending_bucket(&mut self, metas: Vec<AggregateMeta>) {
        self.pending_buckets.push(metas);
    }

    pub fn add_bucket(&mut self, mut data_block: DataBlock) -> Result<()> {
        debug_assert!(
            self.working_bucket.is_empty(),
            "working bucket must be empty when adding new bucket"
        );
        debug_assert!(
            self.pending_buckets.is_empty(),
            "pending buckets must be empty when adding new bucket"
        );

        let Some(meta) = data_block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            return Err(ErrorCode::Internal(
                "TransformMetaDispatcher expects AggregateMeta",
            ));
        };

        if let AggregateMeta::Partitioned { data, .. } = meta {
            debug_assert!(self.working_bucket.is_empty());
            for bucket_data in data {
                self.working_bucket.push_back(bucket_data);
            }
        }
        Ok(())
    }

    pub fn get_meta(&mut self) -> Option<AggregateMeta> {
        self.working_bucket.pop_front()
    }

    /// Refill working bucket from pending buckets.
    ///
    /// returns false if no more pending buckets
    pub fn refill_working_bucket(&mut self) -> bool {
        if let Some(bucket) = self.pending_buckets.pop() {
            self.working_bucket = VecDeque::from(bucket);
        }
        false
    }

    pub fn push_partition_block(&mut self, partition_id: usize, block: DataBlock) {
        if block.is_empty() {
            return;
        }

        let scatter_indices = vec![partition_id as u64; block.num_rows()];
        let ready_blocks = self
            .partition_stream
            .partition(scatter_indices, block, true);

        for ready_block in ready_blocks {
            self.ready_blocks.push(ready_block);
        }
    }
}
