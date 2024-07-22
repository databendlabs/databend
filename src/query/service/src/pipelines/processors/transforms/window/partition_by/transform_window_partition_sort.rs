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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort_merge;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;

use crate::pipelines::processors::transforms::WindowPartitionMeta;

pub struct TransformWindowPartitionSort {
    sort_desc: Vec<SortColumnDescription>,
    schema: DataSchemaRef,
    block_size: usize,
    sort_spilling_batch_bytes: usize,
    enable_loser_tree: bool,
    have_order_col: bool,
}

impl TransformWindowPartitionSort {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        sort_desc: Vec<SortColumnDescription>,
        schema: DataSchemaRef,
        block_size: usize,
        sort_spilling_batch_bytes: usize,
        enable_loser_tree: bool,
        have_order_col: bool,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(BlockMetaTransformer::create(
            input,
            output,
            TransformWindowPartitionSort {
                sort_desc,
                schema,
                block_size,
                sort_spilling_batch_bytes,
                enable_loser_tree,
                have_order_col,
            },
        )))
    }
}

impl BlockMetaTransform<WindowPartitionMeta> for TransformWindowPartitionSort {
    const NAME: &'static str = "TransformWindowPartitionSort";

    fn transform(&mut self, meta: WindowPartitionMeta) -> Result<DataBlock> {
        if let WindowPartitionMeta::Partitioned { bucket, data } = meta {
            let mut sort_blocks = Vec::with_capacity(data.len());
            for bucket_data in data {
                match bucket_data {
                    WindowPartitionMeta::Spilled(_) => unreachable!(),
                    WindowPartitionMeta::BucketSpilled(_) => unreachable!(),
                    WindowPartitionMeta::Partitioned { .. } => unreachable!(),
                    WindowPartitionMeta::Spilling(_) => unreachable!(),
                    WindowPartitionMeta::Payload(p) => {
                        debug_assert!(bucket == p.bucket);
                        let sort_block = DataBlock::sort(&p.data, &self.sort_desc, None)?;
                        sort_blocks.push(sort_block);
                    }
                }
            }

            sort_blocks = sort_merge(
                self.schema.clone(),
                self.block_size,
                self.sort_desc.clone(),
                sort_blocks,
                self.sort_spilling_batch_bytes,
                self.enable_loser_tree,
                self.have_order_col,
            )?;

            return DataBlock::concat(&sort_blocks);
        }

        Err(ErrorCode::Internal(
            "TransformWindowPartitionSort only recv WindowPartitionMeta::Partitioned",
        ))
    }
}
