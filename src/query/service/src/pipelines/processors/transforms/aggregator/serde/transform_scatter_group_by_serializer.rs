// Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::io::flight::default_ipc_fields;
use common_arrow::arrow::io::flight::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;

use crate::api::serialize_block;
use crate::api::ExchangeShuffleMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::transform_group_by_serializer::serialize_group_by;
use crate::pipelines::processors::transforms::aggregator::serde::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;

pub struct TransformScatterGroupBySerializer<Method: HashMethodBounds> {
    method: Method,
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,
    local_pos: usize,
}

impl<Method: HashMethodBounds> TransformScatterGroupBySerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        schema: DataSchemaRef,
        local_pos: usize,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformScatterGroupBySerializer {
                method,
                local_pos,
                ipc_fields,
                options: WriteOptions { compression: None },
            },
        )))
    }
}

impl<Method> BlockMetaTransform<ExchangeShuffleMeta> for TransformScatterGroupBySerializer<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformScatterGroupBySerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut new_blocks = Vec::with_capacity(meta.blocks.len());

        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if index == self.local_pos {
                new_blocks.push(block);
                continue;
            }

            if let Some(meta) = block
                .take_meta()
                .and_then(AggregateMeta::<Method, ()>::downcast_from)
            {
                new_blocks.push(match meta {
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::Spilled(payload) => {
                        let bucket = payload.bucket;
                        let data_block =
                            DataBlock::empty_with_meta(AggregateSerdeMeta::create_spilled(
                                bucket,
                                payload.location,
                                payload.columns_layout,
                            ));

                        serialize_block(bucket, data_block, &self.ipc_fields, &self.options)?
                    }
                    AggregateMeta::HashTable(payload) => {
                        let bucket = payload.bucket;
                        let data_block = serialize_group_by(&self.method, payload)?;
                        let data_block =
                            data_block.add_meta(Some(AggregateSerdeMeta::create(bucket)))?;
                        serialize_block(bucket, data_block, &self.ipc_fields, &self.options)?
                    }
                });

                continue;
            }

            new_blocks.push(block);
        }

        Ok(DataBlock::empty_with_meta(ExchangeShuffleMeta::create(
            new_blocks,
        )))
    }
}
