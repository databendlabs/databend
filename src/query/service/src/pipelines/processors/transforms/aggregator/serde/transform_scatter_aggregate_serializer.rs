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

use std::any::Any;
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
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;

use crate::api::serialize_block;
use crate::api::ExchangeShuffleMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::serde_meta::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::serde::transform_aggregate_serializer::serialize_aggregate;
use crate::pipelines::processors::transforms::aggregator::serde::transform_aggregate_serializer::SerializeAggregateStream;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::AggregatorParams;

pub struct TransformScatterAggregateSerializer<Method: HashMethodBounds> {
    method: Method,
    local_pos: usize,
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,
    params: Arc<AggregatorParams>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Vec<DataBlock>,
    input_data: Vec<Option<SerializeAggregateStream<Method>>>,
}

impl<Method: HashMethodBounds> TransformScatterAggregateSerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        schema: DataSchemaRef,
        local_pos: usize,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = schema.to_arrow();
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);

        Ok(ProcessorPtr::create(Box::new(
            TransformScatterAggregateSerializer {
                method,
                params,
                input,
                output,
                local_pos,
                ipc_fields,
                options: Default::default(),
                input_data: vec![],
                output_data: vec![],
            },
        )))
    }
}

impl<Method: HashMethodBounds> Processor for TransformScatterAggregateSerializer<Method> {
    fn name(&self) -> String {
        String::from("TransformScatterAggregateSerializer")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if !self.output_data.is_empty() {
            let blocks = std::mem::take(&mut self.output_data);
            let block = DataBlock::empty_with_meta(ExchangeShuffleMeta::create(blocks));
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        if !self.input_data.is_empty() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            debug_assert!(data_block.is_empty());
            if let Some(block_meta) = data_block.take_meta() {
                if let Some(block_meta) = ExchangeShuffleMeta::downcast_from(block_meta) {
                    for (index, mut block) in block_meta.blocks.into_iter().enumerate() {
                        if index == self.local_pos {
                            self.input_data.push(None);
                            self.output_data.push(block);
                            continue;
                        }

                        if let Some(meta) = block
                            .take_meta()
                            .and_then(AggregateMeta::<Method, usize>::downcast_from)
                        {
                            match meta {
                                AggregateMeta::Spilling(_) => unreachable!(),
                                AggregateMeta::Partitioned { .. } => unreachable!(),
                                AggregateMeta::Serialized(_) => unreachable!(),
                                AggregateMeta::Spilled(payload) => {
                                    let bucket = payload.bucket;
                                    let data_block = DataBlock::empty_with_meta(
                                        AggregateSerdeMeta::create_spilled(
                                            bucket,
                                            payload.location,
                                            payload.columns_layout,
                                        ),
                                    );

                                    self.input_data.push(None);
                                    self.output_data.push(serialize_block(
                                        bucket,
                                        data_block,
                                        &self.ipc_fields,
                                        &self.options,
                                    )?);
                                }
                                AggregateMeta::HashTable(payload) => {
                                    self.output_data.push(DataBlock::empty());
                                    self.input_data.push(Some(SerializeAggregateStream::create(
                                        &self.method,
                                        &self.params,
                                        payload,
                                    )));
                                }
                            };

                            continue;
                        }

                        self.input_data.push(None);
                        self.output_data.push(block);
                    }

                    return Ok(Event::Sync);
                }
            }

            unreachable!()
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let mut has_next = false;
        let output_is_empty = self.output_data.is_empty();
        for (index, stream) in self.input_data.iter_mut().enumerate() {
            if self.output_data.len() <= index {
                self.output_data.push(DataBlock::empty());
            }

            if let Some(stream) = stream {
                if let Some(data_block) = stream.next() {
                    has_next = true;
                    let bucket = stream.payload.bucket;
                    self.output_data[index] =
                        serialize_block(bucket, data_block?, &self.ipc_fields, &self.options)?;
                }
            }
        }

        if !has_next {
            self.input_data.clear();

            if output_is_empty {
                self.output_data.clear();
            }
        }

        Ok(())
    }
}

impl<Method> BlockMetaTransform<ExchangeShuffleMeta> for TransformScatterAggregateSerializer<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformScatterAggregateSerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut new_blocks = Vec::with_capacity(meta.blocks.len());

        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if index == self.local_pos {
                new_blocks.push(block);
                continue;
            }

            if let Some(meta) = block
                .take_meta()
                .and_then(AggregateMeta::<Method, usize>::downcast_from)
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
                        let data_block = serialize_aggregate(&self.method, &self.params, payload)?;
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
