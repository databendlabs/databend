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
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::local_block_meta_serde;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use futures::future::BoxFuture;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
pub struct TransformAggregateSerializer {
    params: Arc<AggregatorParams>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    input_data: Option<SerializeAggregateStream>,
}

impl TransformAggregateSerializer {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            TransformAggregateSerializer {
                input,
                output,
                params,
                input_data: None,
                output_data: None,
            },
        )))
    }
}

impl Processor for TransformAggregateSerializer {
    fn name(&self) -> String {
        String::from("TransformAggregateSerializer")
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

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            return self.transform_input_data(data_block);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(stream) = &mut self.input_data {
            self.output_data = Option::transpose(stream.next())?;

            if self.output_data.is_none() {
                self.input_data = None;
            }
        }

        Ok(())
    }
}

impl TransformAggregateSerializer {
    fn transform_input_data(&mut self, mut data_block: DataBlock) -> Result<Event> {
        debug_assert!(data_block.is_empty());

        let Some(AggregateMeta::AggregatePayload(p)) = data_block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
        else {
            unreachable!()
        };

        self.input_data = Some(SerializeAggregateStream::create(
            &self.params,
            SerializePayload::AggregatePayload(p),
        ));
        Ok(Event::Sync)
    }
}

pub enum SerializePayload {
    AggregatePayload(AggregatePayload),
}

pub enum FlightSerialized {
    DataBlock(DataBlock),
    Future(BoxFuture<'static, Result<DataBlock>>),
}

unsafe impl Sync for FlightSerialized {}

pub struct FlightSerializedMeta {
    pub serialized_blocks: Vec<FlightSerialized>,
}

impl FlightSerializedMeta {
    pub fn create(blocks: Vec<FlightSerialized>) -> BlockMetaInfoPtr {
        Box::new(FlightSerializedMeta {
            serialized_blocks: blocks,
        })
    }
}

impl std::fmt::Debug for FlightSerializedMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("FlightSerializedMeta").finish()
    }
}

local_block_meta_serde!(FlightSerializedMeta);

#[typetag::serde(name = "exchange_shuffle")]
impl BlockMetaInfo for FlightSerializedMeta {}

pub struct SerializeAggregateStream {
    _params: Arc<AggregatorParams>,
    pub payload: Pin<Box<SerializePayload>>,
    flush_state: PayloadFlushState,
    end_iter: bool,
    nums: usize,
}

unsafe impl Send for SerializeAggregateStream {}

unsafe impl Sync for SerializeAggregateStream {}

impl SerializeAggregateStream {
    pub fn create(params: &Arc<AggregatorParams>, payload: SerializePayload) -> Self {
        let payload = Box::pin(payload);

        SerializeAggregateStream {
            payload,
            flush_state: PayloadFlushState::default(),
            _params: params.clone(),
            end_iter: false,
            nums: 0,
        }
    }
}

impl Iterator for SerializeAggregateStream {
    type Item = Result<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        Result::transpose(self.next_impl())
    }
}

impl SerializeAggregateStream {
    fn next_impl(&mut self) -> Result<Option<DataBlock>> {
        if self.end_iter {
            return Ok(None);
        }

        let SerializePayload::AggregatePayload(p) = self.payload.as_ref().get_ref();
        match p.payload.aggregate_flush(&mut self.flush_state)? {
            Some(block) => {
                self.nums += 1;
                Ok(Some(block.add_meta(Some(
                    AggregateSerdeMeta::create_agg_payload(p.bucket, p.max_partition_count, false),
                ))?))
            }
            None => {
                self.end_iter = true;
                // always return at least one block
                if self.nums == 0 {
                    self.nums += 1;
                    let block = p.payload.empty_block(1);
                    Ok(Some(block.add_meta(Some(
                        AggregateSerdeMeta::create_agg_payload(
                            p.bucket,
                            p.max_partition_count,
                            true,
                        ),
                    ))?))
                } else {
                    Ok(None)
                }
            }
        }
    }
}
