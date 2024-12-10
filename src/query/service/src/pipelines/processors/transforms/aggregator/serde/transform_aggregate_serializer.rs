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
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_functions::aggregates::StateAddr;
use databend_common_hashtable::HashtableEntryRefLike;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use super::SerializePayload;
use crate::pipelines::processors::transforms::aggregator::create_state_serializer;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
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
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::<usize>::downcast_from(block_meta) {
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                    AggregateMeta::AggregatePayload(p) => {
                        self.input_data = Some(SerializeAggregateStream::create(
                            &self.params,
                            SerializePayload::<usize>::AggregatePayload(p),
                        ));
                        return Ok(Event::Sync);
                    }
                }
            }
        }

        unreachable!()
    }
}

pub struct SerializeAggregateStream {
    params: Arc<AggregatorParams>,
    pub payload: Pin<Box<SerializePayload<usize>>>,
    flush_state: PayloadFlushState,
    end_iter: bool,
}

unsafe impl Send for SerializeAggregateStream {}

unsafe impl Sync for SerializeAggregateStream {}

impl SerializeAggregateStream {
    pub fn create(params: &Arc<AggregatorParams>, payload: SerializePayload<usize>) -> Self {
        unsafe {
            let payload = Box::pin(payload);

            SerializeAggregateStream {
                payload,
                flush_state: PayloadFlushState::default(),
                params: params.clone(),
                end_iter: false,
            }
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

        match self.payload.as_ref().get_ref() {
            SerializePayload::AggregatePayload(p) => {
                let block = p.payload.aggregate_flush(&mut self.flush_state)?;

                if block.is_none() {
                    self.end_iter = true;
                }

                match block {
                    Some(block) => Ok(Some(block.add_meta(Some(
                        AggregateSerdeMeta::create_agg_payload(p.bucket, p.max_partition_count),
                    ))?)),
                    None => Ok(None),
                }
            }
        }
    }
}
