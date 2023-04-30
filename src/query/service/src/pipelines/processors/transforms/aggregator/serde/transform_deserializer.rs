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
use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::serde_meta::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::serde::BUCKET_TYPE;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;

pub struct TransformDeserializer<Method: HashMethodBounds, V: Send + Sync + 'static> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    _phantom: PhantomData<(Method, V)>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> TransformDeserializer<Method, V> {
    pub fn try_create(input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformDeserializer::<
            Method,
            V,
        > {
            input,
            output,
            _phantom: Default::default(),
        })))
    }
}

#[async_trait::async_trait]
impl<Method, V> Processor for TransformDeserializer<Method, V>
where
    Method: HashMethodBounds,
    V: Send + Sync + 'static,
{
    fn name(&self) -> String {
        String::from("TransformAggregateDeserializer")
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

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block.take_meta() {
                let option = AggregateSerdeMeta::downcast_ref_from(&block_meta);
                if option.is_some() {
                    let meta = AggregateSerdeMeta::downcast_from(block_meta).unwrap();

                    self.output.push_data(Ok(DataBlock::empty_with_meta(
                        match meta.typ == BUCKET_TYPE {
                            true => AggregateMeta::<Method, V>::create_serialized(
                                meta.bucket,
                                data_block,
                            ),
                            false => AggregateMeta::<Method, V>::create_spilled(
                                meta.bucket,
                                meta.location.unwrap(),
                                meta.columns_layout,
                            ),
                        },
                    )));

                    return Ok(Event::NeedConsume);
                }

                self.output.push_data(data_block.add_meta(Some(block_meta)));
                return Ok(Event::NeedConsume);
            }

            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }
}

pub type TransformGroupByDeserializer<Method> = TransformDeserializer<Method, ()>;
pub type TransformAggregateDeserializer<Method> = TransformDeserializer<Method, usize>;
