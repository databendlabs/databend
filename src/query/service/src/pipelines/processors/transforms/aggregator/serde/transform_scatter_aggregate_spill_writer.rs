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

use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use futures_util::future::BoxFuture;
use opendal::Operator;

use crate::api::ExchangeShuffleMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::transform_aggregate_spill_writer::spilling_aggregate_payload;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::AggregatorParams;

pub struct TransformScatterAggregateSpillWriter<Method: HashMethodBounds> {
    method: Method,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    location_prefix: String,
    params: Arc<AggregatorParams>,
    input_data_block: Option<DataBlock>,
    output_data_block: Option<DataBlock>,
    spilling_futures: Vec<BoxFuture<'static, Result<()>>>,
}

impl<Method: HashMethodBounds> TransformScatterAggregateSpillWriter<Method> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
        location_prefix: String,
        params: Arc<AggregatorParams>,
    ) -> Box<dyn Processor> {
        Box::new(TransformScatterAggregateSpillWriter::<Method> {
            method,
            input,
            output,
            params,
            operator,
            location_prefix,
            input_data_block: None,
            output_data_block: None,
            spilling_futures: vec![],
        })
    }
}

#[async_trait::async_trait]
impl<Method: HashMethodBounds> Processor for TransformScatterAggregateSpillWriter<Method> {
    fn name(&self) -> String {
        String::from("TransformScatterAggregateSpillWriter")
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

        if !self.spilling_futures.is_empty() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if let Some(output_block) = self.output_data_block.take() {
            self.output.push_data(Ok(output_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data_block.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data_block = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.input_data_block.take() {
            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(ExchangeShuffleMeta::downcast_from)
            {
                let mut new_blocks = Vec::with_capacity(block_meta.blocks.len());

                for mut block in block_meta.blocks {
                    let block_meta = block
                        .get_meta()
                        .and_then(AggregateMeta::<Method, usize>::downcast_ref_from);

                    if matches!(block_meta, Some(AggregateMeta::Spilling(_))) {
                        if let Some(AggregateMeta::Spilling(payload)) = block
                            .take_meta()
                            .and_then(AggregateMeta::<Method, usize>::downcast_from)
                        {
                            let (output_block, spilling_future) = spilling_aggregate_payload(
                                self.operator.clone(),
                                &self.method,
                                &self.location_prefix,
                                &self.params,
                                payload,
                            )?;

                            new_blocks.push(output_block);
                            self.spilling_futures.push(Box::pin(spilling_future));
                            continue;
                        }
                    }

                    new_blocks.push(block);
                }

                self.output_data_block = Some(DataBlock::empty_with_meta(
                    ExchangeShuffleMeta::create(new_blocks),
                ));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let spilling_futures = std::mem::take(&mut self.spilling_futures);
        futures::future::try_join_all(spilling_futures).await?;
        Ok(())
    }
}
