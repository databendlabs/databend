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
use std::collections::VecDeque;
use std::intrinsics::unlikely;
use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::ReclusterSampleMeta;
use crate::pipelines::processors::transforms::SampleState;

pub struct TransformRangePartitionIndexer {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    state: Arc<SampleState>,
    input_data: Vec<DataBlock>,
    output_data: VecDeque<DataBlock>,
    bounds: Vec<Vec<u8>>,
    max_value: Option<Vec<u8>>,

    start: Instant,
}

impl TransformRangePartitionIndexer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: Arc<SampleState>,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            state,
            input_data: vec![],
            output_data: VecDeque::new(),
            bounds: vec![],
            max_value: None,
            start: Instant::now(),
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformRangePartitionIndexer {
    fn name(&self) -> String {
        "TransformRangePartitionIndexer".to_owned()
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

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.input_data.is_empty() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            assert!(self.state.done.is_notified());
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut input_data = self.input.pull_data().unwrap()?;
        let meta = input_data
            .take_meta()
            .and_then(ReclusterSampleMeta::downcast_from)
            .expect("require a ReclusterSampleMeta");
        self.input_data = meta.blocks;
        self.state.merge_sample(meta.sample_values)?;
        log::info!("Recluster range partition: {:?}", self.start.elapsed());
        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        let start = Instant::now();
        if let Some(mut block) = self.input_data.pop() {
            let num_rows = block.num_rows();
            let mut builder = Vec::with_capacity(num_rows);
            let last_col = block.get_last_column().as_binary().unwrap();
            if let Some(max_value) = self.max_value.as_ref() {
                let bound_len = self.bounds.len();
                for index in 0..num_rows {
                    let val = unsafe { last_col.index_unchecked(index) };
                    if unlikely(val >= max_value.as_slice()) {
                        let range_id = bound_len + 1;
                        builder.push(range_id as u64);
                        continue;
                    }

                    let idx = self
                        .bounds
                        .binary_search_by(|b| b.as_slice().cmp(val))
                        .unwrap_or_else(|i| i);
                    builder.push(idx as u64);
                }
            } else {
                for index in 0..num_rows {
                    let val = unsafe { last_col.index_unchecked(index) };
                    let idx = self
                        .bounds
                        .binary_search_by(|b| b.as_slice().cmp(val))
                        .unwrap_or_else(|i| i);
                    builder.push(idx as u64);
                }
            }
            block.pop_columns(1);
            block.add_column(UInt64Type::from_data(builder));
            self.output_data.push_back(block);
        }

        log::info!("Recluster range output: {:?}", start.elapsed());
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        self.state.done.notified().await;
        (self.bounds, self.max_value) = self.state.get_bounds();
        Ok(())
    }
}
