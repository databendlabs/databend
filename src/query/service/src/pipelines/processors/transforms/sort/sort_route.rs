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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::SortBound;

pub struct TransformSortRoute {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,

    input_data: Vec<Option<(DataBlock, SortBound)>>,
    cur_index: u32,
}

impl TransformSortRoute {
    pub(super) fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>) -> Self {
        Self {
            input_data: vec![None; inputs.len()],
            cur_index: 0,
            inputs,
            output,
        }
    }

    fn process(&mut self) -> Result<Event> {
        for (input, data) in self.inputs.iter().zip(self.input_data.iter_mut()) {
            let meta = match data {
                Some((_, meta)) => *meta,
                None => {
                    let Some(mut block) = input.pull_data().transpose()? else {
                        input.set_need_data();
                        continue;
                    };
                    input.set_need_data();

                    let meta = block
                        .take_meta()
                        .and_then(SortBound::downcast_from)
                        .expect("require a SortBound");

                    data.insert((block, meta)).1
                }
            };

            log::debug!(
                "input {:?}, meta: {meta:?}, cur_index {}",
                input as *const _,
                self.cur_index
            );

            assert!(!input.is_finished() || meta.next.is_none());
            if meta.index == self.cur_index {
                let (block, meta) = data.take().unwrap();
                self.output.push_data(Ok(block));
                if meta.next.is_none() {
                    self.cur_index += 1;
                }
                return Ok(Event::NeedConsume);
            }
        }

        Ok(Event::NeedData)
    }
}

impl Processor for TransformSortRoute {
    fn name(&self) -> String {
        "SortRoute".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            for input in &self.inputs {
                input.set_not_need_data();
            }
            return Ok(Event::NeedConsume);
        }

        self.process()?;

        if self.inputs.iter().all(|input| input.is_finished()) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }
}
