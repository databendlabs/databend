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
use std::assert_matches::debug_assert_matches;
use std::fmt::Debug;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use super::SortBound;
use super::SortBoundNext;
use crate::Transform;

pub struct TransformSortRoute {
    inputs: Vec<Input>,
    output: Arc<OutputPort>,

    cur_index: u32,
}

struct Input {
    port: Arc<InputPort>,
    data: Option<(DataBlock, SortBound)>,
    index: u32,
}

impl Input {
    fn is_finished(&self) -> bool {
        self.port.is_finished() && self.data.is_none()
    }

    fn pull(&mut self) -> Result<Option<SortBound>> {
        Ok(match &self.data {
            Some((_, meta)) => Some(*meta),
            None => {
                let Some(mut block) = self.port.pull_data().transpose()? else {
                    self.port.set_need_data();
                    return Ok(None);
                };
                self.port.set_need_data();

                let meta = block
                    .take_meta()
                    .and_then(SortBound::downcast_from)
                    .expect("require a SortBound");

                debug_assert_matches!(
                    meta.next,
                    SortBoundNext::Skip | SortBoundNext::More | SortBoundNext::Last
                );
                assert_eq!(meta.index, self.index);
                if meta.next.is_skip() && block.is_empty() {
                    self.index = meta.index + 1;
                    return Ok(None);
                }
                assert!(!block.is_empty());
                Some(self.data.insert((block, meta)).1)
            }
        })
    }
}

impl Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Input")
            .field_with("port", |f| {
                f.debug_struct("InputPort")
                    .field("has_data", &self.port.has_data())
                    .field("is_finished", &self.port.is_finished())
                    .finish()
            })
            .field(
                "data",
                &self
                    .data
                    .as_ref()
                    .map(|(block, meta)| (block.num_rows(), meta)),
            )
            .field("index", &self.index)
            .finish()
    }
}

impl TransformSortRoute {
    pub fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>) -> Self {
        Self {
            inputs: inputs
                .into_iter()
                .map(|port| Input {
                    port,
                    data: None,
                    index: 0,
                })
                .collect(),
            cur_index: 0,
            output,
        }
    }

    fn process(&mut self) -> Result<Event> {
        if self.try_output()? {
            return Ok(Event::NeedConsume);
        }

        for input in &self.inputs {
            if input.index <= self.cur_index && !input.port.is_finished() {
                assert!(input.data.is_none());
                return Ok(Event::NeedData);
            }
        }

        let Some(min_index) = self
            .inputs
            .iter()
            .filter_map(|input| (!input.is_finished()).then_some(input.index))
            .min()
        else {
            self.output.finish();
            return Ok(Event::Finished);
        };
        assert!(min_index > self.cur_index);
        self.cur_index = min_index;

        if self.try_output()? {
            return Ok(Event::NeedConsume);
        }

        if self.inputs.iter().all(Input::is_finished) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }

    fn try_output(&mut self) -> Result<bool> {
        let n = self.inputs.len();
        for i in 0..=n {
            let input = &mut self.inputs[i % n];

            let Some(meta) = input.pull()? else {
                continue;
            };
            assert_eq!(meta.index, input.index);

            log::debug!(
                "input {:?}, input_index: {}, meta: {meta:?}, cur_index {}",
                input as *const _,
                input.index,
                self.cur_index
            );

            assert!(meta.index >= self.cur_index);
            if meta.index == self.cur_index {
                let (block, meta) = input.data.take().unwrap();
                if meta.next.is_last() {
                    input.index += 1;
                    self.cur_index += 1;
                }
                self.output.push_data(Ok(block));
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl Processor for TransformSortRoute {
    fn name(&self) -> String {
        "SortRoute".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    #[fastrace::trace(name = "TransformSortRoute::event")]
    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.port.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            for input in &self.inputs {
                input.port.set_not_need_data();
            }
            return Ok(Event::NeedConsume);
        }

        self.process()
    }
}

pub struct SortDummyRoute {}

impl Transform for SortDummyRoute {
    const NAME: &'static str = "SortDummyRoute";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        data.take_meta()
            .and_then(SortBound::downcast_from)
            .expect("require a SortBound");
        data.pop_columns(1);
        Ok(data)
    }
}
