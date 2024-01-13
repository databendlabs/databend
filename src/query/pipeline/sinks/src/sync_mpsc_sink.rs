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
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;

/// Sink with multiple inputs.
pub trait SyncMpscSink: Send {
    const NAME: &'static str;

    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<bool>;
}

pub struct SyncMpscSinker<T: SyncMpscSink + 'static> {
    inner: T,
    finished: bool,
    inputs: Vec<Arc<InputPort>>,
    input_data: Option<DataBlock>,

    cur_input_index: usize,
    called_on_start: bool,
    called_on_finish: bool,
}

impl<T: SyncMpscSink + 'static> SyncMpscSinker<T> {
    pub fn create(inputs: Vec<Arc<InputPort>>, inner: T) -> Box<dyn Processor> {
        for input in &inputs {
            input.set_need_data();
        }
        Box::new(SyncMpscSinker {
            inner,
            inputs,
            finished: false,
            input_data: None,
            cur_input_index: 0,
            called_on_start: false,
            called_on_finish: false,
        })
    }

    fn get_current_input(&mut self) -> Option<Arc<InputPort>> {
        let mut finished = true;
        let mut index = self.cur_input_index;

        loop {
            let input = &self.inputs[index];

            if !input.is_finished() {
                finished = false;

                if input.has_data() {
                    self.cur_input_index = index;
                    return Some(input.clone());
                }
            }

            index += 1;
            if index == self.inputs.len() {
                index = 0;
            }

            if index == self.cur_input_index {
                return match finished {
                    true => Some(input.clone()),
                    false => None,
                };
            }
        }
    }

    fn finish_inputs(&mut self) {
        for input in &self.inputs {
            input.finish();
        }
    }
}

#[async_trait::async_trait]
impl<T: SyncMpscSink + 'static> Processor for SyncMpscSinker<T> {
    fn name(&self) -> String {
        T::NAME.to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Sync);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.finished {
            if !self.called_on_finish {
                return Ok(Event::Sync);
            }

            self.finish_inputs();
            return Ok(Event::Finished);
        }

        match self.get_current_input() {
            Some(input) => {
                if input.is_finished() {
                    // All finished
                    if self.called_on_finish {
                        Ok(Event::Finished)
                    } else {
                        Ok(Event::Sync)
                    }
                } else {
                    let block = input.pull_data().unwrap()?;
                    self.input_data = Some(block);
                    Ok(Event::Sync)
                }
            }
            None => Ok(Event::NeedData),
        }
    }

    fn process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.on_start()?;
        } else if let Some(data_block) = self.input_data.take() {
            self.finished = self.inner.consume(data_block)?;
        } else if !self.called_on_finish {
            self.called_on_finish = true;
            self.inner.on_finish()?;
        }

        Ok(())
    }
}
