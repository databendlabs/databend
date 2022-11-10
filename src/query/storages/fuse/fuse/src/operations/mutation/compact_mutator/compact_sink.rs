// Copyright 2022 Datafuse Labs.
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
use common_exception::ErrorCode;
use common_datablocks::{DataBlock, MetaInfos};
use common_storages_table_meta::meta::{Statistics, Location};

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;

enum State {
    None,
    Sort(MetaInfos),
    Commit,
    Abort,
    Finish,
}

pub struct CompactSink {
    state: State,
    unchanged_segments_statistics: Statistics,
    unchanged_segments_locations: Vec<(usize, Location)>,

    inputs: Vec<Arc<InputPort>>,
    cur_input_index: usize,
    
}

impl CompactSink {
    fn get_current_input(&mut self) -> Option<Arc<InputPort>> {
        let mut finished = true;
        let mut index = self.cur_input_index;

        loop {
            let input = &self.inputs[index];

            if !input.is_finished() {
                finished = false;
                input.set_need_data();

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
}

#[async_trait::async_trait]
impl Processor for CompactSink {
    fn name(&self) -> String {
        "CompactSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let current_input = self.get_current_input();
        if let Some(cur_input) = current_input {
            if cur_input.is_finished() {
                self.state = State::Finish;
                return Ok(Event::Finished);
            }

            

            
        }
        todo!()
    }

    fn process(&mut self) -> Result<()> {
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        Ok(())
    }
}

