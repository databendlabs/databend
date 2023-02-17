// Copyright 2023 Datafuse Labs.
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
use std::collections::HashSet;
use std::sync::Arc;

use common_exception::Result;

use crate::processors::port::InputPort;
use crate::processors::port::OutputPort;
use crate::processors::processor::Event;
use crate::processors::Processor;

pub struct ShuffleProcessor {
    channel: Vec<(Arc<InputPort>, Arc<OutputPort>)>,
    not_finished: HashSet<usize>,
}

impl ShuffleProcessor {
    pub fn create(channel: Vec<(Arc<InputPort>, Arc<OutputPort>)>) -> Self {
        let not_finished = (0..channel.len()).collect();
        ShuffleProcessor {
            channel,
            not_finished,
        }
    }
}

#[async_trait::async_trait]
impl Processor for ShuffleProcessor {
    fn name(&self) -> String {
        "Shuffle".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let mut finished = Vec::with_capacity(self.not_finished.len());
        let mut need_data = true;
        for i in self.not_finished.iter() {
            let (input, output) = &self.channel[*i];
            match (input.is_finished(), output.is_finished()) {
                (true, true) => finished.push(*i),
                (true, false) => {
                    output.finish();
                    finished.push(*i);
                }
                (false, true) => {
                    input.finish();
                    finished.push(*i);
                }
                (false, false) => {
                    if !output.can_push() {
                        need_data = false;
                        break;
                    }
                    if input.has_data() {
                        need_data = false;
                        output.push_data(input.pull_data().unwrap());
                    }
                }
            }
        }
        if finished.len() == self.not_finished.len() {
            return Ok(Event::Finished);
        }

        for i in finished {
            self.not_finished.remove(&i);
        }

        if need_data {
            Ok(Event::NeedData)
        } else {
            Ok(Event::NeedConsume)
        }
    }
}
