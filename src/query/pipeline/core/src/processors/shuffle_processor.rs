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
use std::sync::Arc;

use common_exception::Result;

use crate::processors::port::InputPort;
use crate::processors::port::OutputPort;
use crate::processors::processor::Event;
use crate::processors::Processor;

pub struct ShuffleProcessor {
    channel: Vec<(Arc<InputPort>, Arc<OutputPort>)>,
}

impl ShuffleProcessor {
    pub fn create(channel: Vec<(Arc<InputPort>, Arc<OutputPort>)>) -> Self {
        ShuffleProcessor { channel }
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
        let mut finished = true;
        let mut need_data = true;
        for (input, output) in self.channel.iter() {
            match (input.is_finished(), output.is_finished()) {
                (true, true) => {}
                (true, false) => {
                    output.finish();
                }
                (false, true) => {
                    input.finish();
                }
                (false, false) => {
                    finished = false;
                    if !output.can_push() {
                        return Ok(Event::NeedConsume);
                    }
                    if input.has_data() {
                        need_data = false;
                        output.push_data(input.pull_data().unwrap());
                    }
                }
            }
        }

        if finished {
            Ok(Event::Finished)
        } else if need_data {
            Ok(Event::NeedData)
        } else {
            Ok(Event::NeedConsume)
        }
    }
}
