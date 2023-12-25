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

use crate::processors::Event;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;

pub struct ShuffleProcessor {
    channel: Vec<(Arc<InputPort>, Arc<OutputPort>)>,
}

impl ShuffleProcessor {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        outputs: Vec<Arc<OutputPort>>,
        rule: Vec<usize>,
    ) -> Self {
        let len = rule.len();
        debug_assert!({
            let mut sorted = rule.clone();
            sorted.sort();
            let expected = (0..len).collect::<Vec<_>>();
            sorted == expected
        });

        let mut channel = Vec::with_capacity(len);
        for (i, input) in inputs.into_iter().enumerate() {
            let output = outputs[rule[i]].clone();
            channel.push((input, output));
        }
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
        for (input, output) in self.channel.iter() {
            if output.is_finished() || input.is_finished() {
                input.finish();
                output.finish();
                continue;
            }
            finished = false;
            input.set_need_data();
            if output.can_push() && input.has_data() {
                output.push_data(input.pull_data().unwrap());
            }
        }
        if finished {
            return Ok(Event::Finished);
        }
        Ok(Event::NeedData)
    }
}
