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

/// [`ShuffleProcessor`] is used to re-order the input data according to the rule.
///
/// `rule` is a vector of [usize], each element is the index of the output port.
///
/// For example, if the rule is `[1, 2, 0]`, the data flow will be:
///
/// - input 0 -> output 1
/// - input 1 -> output 2
/// - input 2 -> output 0
pub struct ShuffleProcessor {
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,

    rule: Vec<usize>,
}

impl ShuffleProcessor {
    pub fn create(rule: Vec<usize>) -> Self {
        debug_assert!({
            let mut sorted = rule.clone();
            sorted.sort();
            let expected = (0..rule.len()).collect::<Vec<_>>();
            sorted == expected
        });

        let num = rule.len();
        let mut inputs = Vec::with_capacity(num);
        let mut outputs = Vec::with_capacity(num);

        for _ in 0..num {
            inputs.push(InputPort::create());
            outputs.push(OutputPort::create());
        }

        ShuffleProcessor {
            inputs,
            outputs,
            rule,
        }
    }

    pub fn get_inputs(&self) -> &[Arc<InputPort>] {
        &self.inputs
    }

    pub fn get_outputs(&self) -> &[Arc<OutputPort>] {
        &self.outputs
    }

    fn get_ready_inputs(&self) -> Option<Vec<usize>> {
        let mut ready = Vec::with_capacity(self.inputs.len());
        let mut all_finished = true;
        for (i, input) in self.inputs.iter().enumerate() {
            if !input.is_finished() {
                all_finished = false;
                input.set_need_data();
                if input.has_data() {
                    ready.push(i);
                }
            }
        }
        if all_finished { None } else { Some(ready) }
    }

    fn get_ready_outputs(&self) -> Option<Vec<usize>> {
        let mut ready = Vec::with_capacity(self.outputs.len());
        let mut all_finished = true;
        for (i, output) in self.outputs.iter().enumerate() {
            if !output.is_finished() {
                all_finished = false;
                if output.can_push() {
                    ready.push(i);
                }
            }
        }
        if all_finished { None } else { Some(ready) }
    }

    fn finish_inputs(&mut self) {
        for input in &self.inputs {
            input.finish();
        }
    }

    fn finish_outputs(&mut self) {
        for output in &self.outputs {
            output.finish();
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
        let ready_inputs = self.get_ready_inputs();
        let ready_outputs = self.get_ready_outputs();

        if let Some(ready_outputs) = ready_outputs {
            if let Some(ready_inputs) = ready_inputs {
                if ready_inputs.is_empty() {
                    Ok(Event::NeedData)
                } else {
                    for input in ready_inputs {
                        let output = self.rule[input];
                        if ready_outputs.contains(&output) {
                            let block = self.inputs[input].pull_data().unwrap();
                            self.outputs[output].push_data(block);
                        }
                    }
                    Ok(Event::NeedConsume)
                }
            } else {
                self.finish_outputs();
                Ok(Event::Finished)
            }
        } else {
            self.finish_inputs();
            Ok(Event::Finished)
        }
    }
}
