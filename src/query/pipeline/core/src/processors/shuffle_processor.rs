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
use crate::processors::EventCause;
use crate::processors::InputPort;
use crate::processors::OutputPort;
use crate::processors::Processor;

pub struct ShuffleProcessor {
    input2output: Vec<usize>,
    output2input: Vec<usize>,

    finished_port: usize,
    inputs: Vec<(bool, Arc<InputPort>)>,
    outputs: Vec<(bool, Arc<OutputPort>)>,
}

impl ShuffleProcessor {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        outputs: Vec<Arc<OutputPort>>,
        edges: Vec<usize>,
    ) -> Self {
        let len = edges.len();
        debug_assert!({
            let mut sorted = edges.clone();
            sorted.sort();
            let expected = (0..len).collect::<Vec<_>>();
            sorted == expected
        });

        let mut input2output = vec![0_usize; edges.len()];
        let mut output2input = vec![0_usize; edges.len()];

        for (input, output) in edges.into_iter().enumerate() {
            input2output[input] = output;
            output2input[output] = input;
        }

        ShuffleProcessor {
            input2output,
            output2input,
            finished_port: 0,
            inputs: inputs.into_iter().map(|x| (false, x)).collect(),
            outputs: outputs.into_iter().map(|x| (false, x)).collect(),
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

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        let ((input_finished, input), (output_finished, output)) = match cause {
            EventCause::Other => unreachable!(),
            EventCause::Input(index) => (
                &mut self.inputs[index],
                &mut self.outputs[self.input2output[index]],
            ),
            EventCause::Output(index) => (
                &mut self.inputs[self.output2input[index]],
                &mut self.outputs[index],
            ),
        };

        if output.is_finished() {
            input.finish();

            if !*input_finished {
                *input_finished = true;
                self.finished_port += 1;
            }

            if !*output_finished {
                *output_finished = true;
                self.finished_port += 1;
            }

            return match self.finished_port == (self.inputs.len() + self.outputs.len()) {
                true => Ok(Event::Finished),
                false => Ok(Event::NeedConsume),
            };
        }

        if !output.can_push() {
            input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if input.has_data() {
            output.push_data(input.pull_data().unwrap());
            return Ok(Event::NeedConsume);
        }

        if input.is_finished() {
            output.finish();

            if !*input_finished {
                *input_finished = true;
                self.finished_port += 1;
            }

            if !*output_finished {
                *output_finished = true;
                self.finished_port += 1;
            }

            return match self.finished_port == (self.inputs.len() + self.outputs.len()) {
                true => Ok(Event::Finished),
                false => Ok(Event::NeedConsume),
            };
        }

        input.set_need_data();
        Ok(Event::NeedData)
    }
}
