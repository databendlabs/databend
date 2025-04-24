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
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;

struct TransformSortRoute {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    cur_input: usize,
}

impl TransformSortRoute {
    fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>) -> Self {
        Self {
            inputs,
            output,
            cur_input: 0,
        }
    }

    fn process(&mut self) -> Result<()> {
        for (i, input) in self.inputs.iter().enumerate() {
            if i != self.cur_input {
                if !input.is_finished() && !input.has_data() {
                    input.set_need_data();
                }
                continue;
            }

            if input.is_finished() {
                self.cur_input = i + 1;
                continue;
            }

            match input.pull_data() {
                Some(data) => self.output.push_data(data),
                None => input.set_need_data(),
            }
        }

        Ok(())
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

pub fn add_range_shuffle_route(pipeline: &mut Pipeline) -> Result<()> {
    let inputs = pipeline.output_len();
    let inputs_port = (0..inputs).map(|_| InputPort::create()).collect::<Vec<_>>();
    let output = OutputPort::create();

    let processor = ProcessorPtr::create(Box::new(TransformSortRoute::new(
        inputs_port.clone(),
        output.clone(),
    )));

    let pipe = Pipe::create(inputs, 1, vec![PipeItem::create(
        processor,
        inputs_port,
        vec![output],
    )]);

    pipeline.add_pipe(pipe);
    Ok(())
}
