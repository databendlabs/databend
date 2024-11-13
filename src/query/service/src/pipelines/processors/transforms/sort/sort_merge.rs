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

pub struct TransformSortRangeMerge {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    cur: usize,
}

impl TransformSortRangeMerge {
    pub fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>) -> Self {
        TransformSortRangeMerge {
            inputs,
            output,
            cur: 0,
        }
    }
}

impl Processor for TransformSortRangeMerge {
    fn name(&self) -> String {
        "TransformSortRangeMerge".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for i in self.cur..self.inputs.len() {
                self.inputs[i].finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.inputs[self.cur].set_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.inputs[self.cur].has_data() {
            let data = self.inputs[self.cur].pull_data().unwrap()?;
            self.inputs[self.cur].set_need_data();
            self.output.push_data(Ok(data));
            return Ok(Event::NeedConsume);
        }

        if self.inputs[self.cur].is_finished() {
            self.cur += 1;
            if self.cur >= self.inputs.len() {
                self.output.finish();
                return Ok(Event::Finished);
            }
        }

        self.inputs[self.cur].set_need_data();
        Ok(Event::NeedData)
    }
}

pub fn add_range_shuffle_merge(pipeline: &mut Pipeline) -> Result<()> {
    let inputs = pipeline.output_len();
    let inputs_port = (0..inputs).map(|_| InputPort::create()).collect::<Vec<_>>();
    let output = OutputPort::create();

    let processor = ProcessorPtr::create(Box::new(TransformSortRangeMerge::new(
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
