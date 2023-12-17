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
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
// TODO Shuffle?
pub struct BroadcastProcessor {
    input_port: Arc<InputPort>,
    output_ports: Vec<Arc<OutputPort>>,
    input_data: Option<Result<DataBlock>>,
    output_index: usize,
}

impl BroadcastProcessor {
    #[allow(dead_code)]
    pub fn new(num_outputs: usize) -> Self {
        let mut output_ports = Vec::with_capacity(num_outputs);
        for _ in 0..num_outputs {
            output_ports.push(OutputPort::create())
        }
        let input_port = InputPort::create();

        Self {
            input_port,
            output_ports,
            input_data: None,
            output_index: 0,
        }
    }

    #[allow(dead_code)]
    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let outputs = self.output_ports.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], outputs)
    }
}

impl Processor for BroadcastProcessor {
    fn name(&self) -> String {
        "BroadcastProcessor".to_owned()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let finished = self.input_port.is_finished() && self.input_data.is_none();

        if finished {
            for o in &self.output_ports {
                o.finish()
            }
            return Ok(Event::Finished);
        }

        if self.input_port.has_data() && self.input_data.is_none() {
            self.input_data = Some(self.input_port.pull_data().unwrap());
            // reset output index
            self.output_index = 0;
        }

        if let Some(data) = &self.input_data {
            while self.output_index < self.output_ports.len() {
                let output = &self.output_ports[self.output_index];
                if output.can_push() {
                    self.output_index += 1;
                    output.push_data(data.clone());
                } else {
                    self.input_port.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }
            }

            if self.output_index == self.output_ports.len() {
                self.input_data = None;
            };
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }
}
