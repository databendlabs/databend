use std::sync::Arc;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use common_exception::Result;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};

pub struct ResizeProcessor {
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,
}

impl ResizeProcessor {
    pub fn create(inputs: usize, outputs: usize) -> Self {
        let mut inputs_port = Vec::with_capacity(inputs);
        let mut outputs_port = Vec::with_capacity(outputs);

        for _index in 0..inputs {
            inputs_port.push(InputPort::create());
        }

        for _index in 0..outputs {
            outputs_port.push(OutputPort::create());
        }

        ResizeProcessor {
            inputs: inputs_port,
            outputs: outputs_port,
        }
    }

    pub fn get_inputs(&self) -> &[Arc<InputPort>] {
        &self.inputs
    }

    pub fn get_outputs(&self) -> &[Arc<OutputPort>] {
        &self.outputs
    }
}

#[async_trait::async_trait]
impl Processor for ResizeProcessor {
    fn name(&self) -> &'static str {
        "Resize"
    }

    fn event(&mut self) -> Result<Event> {
        todo!()
    }
}
