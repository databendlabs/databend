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
        // ResizeProcessor {}
        unimplemented!()
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
    fn event(&mut self) -> Result<Event> {
        todo!()
    }
}
