use std::sync::Arc;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub enum NewPipe {
    SimplePipe {
        processors: Vec<ProcessorPtr>,
        inputs_port: Vec<Arc<InputPort>>,
        outputs_port: Vec<Arc<OutputPort>>,
    },
    ResizePipe {
        processor: ProcessorPtr,
        inputs_port: Vec<Arc<InputPort>>,
        outputs_port: Vec<Arc<OutputPort>>,
    },
}

impl NewPipe {
    pub fn size(&self) -> usize {
        match self {
            NewPipe::SimplePipe { outputs_port, .. } => outputs_port.len(),
            NewPipe::ResizePipe { outputs_port, .. } => outputs_port.len(),
        }
    }
}

pub struct SourcePipeBuilder {
    processors: Vec<ProcessorPtr>,
    outputs_port: Vec<Arc<OutputPort>>,
}

impl SourcePipeBuilder {
    pub fn create() -> SourcePipeBuilder {
        SourcePipeBuilder {
            processors: vec![],
            outputs_port: vec![],
        }
    }

    pub fn finalize(self) -> NewPipe {
        assert_eq!(self.processors.len(), self.outputs_port.len());
        NewPipe::SimplePipe {
            processors: self.processors,
            inputs_port: vec![],
            outputs_port: self.outputs_port,
        }
    }

    pub fn add_source(&mut self, source: ProcessorPtr, output_port: Arc<OutputPort>) {
        self.processors.push(source);
        self.outputs_port.push(output_port);
    }
}
