use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub enum NewPipe {
    SimplePipe {
        processors: Vec<ProcessorPtr>,
        inputs_port: Vec<InputPort>,
        outputs_port: Vec<OutputPort>,
    },
    ResizePipe {
        process: ProcessorPtr,
        inputs_port: Vec<InputPort>,
        outputs_port: Vec<OutputPort>,
    },
}

pub struct SourcePipeBuilder {
    processors: Vec<ProcessorPtr>,
    outputs_port: Vec<OutputPort>,
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

    pub fn add_source(&mut self, source: ProcessorPtr, output_port: OutputPort) {
        self.processors.push(source);
        self.outputs_port.push(output_port);
    }
}

