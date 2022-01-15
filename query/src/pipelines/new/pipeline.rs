use common_exception::{ErrorCode, Result};
use crate::pipelines::new::pipe::NewPipe;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::ResizeProcessor;

pub struct NewPipeline {
    pub pipes: Vec<NewPipe>,
}

impl NewPipeline {
    pub fn create() -> NewPipeline {
        NewPipeline { pipes: Vec::new() }
    }

    pub fn add_pipe(&mut self, pipe: NewPipe) {
        self.pipes.push(pipe);
    }

    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        match self.pipes.last() {
            None => Err(ErrorCode::LogicalError("")),
            Some(pipe) => {
                let processor = ResizeProcessor::create(pipe.size(), new_size);
                let inputs_port = processor.get_inputs().to_vec();
                let outputs_port = processor.get_outputs().to_vec();
                self.pipes.push(NewPipe::ResizePipe {
                    inputs_port,
                    outputs_port,
                    processor: ProcessorPtr::create(Box::new(processor)),
                });
                Ok(())
            }
        }
    }
}
