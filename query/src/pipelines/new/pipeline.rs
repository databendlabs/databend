use common_exception::Result;
use crate::pipelines::new::pipe::NewPipe;

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
        // TODO: add resize processor;
        unimplemented!()
    }
}
