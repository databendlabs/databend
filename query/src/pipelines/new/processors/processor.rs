use std::cell::UnsafeCell;
use std::sync::Arc;
use common_exception::{ErrorCode, Result};
use common_base::Runtime;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};

pub enum PrepareState {
    NeedData,
    NeedConsume,
    Sync,
    Async,
    Finished,
}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor {
    fn prepare(&mut self) -> Result<PrepareState>;

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented process."))
    }

    // Asynchronous work.
    async fn async_process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented async_process."))
    }

    // TODO: maybe remove it?
    fn connect_input(&mut self, input: InputPort) -> Result<()> {
        unimplemented!("")
    }

    // TODO: maybe remove it?
    fn connect_output(&mut self, output: OutputPort) -> Result<()> {
        unimplemented!("")
    }
}

pub type ProcessorPtr = Arc<UnsafeCell<dyn Processor>>;

pub type Processors = Vec<Box<dyn Processor>>;
