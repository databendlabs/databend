use std::cell::UnsafeCell;
use std::sync::Arc;

use common_base::Runtime;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;

pub enum Event {
    NeedData,
    NeedConsume,
    Sync,
    Async,
    Finished,
}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor: Send {
    fn event(&mut self) -> Result<Event>;

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented process."))
    }

    // Asynchronous work.
    async fn async_process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented async_process."))
    }
}

pub type ProcessorPtr = Arc<UnsafeCell<dyn Processor>>;

pub type Processors = Vec<ProcessorPtr>;
