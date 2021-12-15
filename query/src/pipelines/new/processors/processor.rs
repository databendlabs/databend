use common_exception::Result;
use common_base::Runtime;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};

pub enum PrepareState {
    NeedData,
    NeedConsume,
    Sync,
    Async,

}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor {
    // const NAME: str;

    fn prepare(&self) -> PrepareState;

    // Synchronous work.
    fn process(&mut self) -> Result<()>;

    // Asynchronous work.
    async fn async_process(&mut self) -> Result<()>;

    fn connect_input(&mut self, input: InputPort) -> Result<()>;

    fn connect_output(&mut self, output: OutputPort) -> Result<()>;
}

pub type Processors = Vec<Box<dyn Processor>>;
