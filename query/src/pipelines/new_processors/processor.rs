use common_exception::Result;
use crate::pipelines::new_processors::port::{InputPorts, OutputPorts};
use futures::executor::block_on;
use common_base::Runtime;

enum PrepareState {
    NeedData,
    NeedConsume,
    Sync,
    Async,

}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor {
    const NAME: str;

    fn prepare(&self) -> PrepareState;

    // Synchronous work.
    fn process(&mut self) -> Result<()>;

    // Asynchronous work.
    async fn async_process(&mut self) -> Result<()>;

    fn attach_inputs(&mut self, inputs: InputPorts) -> Result<()>;

    fn attach_outputs(&mut self, outputs: OutputPorts) -> Result<()>;
}
