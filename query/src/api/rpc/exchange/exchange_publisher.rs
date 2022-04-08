use std::sync::Arc;
use common_base::tokio::sync::mpsc::Sender;
use common_datablocks::DataBlock;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::sessions::QueryContext;
use common_exception::Result;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;

pub struct ExchangePublisher {
    ctx: Arc<QueryContext>,
    local_pos: usize,
    publish_sender: Vec<Sender<DataBlock>>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
}

impl ExchangePublisher {
    pub fn try_create() -> Result<ProcessorPtr> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl Processor for ExchangePublisher {
    fn name(&self) -> &'static str {
        "ExchangePublisher"
    }

    fn event(&mut self) -> Result<Event> {
        todo!()
    }
}
