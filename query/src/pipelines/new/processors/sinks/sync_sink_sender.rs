use std::sync::Arc;
use common_base::tokio::sync::mpsc::Sender;
use common_datablocks::DataBlock;
use common_exception::Result;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::sinks::sync_sink::{Sink, SinkWrap};

pub struct SyncSenderSink {
    sender: Sender<Result<DataBlock>>,
}

impl SyncSenderSink {
    pub fn create(sender: Sender<Result<DataBlock>>, input: Arc<InputPort>) -> ProcessorPtr {
        SinkWrap::create(input, SyncSenderSink { sender })
    }
}

#[async_trait::async_trait]
impl Sink for SyncSenderSink {
    const NAME: &'static str = "SyncSenderSink";

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.sender.blocking_send(Ok(data_block)).unwrap();
        Ok(())
    }
}
