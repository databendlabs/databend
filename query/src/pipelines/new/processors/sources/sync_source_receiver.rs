use std::sync::Arc;

use common_base::tokio::sync::mpsc::Receiver;
// use std::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_exception::Result;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::sources::sync_source::SyncSource;
use crate::pipelines::new::processors::sources::SyncSourcer;

pub struct SyncReceiverSource {
    receiver: Receiver<Result<DataBlock>>,
}

impl SyncReceiverSource {
    pub fn create(rx: Receiver<Result<DataBlock>>, out: Arc<OutputPort>) -> Result<ProcessorPtr> {
        SyncSourcer::create(vec![out], SyncReceiverSource { receiver: rx })
    }
}

#[async_trait::async_trait]
impl SyncSource for SyncReceiverSource {
    const NAME: &'static str = "SyncReceiverSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.receiver.blocking_recv() {
            None => Ok(None),
            Some(Err(cause)) => Err(cause),
            Some(Ok(data_block)) => Ok(Some(data_block)),
        }
    }
}
