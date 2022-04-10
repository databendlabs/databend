use common_base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_exception::Result;
use crate::api::DataExchange;
use crate::pipelines::new::NewPipeline;

pub struct ExchangeSubscriber {}

impl ExchangeSubscriber {
    pub fn via_exchange(
        rx: Receiver<Option<DataBlock>>,
        exchange: &DataExchange,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        // TODO:
    }
}
