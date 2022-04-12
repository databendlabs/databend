use std::sync::Arc;
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_exception::Result;
use crate::api::DataExchange;
use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::Event;

pub struct ExchangeSubscriber {}

impl ExchangeSubscriber {
    pub fn via_exchange(
        rx: Receiver<Option<FlightData>>,
        exchange_params: &ExchangeParams,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        // TODO:
        unimplemented!()
    }

    pub fn create_source(rx: Receiver<Option<FlightData>>, pipeline: &mut NewPipeline) -> Result<()> {
        // TODO:
        unimplemented!()
    }
}

struct ViaExchangeSubscriber {
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,
}

#[async_trait::async_trait]
impl Processor for ViaExchangeSubscriber {
    fn name(&self) -> &'static str {
        "ViaExchangeSubscriber"
    }

    fn event(&mut self) -> Result<Event> {
        // TODO: receive data
    }
}
