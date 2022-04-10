use std::sync::Arc;
use common_base::tokio::sync::mpsc::Sender;
use common_datablocks::DataBlock;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use common_planners::Expression;
use crate::api::{DataExchange, HashDataExchange, MergeExchange};
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;

pub struct ExchangePublisher {
    ctx: Arc<QueryContext>,
    local_pos: usize,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    publish_sender: Vec<Sender<DataBlock>>,
}

impl ExchangePublisher {
    fn via_merge_exchange(ctx: &Arc<QueryContext>, exchange: &MergeExchange) -> Result<()> {
        match exchange.destination_id == ctx.get_cluster().local_id() {
            true => Ok(()), /* do nothing */
            false => Err(ErrorCode::LogicalError("Locally depends on merge exchange, but the localhost is not a coordination node.")),
        }
    }

    pub fn via_exchange(
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        match exchange {
            DataExchange::Merge(exchange) => Self::via_merge_exchange(ctx, exchange),
            DataExchange::HashDataExchange(exchange) => pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    ExchangePublisher::try_create(
                        transform_input_port,
                        transform_output_port,
                        ctx.clone(),
                        exchange.clone(),
                    )
                }
            )
        }
    }

    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        exchange: HashDataExchange,
    ) -> Result<ProcessorPtr> {
        // exchange.de
        Ok(ProcessorPtr::create(Box::new(ExchangePublisher {
            ctx,
            input,
            output,
            local_pos: 0,
            publish_sender: vec![],
        })))
    }
}

struct HashExchangePublisher {
    ctx: Arc<QueryContext>,

    query_id: String,
    fragment_id: String,
    destination_ids: Vec<String>,
    exchange_expression: Expression,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<Vec<DataBlock>>,
    peer_endpoint_publisher: Vec<Sender<DataBlock>>,
}

impl HashExchangePublisher {
    pub fn try_create() -> Result<ProcessorPtr> {
        unimplemented!()
    }

    fn get_peer_endpoint_publisher(&self) -> Result<Vec<Sender<DataBlock>>> {
        let mut res = Vec::with_capacity(self.destination_ids.len());
        let exchange_manager = self.ctx.get_exchange_manager();

        for destination_id in &self.destination_ids {
            res.push(exchange_manager.get_fragment_sink(&self.query_id, destination_id)?);
        }

        Ok(res)
    }
}

#[async_trait::async_trait]
impl Processor for HashExchangePublisher {
    fn name(&self) -> &'static str {
        "HashExchangePublisher"
    }

    fn event(&mut self) -> Result<Event> {
        if self.peer_endpoint_publisher.is_empty() {
            self.peer_endpoint_publisher = self.get_peer_endpoint_publisher()?;
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        // This may cause other cluster nodes to idle.
        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        // for x in self.peer_endpoint_publisher {
        //     x.try_send()
        // }
        // TODO:
        // if let Some(data) = self.output_data
        unimplemented!()
        // if self.publish_sender.is_empty() {
        //     // TODO: init
        // }
        // todo!()
    }

    fn process(&mut self) -> Result<()> {
        todo!()
    }

    async fn async_process(&mut self) -> Result<()> {
        todo!()
    }
}
