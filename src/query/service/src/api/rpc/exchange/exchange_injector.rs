// use std::sync::Arc;
// use common_pipeline_core::Pipeline;
// use common_exception::Result;
// use common_expression::DataBlock;
// use crate::api::ExchangeSorting;
//
// enum SerializeRes {
//     Sync()
// }
//
// #[async_trait::async_trait]
// trait ExchangeSerializer {
//     fn serialize(&self, data_block: DataBlock) -> Result<SerializeRes>;
// }
//
// trait ExchangeInjector {
//     fn apply_scatter(&self, pipeline: &mut Pipeline) -> Result<()>;
//
//     fn get_exchange_sorting(&self) -> Result<Arc<dyn ExchangeSorting>>;
//
//     fn get_exchange_serializer(&self) -> Result<Arc<dyn ExchangeSerializer>>;
// }

use std::sync::Arc;

use common_exception::Result;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::DataExchange;
use crate::sessions::QueryContext;

pub trait ExchangeInjector: Send + Sync + 'static {
    fn flight_scatter(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>>;
}

pub struct DefaultExchangeInjector;

impl DefaultExchangeInjector {
    pub fn create() -> Arc<dyn ExchangeInjector> {
        Arc::new(DefaultExchangeInjector {})
    }
}

impl ExchangeInjector for DefaultExchangeInjector {
    fn flight_scatter(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        Ok(Arc::new(match exchange {
            DataExchange::Merge(_) => unreachable!(),
            DataExchange::Broadcast(exchange) => Box::new(BroadcastFlightScatter::try_create(
                exchange.destination_ids.len(),
            )?),
            DataExchange::ShuffleDataExchange(exchange) => HashFlightScatter::try_create(
                ctx.get_function_context()?,
                exchange.shuffle_keys.clone(),
                exchange.destination_ids.len(),
            )?,
        }))
    }
}
