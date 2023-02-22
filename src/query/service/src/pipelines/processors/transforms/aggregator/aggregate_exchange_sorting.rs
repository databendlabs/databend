use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;

use crate::api::ExchangeSorting;
use crate::pipelines::processors::transforms::aggregator::AggregateInfo;

pub struct AggregateExchangeSorting {}

impl AggregateExchangeSorting {
    pub fn create() -> Arc<dyn ExchangeSorting> {
        Arc::new(AggregateExchangeSorting {})
    }
}

impl ExchangeSorting for AggregateExchangeSorting {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        match data_block.get_meta() {
            None => Ok(-1),
            Some(block_meta_info) => match block_meta_info.as_any().downcast_ref::<AggregateInfo>() {
                None => Err(ErrorCode::Internal(
                    "Internal error, AggregateExchangeSorting only recv AggregateInfo",
                )),
                Some(meta_info) => Ok(meta_info.bucket),
            },
        }
    }
}
