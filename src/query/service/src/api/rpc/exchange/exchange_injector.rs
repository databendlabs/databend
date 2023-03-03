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
