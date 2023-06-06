use common_expression::DataBlock;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use crate::index_analyzer::block_filter_meta::BlocksFilterMeta;

struct BlockFilterReadTransform {}

#[async_trait::async_trait]
impl AsyncTransform for BlockFilterReadTransform {
    const NAME: &'static str = "BlockFilterReadTransform";

    async fn transform(&mut self, mut data: DataBlock) -> common_exception::Result<DataBlock> {
        // TODO: read meta
        BlocksFilterMeta::
        todo!()
        // data
    }
}
