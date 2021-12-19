use common_datablocks::DataBlock;
use common_exception::Result;

#[async_trait::async_trait]
pub trait AsyncSource {
    async fn generate(&mut self) -> Result<Option<DataBlock>>;
}

// TODO: This can be refactored using proc macros
pub struct ASyncSourceProcessorWrap<T: AsyncSource> {
    inner: T,
}
