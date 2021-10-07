use common_exception::Result;
use common_datablocks::DataBlock;
use std::task::Context;

#[async_trait::async_trait]
pub trait Sink: Send + Unpin + Sync {
    const NAME: &'static str;

    async fn ready(&mut self) -> Result<()>;

    fn consume(&mut self, data: Option<Result<DataBlock>>, cx: &mut Context<'_>);
}