use std::task::Context;

use common_base::tokio::macros::support::Pin;
use common_base::tokio::macros::support::Poll;
use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Stream;

#[async_trait::async_trait]
pub trait Source: Send + Unpin + Sync {
    const NAME: &'static str;

    async fn ready(&mut self) -> Result<()>;

    // async fn generate(&mut self) -> Result<Option<DataBlock>>;
    fn generate(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<DataBlock>>>;
}

pub struct SourceStream<T: Source> {
    source: T,
}

impl<T: Source> SourceStream<T> {
    #[allow(dead_code)]
    pub fn create(source: T) -> SourceStream<T> {
        SourceStream { source }
    }
}

impl<T: Source> Stream for SourceStream<T> {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.source.generate(cx)
    }
}

// #[async_trait::async_trait]
// impl<T: Source> Processor for T {
//     fn name(&self) -> &str {
//         Self::NAME
//     }
//
//     fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
//         Err(ErrorCode::UnImplement("must be not implement."))
//     }
//
//     fn inputs(&self) -> Vec<Arc<dyn Processor>> {
//         vec![]
//     }
//
//     fn as_any(&self) -> &dyn Any {
//         todo!("remove this function")
//     }
//
//     async fn execute(&self) -> Result<SendableDataBlockStream> {
//
//         todo!("async -> sync and create source stream")
//     }
// }
