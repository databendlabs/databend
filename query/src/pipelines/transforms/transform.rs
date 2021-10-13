use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::Stream;

#[async_trait::async_trait]
pub trait StatelessTransform: Send + Unpin + Sync {
    const NAME: &'static str;

    async fn ready(&mut self) -> Result<()>;

    ///
    fn finalized(&self) -> Result<Option<DataBlock>>;

    fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>>;
}

pub struct StatelessTransformStream<T: StatelessTransform> {
    inner: T,
    is_finalized: bool,
    upstream: SendableDataBlockStream,
}

impl<T: StatelessTransform> StatelessTransformStream<T> {
    fn poll_finalized(&mut self) -> Option<Result<DataBlock>> {
        if !self.is_finalized {
            self.is_finalized = true;
            return match self.inner.finalized() {
                Ok(None) => None,
                Err(cause) => Some(Err(cause)),
                Ok(Some(data)) => Some(Ok(data)),
            };
        }

        None
    }
}

impl<T: StatelessTransform> Stream for StatelessTransformStream<T> {
    type Item = Result<DataBlock>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut_self = self.get_mut();
        loop {
            match mut_self.upstream.as_mut().poll_next(cx) {
                Poll::Pending => {
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    return Poll::Ready(mut_self.poll_finalized());
                }
                Poll::Ready(Some(Err(cause))) => {
                    return Poll::Ready(Some(Err(cause)));
                }
                Poll::Ready(Some(Ok(data))) => match mut_self.inner.transform(data) {
                    Ok(None) => {
                        continue;
                    }
                    Ok(Some(data)) => {
                        return Poll::Ready(Some(Ok(data)));
                    }
                    Err(cause) => {
                        return Poll::Ready(Some(Err(cause)));
                    }
                },
            }
        }
    }
}

// #[async_trait::async_trait]
// impl<T: Transform> Processor for T {
//     fn name(&self) -> &str {
//         Self::NAME
//     }
//
//     fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
//         todo!("remove this function")
//     }
//
//     fn inputs(&self) -> Vec<Arc<dyn Processor>> {
//         todo!("remove this function")
//     }
//
//     fn as_any(&self) -> &dyn Any {
//         todo!("remove this function")
//     }
//
//     async fn execute(&self) -> Result<SendableDataBlockStream> {
//         todo!("async -> sync and create transform stream")
//     }
// }
