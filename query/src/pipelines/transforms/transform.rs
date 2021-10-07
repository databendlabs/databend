use common_exception::{Result, ErrorCode};
use common_datablocks::DataBlock;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use common_streams::SendableDataBlockStream;
use crate::pipelines::processors::Processor;
use std::sync::Arc;
use std::any::Any;

pub enum TransformStatus {
    PollUpstream,
    PushDownstream(DataBlock),
    End,
}

#[async_trait::async_trait]
pub trait StatefulTransform: Send + Unpin + Sync {
    const NAME: &'static str;

    async fn ready(&mut self) -> Result<()>;

    fn transform(&mut self, data: Option<DataBlock>) -> Result<TransformStatus>;
}

#[async_trait::async_trait]
pub trait StatelessTransform: Send + Unpin + Sync {
    const NAME: &'static str;

    async fn ready(&mut self) -> Result<()>;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;
}

pub struct StatefulTransformStream<T: StatefulTransform> {
    transform: T,
    end_for_upstream: bool,
    upstream: SendableDataBlockStream,
}

impl<T: StatefulTransform> StatefulTransformStream<T> {
    pub fn create(mut transform: T, upstream: SendableDataBlockStream) -> StatefulTransformStream<T> {
        StatefulTransformStream { transform, end_for_upstream: false, upstream }
    }

    fn transform_data(&mut self, data: Option<Result<DataBlock>>) -> Result<TransformStatus> {
        match data {
            None => {
                self.end_for_upstream = true;
                self.transform.transform(None)
            }
            Some(Err(cause)) => Err(cause),
            Some(Ok(ready_data)) => self.transform.transform(Some(ready_data))
        }
    }
}

impl<T: StatefulTransform> Stream for StatefulTransformStream<T> {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // fast path for end stream.
            if self.end_for_upstream {
                match self.transform.transform(None) {
                    Err(cause) => { return Poll::Ready(Some(Err(cause))); }
                    Ok(TransformStatus::End) => { return Poll::Ready(None); }
                    Ok(TransformStatus::PushDownstream(data)) => { return Poll::Ready(Some(Ok(data))); }
                    Ok(TransformStatus::PollUpstream) => unreachable!("")
                }
            }

            match self.upstream.poll_next_unpin(cx) {
                Poll::Pending => { return Poll::Pending; }
                Poll::Ready(ready_data) => {
                    match self.transform_data(ready_data) {
                        Err(cause) => { return Poll::Ready(Some(Err(cause))); }
                        Ok(TransformStatus::End) => { return Poll::Ready(None); }
                        Ok(TransformStatus::PollUpstream) => { continue; }
                        Ok(TransformStatus::PushDownstream(data)) => { return Poll::Ready(Some(Ok(data))); }
                    };
                }
            }
        }
    }
}

pub struct StatelessTransformStream<T: StatelessTransform> {
    transform: T,
    upstream: SendableDataBlockStream,
}

impl<T: StatelessTransform> Stream for StatelessTransformStream<T> {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.upstream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(data))) => Poll::Ready(Some(self.transform.transform(data))),
            other => other
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