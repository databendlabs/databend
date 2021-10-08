// use common_exception::Result;
// use common_datablocks::DataBlock;
// use std::task::Context;
// use common_streams::SendableDataBlockStream;
// use futures::Stream;
// use common_base::tokio::macros::support::{Pin, Poll};
//
// #[async_trait::async_trait]
// pub trait Sink: Send + Unpin + Sync {
//     const NAME: &'static str;
//
//     async fn ready(&mut self) -> Result<()>;
//
//     fn finalized(&self, msg: Result<()>, cx: &mut Context<'_>) -> Poll<()>;
//
//     fn consume(&mut self, data: DataBlock, cx: &mut Context<'_>) -> Poll<()>;
// }
//
// struct SinkStream<T: Sink> {
//     inner: T,
//     is_finalized: bool,
//     upstream: SendableDataBlockStream,
// }
//
// impl<T: Sink> SinkStream<T> {
//     fn finalized(&mut self, msg: Result<()>, cx: &mut Context<'_>) -> Poll<()> {
//
//     }
// }
//
// impl<T: Sink> Stream for SinkStream<T> {
//     type Item = ();
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let mut_self = self.get_mut();
//         loop {
//             match mut_self.upstream.as_mut().poll_next(cx) {
//                 Poll::Pending => { return Poll::Pending; }
//                 Poll::Ready(None) => {
//                     return match mut_self.finalized(Ok(()), cx) {
//                         Poll::Pending => { Poll::Pending }
//                         Poll::Ready(()) => { Poll::Ready(None) }
//                     };
//                 },
//                 Poll::Ready(Some(Err(cause))) => {
//                     return match mut_self.finalized(Err(cause), cx) {
//                         Poll::Pending => { Poll::Pending }
//                         Poll::Ready(()) => { Poll::Ready(None) }
//                     };
//                 }
//                 Poll::Ready(Some(Ok(data))) => {
//                     if let Poll::Pending = mut_self.inner.consume(data, cx) {
//                         return Poll::Pending;
//                     }
//                 },
//             }
//         }
//     }
// }
