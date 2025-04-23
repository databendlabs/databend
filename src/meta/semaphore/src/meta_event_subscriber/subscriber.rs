// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::sync::Arc;

use databend_common_meta_client::ClientHandle;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use futures::FutureExt;
use futures::Stream;
use futures::TryStreamExt;
use log::error;
use log::info;
use tonic::Status;

use crate::errors::ConnectionClosed;
use crate::meta_event_subscriber::processor::Processor;

/// Watch semaphore events and update local queue, then send semaphore acquired/removed events to the `tx`.
pub(crate) struct MetaEventSubscriber {
    pub(crate) left: String,
    pub(crate) right: String,
    pub(crate) meta_client: Arc<ClientHandle>,

    pub(crate) processor: Processor,

    /// Contains descriptive information about the context of this watcher.
    pub(crate) ctx: String,
}

impl MetaEventSubscriber {
    /// Subscribe to the key-value changes in the interested range, feed them into local queue and generate [`SemaphoreEvent`]
    pub(crate) async fn subscribe_kv_changes(
        self,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) {
        let ctx = self.ctx.clone();
        let res = self.do_subscribe(cancel).await;
        if let Err(e) = res {
            error!("{} watcher error: {}", ctx, e);
        }
    }

    async fn do_subscribe(
        self,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ConnectionClosed> {
        let strm = self.new_watch_stream().await?;
        self.process_meta_event_loop(strm, cancel).await
    }

    /// Create a new watch stream to watch the key-value change event in the interested range.
    pub(crate) async fn new_watch_stream(
        &self,
    ) -> Result<tonic::Streaming<WatchResponse>, ConnectionClosed> {
        let watch =
            WatchRequest::new(self.left.clone(), Some(self.right.clone())).with_initial_flush(true);

        let strm = self.meta_client.request(watch).await.map_err(|x| {
            ConnectionClosed::new_str(x.to_string())
                .context("send watch request")
                .context(&self.ctx)
        })?;

        info!(
            "{} watch stream created: [{}, {})",
            self.ctx, self.left, self.right
        );

        Ok(strm)
    }

    /// The main loop of the semaphore engine.
    ///
    /// # Arguments
    ///
    /// * `cancel` - A future that, when ready, signals this loop to terminate.
    ///
    /// # Behavior
    ///
    /// This function watches for key-value changes in the metadata store and processes them through
    /// a local queue [`SemaphoreQueue`]. The queue generates semaphore state change events (like
    /// `Acquired` and `Removed`) which are then sent to the caller via the provided `tx` channel.
    ///
    /// The loop continues until either the watch stream closes(error quit) or the `cancel` future
    /// completes(normal quit).
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if terminated normally(i.e., the `cancel` future is ready), or
    /// `Err(ConnectionClosed)` if the metadata connection was closed unexpectedly.
    pub(crate) async fn process_meta_event_loop(
        mut self,
        mut strm: impl Stream<Item = Result<WatchResponse, Status>> + Send + Unpin + 'static,
        mut cancel: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ConnectionClosed> {
        //
        let mut c = std::pin::pin!(cancel);

        loop {
            let watch_result = futures::select! {
                _ = c.as_mut().fuse() => {
                    info!("Semaphore loop canceled by user");
                    return Ok(());
                }

                watch_result = strm.try_next().fuse() => {
                    watch_result
                }
            };

            info!(
                "{} received event from watch-stream: {:?}",
                self.ctx, watch_result
            );

            let Some(watch_response) = watch_result? else {
                // TODO: add retry connecting.
                error!("watch-stream closed: {}", self.ctx);
                return Err(ConnectionClosed::new_str("watch-stream closed").context(&self.ctx));
            };

            self.processor
                .process_watch_response(watch_response)
                .await?;
        }
    }
}
