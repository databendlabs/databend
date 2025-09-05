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

use core::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_client::ClientHandle;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use display_more::DisplayOptionExt;
use futures::FutureExt;
use futures::Stream;
use futures::TryStreamExt;
use log::error;
use log::info;
use log::warn;
use tonic::Status;

use crate::errors::ConnectionClosed;
use crate::errors::ProcessorError;
use crate::meta_event_subscriber::processor::Processor;

/// Watch semaphore events and update local queue, then send semaphore acquired/removed events to the `tx`.
pub(crate) struct MetaEventSubscriber {
    pub(crate) left: String,
    pub(crate) right: String,
    pub(crate) meta_client: Arc<ClientHandle>,

    /// The duration after which the permit entry will be removed from meta-service.
    pub(crate) permit_ttl: Duration,

    pub(crate) processor: Processor,

    /// Contains descriptive information about the context of this watcher.
    pub(crate) watcher_name: String,
}

impl MetaEventSubscriber {
    /// Subscribe to the key-value changes in the interested range, feed them into local queue and generate [`SemaphoreEvent`]
    pub(crate) async fn subscribe_kv_changes(
        self,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) {
        let watcher_name = self.watcher_name.clone();
        let res = self.do_subscribe(cancel).await;
        if let Err(e) = res {
            error!("{} watcher error: {}", watcher_name, e);
        }
    }

    async fn do_subscribe(
        mut self,
        cancel: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ConnectionClosed> {
        // Retry connect if there is a connection error to the remote meta-service
        let mut ith = 0u64;
        let mut sleep_time = Duration::from_millis(20);
        let max_sleep_time = Duration::from_secs(10);

        let mut c = std::pin::pin!(cancel);

        loop {
            ith += 1;
            let strm = self.new_watch_stream(format!("retry {ith}")).await?;

            let res = self.process_meta_event_loop(strm, c.as_mut()).await;
            match res {
                Ok(()) => return Ok(()),
                Err(ProcessorError::AcquirerClosed(e)) => {
                    info!("{}: {}", self.watcher_name, e);
                    return Ok(());
                }
                Err(ProcessorError::ConnectionClosed(e)) => {
                    sleep_time = sleep_time * 3 / 2;
                    sleep_time = sleep_time.min(max_sleep_time);

                    warn!(
                        "{}: {}; About to retry {} times connecting to remote meta-service after {:?}",
                        self.watcher_name, e, ith+1, sleep_time
                    );
                    tokio::time::sleep(sleep_time).await;
                }
            }
        }
    }

    /// Create a new watch stream to watch the key-value change event in the interested range.
    pub(crate) async fn new_watch_stream(
        &self,
        ctx: impl fmt::Display,
    ) -> Result<tonic::Streaming<WatchResponse>, ConnectionClosed> {
        let watch =
            WatchRequest::new(self.left.clone(), Some(self.right.clone())).with_initial_flush(true);

        let strm = self.meta_client.request(watch).await.map_err(|x| {
            ConnectionClosed::new_str(x.to_string())
                .context("send watch request")
                .context(&self.watcher_name)
        })?;

        info!(
            "{} {} watch stream created: [{}, {})",
            self.watcher_name, ctx, self.left, self.right
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
        &mut self,
        mut strm: impl Stream<Item = Result<WatchResponse, Status>> + Send + Unpin + 'static,
        mut cancel: impl Future<Output = ()> + Send,
    ) -> Result<(), ProcessorError> {
        //
        let mut c = std::pin::pin!(cancel);
        let timeout_duration = self.permit_ttl * 3 / 2;

        loop {
            let timeout_fu = tokio::time::sleep(timeout_duration);

            let watch_result = futures::select! {
                _ = c.as_mut().fuse() => {
                    info!("{}: process_meta_event_loop canceled by user", self.watcher_name);
                    return Ok(());
                }

                _ = timeout_fu.fuse() => {
                    warn!("{}: process_meta_event_loop timeout waiting for an event", self.watcher_name);
                    return Err(ProcessorError::ConnectionClosed(
                        ConnectionClosed::new_str("timeout").context(&self.watcher_name)
                    ));
                }

                watch_result = strm.try_next().fuse() => {
                    watch_result
                }
            };

            match &watch_result {
                Ok(t) => {
                    log::debug!(
                        "{} received event from watch-stream: Ok({})",
                        self.watcher_name,
                        t.display()
                    );
                }
                Err(e) => {
                    info!(
                        "{} received event from watch-stream: Err({})",
                        self.watcher_name, e
                    );
                }
            }

            let Some(watch_response) = watch_result? else {
                warn!("watch-stream closed: {}", self.watcher_name);

                return Err(ProcessorError::ConnectionClosed(
                    ConnectionClosed::new_str("watch-stream closed").context(&self.watcher_name),
                ));
            };

            self.processor
                .process_watch_response(watch_response)
                .await?;
        }
    }
}
