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

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::Change;
use databend_common_meta_types::SeqV;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::info;
use log::warn;
use prost::Message;
use span_map::SpanMap;
use tokio::sync::mpsc;
use tonic::Status;

use crate::metrics::network_metrics;
use crate::metrics::server_metrics;
use crate::watcher::dispatch::command::Command;
use crate::watcher::dispatch::dispatcher_handle::DispatcherHandle;
use crate::watcher::dispatch::Update;
use crate::watcher::watch_stream::WatchStreamSender;
use crate::watcher::WatchDesc;
use crate::watcher::WatcherId;

/// Receives events from event sources via `rx` and dispatches them to interested watchers.
///
/// The [`Dispatcher`] acts as a central hub for the watch system, managing
/// subscriptions and ensuring that each watcher receives only the events
/// they have registered interest in. It maintains a mapping of watchers
/// and their watch descriptors to efficiently route events.
pub struct Dispatcher {
    rx: mpsc::UnboundedReceiver<Command>,

    watchers: SpanMap<String, Arc<WatchStreamSender>>,

    current_watcher_id: WatcherId,
}

impl Dispatcher {
    /// Spawn a dispatcher loop task.
    ///
    /// Creates a new [`Dispatcher`] instance and spawns it as an asynchronous task.
    /// The dispatcher will process incoming commands and route watch events to the
    /// appropriate subscribers.
    ///
    /// Returns a handle that can be used to send commands to the dispatcher.
    pub(crate) fn spawn() -> DispatcherHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        let dispatcher = Dispatcher {
            rx,
            watchers: SpanMap::new(),
            current_watcher_id: 1,
        };

        let _h = databend_common_base::runtime::spawn(dispatcher.main());

        DispatcherHandle::new(tx)
    }

    #[fastrace::trace]
    async fn main(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Command::Update(kv_change) => {
                    self.dispatch(kv_change).await;
                }
                Command::Request { req } => req(&mut self),
                Command::RequestAsync { req } => req(&mut self).await,
            }
        }

        info!("watch-event-Dispatcher: all event senders are closed. quit.");
    }

    /// Creates a future that streams key-value changes to a watcher.
    ///
    /// This method constructs a future that will process each item in the stream and
    /// forward it to the watcher via `tx` as a [`WatchResponse`]. The future terminates
    /// if the stream ends or if sending to the watcher fails.
    ///
    /// # Parameters
    /// * `to_tx` - The sender channel connected to a watcher client
    /// * `strm` - A stream of key-value pairs with sequence versions
    pub fn send_stream(
        to_tx: mpsc::Sender<Result<WatchResponse, Status>>,
        mut strm: BoxStream<'static, Result<(String, SeqV), io::Error>>,
    ) -> BoxFuture<'static, ()> {
        let fu = async move {
            while let Some(res) = strm.next().await {
                let (key, seq_v) = match res {
                    Ok((key, seq)) => (key, seq),
                    Err(err) => {
                        warn!("watch-event-Dispatcher: recv error from kv stream: {}", err);
                        to_tx
                            .send(Err(Status::internal(err.to_string())))
                            .await
                            .ok();
                        break;
                    }
                };

                let resp =
                    WatchResponse::new(&Change::new(None, Some(seq_v)).with_id(key)).unwrap();
                let resp_size = resp.encoded_len() as u64;

                if let Err(_err) = to_tx.send(Ok(resp)).await {
                    warn!("watch-event-Dispatcher: fail to send to watcher; close this stream");
                    break;
                } else {
                    network_metrics::incr_sent_bytes(resp_size);
                }
            }
        };
        Box::pin(fu)
    }

    /// Dispatch a kv change event to interested watchers.
    async fn dispatch(&mut self, update: Update) {
        let is_delete = update.after.is_none();

        let resp = WatchResponse::new3(
            update.key.clone(),
            update.before.clone(),
            update.after.clone(),
        );
        let resp_size = resp.encoded_len() as u64;

        let mut removed = vec![];

        for sender in self.watchers.get(&update.key) {
            let interested = sender.desc.interested;

            match interested {
                FilterType::All => {}
                FilterType::Update => {
                    if is_delete {
                        continue;
                    }
                }
                FilterType::Delete => {
                    if !is_delete {
                        continue;
                    }
                }
            }

            if let Err(_err) = sender.send(resp.clone()).await {
                warn!(
                    "watch-event-Dispatcher: fail to send to watcher {}; close this stream",
                    sender.desc.watcher_id
                );
                removed.push(sender.clone());
            } else {
                network_metrics::incr_sent_bytes(resp_size);
            };
        }

        for sender in removed {
            self.remove_watcher(sender);
        }
    }

    #[fastrace::trace]
    pub fn add_watcher(
        &mut self,
        req: WatchRequest,
        tx: mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> Result<Arc<WatchStreamSender>, &'static str> {
        info!("watch-event-Dispatcher::add_watcher: {:?}", req);

        let interested = req.filter_type();
        let desc = self.new_watch_desc(req.key, req.key_end, interested)?;

        let stream_sender = Arc::new(WatchStreamSender::new(desc, tx));

        self.watchers
            .insert(stream_sender.desc.key_range.clone(), stream_sender.clone());

        server_metrics::incr_watchers(1);

        Ok(stream_sender)
    }

    fn new_watch_desc(
        &mut self,
        key: String,
        key_end: Option<String>,
        interested: FilterType,
    ) -> Result<WatchDesc, &'static str> {
        self.current_watcher_id += 1;
        let watcher_id = self.current_watcher_id;

        let range = WatchRequest::build_key_range(&key, &key_end)?;

        let desc = WatchDesc::new(watcher_id, interested, range);
        Ok(desc)
    }

    #[fastrace::trace]
    pub fn remove_watcher(&mut self, stream_sender: Arc<WatchStreamSender>) {
        info!(
            "watch-event-Dispatcher::remove_watcher: {:?}",
            stream_sender
        );

        self.watchers.remove(.., stream_sender);

        server_metrics::incr_watchers(-1);
    }

    pub fn watch_senders(&self) -> BTreeSet<&Arc<WatchStreamSender>> {
        self.watchers.values(..)
    }
}
