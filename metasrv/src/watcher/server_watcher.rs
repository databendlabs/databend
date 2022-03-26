//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::BTreeMap;
use std::sync::Arc;

use clap::Parser;
use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::mpsc::Sender;
use common_base::tokio::sync::oneshot;
use common_base::tokio::sync::oneshot::Receiver as OneshotReceiver;
use common_base::tokio::sync::oneshot::Sender as OneshotSender;
use common_meta_types::protobuf::watch_request::RequestUnion::CancelRequest as WatchCancelRequest;
use common_meta_types::protobuf::watch_request::RequestUnion::CreateRequest as WatchCreateRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::MetaWatcherResult;
use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;
use tonic::Status;
use tonic::Streaming;

use super::Watcher;

pub const META_WATCHER_NOTIFY_INTERVAL: &str = "META_WATCHER_NOTIFY_INTERVAL";
pub const META_WATCHER_BUFFER_SIZE: &str = "META_WATCHER_BUFFER_SIZE";

pub type WatcherId = i64;
pub type WatcherStreamSender = Sender<Result<WatchResponse, Status>>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Parser)]
pub struct WatcherConfig {
    /// Watcher notify interval ms.
    #[clap(long, env = META_WATCHER_NOTIFY_INTERVAL, default_value = "100")]
    pub notify_interval_ms: u32,

    /// Watcher buffer applied msg size.
    #[clap(long, env = META_WATCHER_BUFFER_SIZE, default_value = "1024000")]
    pub buffer_size: u32,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            notify_interval_ms: 100,
            buffer_size: 1024000,
        }
    }
}

type CreateWatcherEvent = (Streaming<WatchRequest>, WatcherStreamSender);

pub struct MetaServiceWatcher {
    /// A channel for sending create watch request.
    create_tx: mpsc::UnboundedSender<CreateWatcherEvent>,

    shutdown_tx: OneshotSender<()>,
}

struct ServiceWatcherCore {
    /// A channel for receiving create watcher request.
    create_rx: mpsc::UnboundedReceiver<CreateWatcherEvent>,

    /// A channel for sending watch request.
    watch_tx: Arc<mpsc::UnboundedSender<WatchRequest>>,

    /// A channel for receiving watch request.
    watch_rx: mpsc::UnboundedReceiver<WatchRequest>,

    shutdown_rx: OneshotReceiver<()>,

    watchers: BTreeMap<WatcherId, Watcher>,

    current_watcher_id: WatcherId,
}

impl MetaServiceWatcher {
    pub fn create() -> Self {
        let (create_tx, create_rx) = mpsc::unbounded_channel();
        let (watch_tx, watch_rx) = mpsc::unbounded_channel::<WatchRequest>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let core = ServiceWatcherCore {
            create_rx,
            watch_tx: Arc::new(watch_tx),
            watch_rx,
            shutdown_rx,
            watchers: BTreeMap::new(),
            current_watcher_id: 1,
        };

        let _h = Some(tokio::spawn(core.watcher_main()));

        MetaServiceWatcher {
            create_tx,
            shutdown_tx,
        }
    }

    pub async fn create_watcher(&self, stream: Streaming<WatchRequest>, tx: WatcherStreamSender) {
        //self.watcher.watch(watch).await
        self.create_tx.send((stream, tx));
    }
}

impl ServiceWatcherCore {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn watcher_main(mut self) -> MetaWatcherResult<()> {
        loop {
            tokio::select! {
                watch = self.create_rx.recv() => {
                    match watch {
                        Some((streaming, tx)) => {self.create_watcher(streaming, tx).await;},
                        None => {},
                    }
                }

            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_watcher(
        &mut self,
        mut stream: Streaming<WatchRequest>,
        tx: WatcherStreamSender,
    ) {
        tracing::info!("create_watcher 111");
        //let Some(req) = stream.message().await.unwrap;
        if let Some(req) = stream.message().await.unwrap() {
            match req.request_union {
                Some(WatchCreateRequest(create)) => {
                    // todo: get the current value of key
                    let resp = WatchResponse {
                        watch_id: self.current_watcher_id,
                        created: true,
                        canceled: false,
                        events: vec![],
                    };

                    let watcher = Watcher::spawn(
                        self.current_watcher_id,
                        create,
                        stream,
                        tx,
                        self.watch_tx.clone(),
                        resp,
                    );

                    self.watchers.insert(self.current_watcher_id, watcher);

                    self.current_watcher_id += 1;
                }
                _ => {}
            }
        };
    }

    async fn cancel(&self, cancel: common_meta_types::protobuf::WatchCancelRequest) {
        let _ = WatchResponse {
            watch_id: 1,
            created: true,
            canceled: false,
            events: vec![],
        };
    }
}
