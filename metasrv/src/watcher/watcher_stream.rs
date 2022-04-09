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

use std::collections::BTreeSet;
use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_tracing::tracing;
use tonic::Streaming;

use super::CloseWatcherStreamReq;
use super::WatcherId;
use super::WatcherStreamId;
use super::WatcherStreamSender;

#[derive(Debug)]
pub struct WatcherStream {
    id: WatcherStreamId,

    tx: WatcherStreamSender,

    /// notify manager to stop watcher stream
    close_stream_tx: Arc<mpsc::UnboundedSender<CloseWatcherStreamReq>>,

    shutdown_tx: mpsc::UnboundedSender<()>,

    /// save stream watcher ids
    pub watchers: BTreeSet<WatcherId>,
}

pub struct WatcherStreamCore {
    id: WatcherStreamId,

    stream: Streaming<WatchRequest>,

    watch_tx: Arc<mpsc::UnboundedSender<(WatcherStreamId, WatchRequest)>>,

    /// notify manager to stop watcher stream
    close_stream_tx: Arc<mpsc::UnboundedSender<CloseWatcherStreamReq>>,

    /// notify the core shutdown
    shutdown_rx: mpsc::UnboundedReceiver<()>,
}

impl WatcherStream {
    pub fn spawn(
        id: WatcherStreamId,
        stream: Streaming<WatchRequest>,
        tx: WatcherStreamSender,
        watch_tx: Arc<mpsc::UnboundedSender<(WatcherStreamId, WatchRequest)>>,
        close_stream_tx: Arc<mpsc::UnboundedSender<CloseWatcherStreamReq>>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();
        let core = WatcherStreamCore {
            id,
            stream,
            watch_tx,
            shutdown_rx,
            close_stream_tx: close_stream_tx.clone(),
        };

        let _ = tokio::spawn(core.watcher_main());

        WatcherStream {
            id,
            tx,
            close_stream_tx,
            shutdown_tx,
            watchers: BTreeSet::new(),
        }
    }

    pub fn add_watcher(&mut self, id: WatcherId) {
        self.watchers.insert(id);
    }

    pub fn get_watchers(&self) -> &BTreeSet<WatcherId> {
        return &self.watchers;
    }

    pub async fn send(&self, resp: WatchResponse) {
        let ret = self.tx.send(Ok(resp)).await;
        match ret {
            Err(err) => {
                tracing::info!(
                    "close watcher stream {:?} cause send err: {:?}",
                    self.id,
                    err
                );
                let _ = self.close_stream_tx.send((self.id, err.to_string()));
                let _ = self.shutdown_tx.send(());
            }
            Ok(_) => {}
        }
    }
}

impl WatcherStreamCore {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn watcher_main(mut self) {
        loop {
            tokio::select! {
                    msg = self.stream.message() => {
                    match msg {
                        Ok(msg) => {
                            if let Some(req) = msg {
                                let _ = self.watch_tx.send((self.id, req));
                            }
                        }
                        Err(err) => {
                            tracing::info!(
                                "close watcher stream {:?} cause recv err: {:?}",
                                self.id,
                                err
                            );
                            let _ = self.close_stream_tx.send((self.id, err.to_string()));
                            break;
                        }
                    }
                },
                _ = self.shutdown_rx.recv() => {
                    tracing::info!(
                        "close watcher stream {:?} has been shutdown",
                        self.id,
                    );
                    break;
                }
            }
        }
    }
}
