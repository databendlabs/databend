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

use common_base::tokio::sync::mpsc;
use common_meta_types::protobuf::WatchResponse;
use common_tracing::tracing;

use super::CloseWatcherStream;
use super::WatcherEvent;
use super::WatcherId;
use super::WatcherStreamSender;

#[derive(Debug)]
pub struct WatcherStream {
    id: WatcherId,

    tx: WatcherStreamSender,

    pub key: String,

    pub key_end: String,

    /// notify manager to stop watcher stream
    close_stream_tx: mpsc::UnboundedSender<WatcherEvent>,
}

impl WatcherStream {
    pub fn new(
        id: WatcherId,
        tx: WatcherStreamSender,
        close_stream_tx: mpsc::UnboundedSender<WatcherEvent>,
        key: String,
        key_end: String,
    ) -> Self {
        WatcherStream {
            id,
            tx,
            key,
            key_end,
            close_stream_tx,
        }
    }

    pub async fn send(&self, resp: WatchResponse) {
        let ret = self.tx.send(Ok(resp)).await;
        if let Err(err) = ret {
            tracing::info!(
                "close watcher stream {:?} cause send err: {:?}",
                self.id,
                err
            );
            let close_req: CloseWatcherStream = (self.id, err.to_string());
            let _ = self
                .close_stream_tx
                .send(WatcherEvent::CloseWatcherStreamEvent(close_req));
        }
    }
}
