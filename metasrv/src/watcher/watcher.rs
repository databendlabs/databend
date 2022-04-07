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

use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_meta_types::protobuf::WatchCreateRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use tonic::Streaming;

use super::WatcherId;
use super::WatcherStreamId;
use super::WatcherStreamSender;

#[derive(Debug)]
pub struct Watcher {
    id: WatcherId,

    // owner watcher stream id
    owner: WatcherStreamId,

    pub key: String,

    pub key_end: String,
}

#[derive(Debug)]
pub struct WatcherStream {
    id: WatcherStreamId,

    //task: task::JoinHandle<impl Future<Output = ()>>,
    tx: WatcherStreamSender,
}

pub struct WatcherStreamCore {
    id: WatcherStreamId,

    stream: Streaming<WatchRequest>,

    watch_tx: Arc<mpsc::UnboundedSender<(WatcherStreamId, WatchRequest)>>,
}

impl Watcher {
    pub fn new(id: WatcherId, owner: WatcherStreamId, create: WatchCreateRequest) -> Watcher {
        Watcher {
            id,
            owner,
            key: create.key,
            key_end: create.key_end,
        }
    }
}

impl WatcherStream {
    pub fn spawn(
        id: WatcherStreamId,
        stream: Streaming<WatchRequest>,
        tx: WatcherStreamSender,
        watch_tx: Arc<mpsc::UnboundedSender<(WatcherStreamId, WatchRequest)>>,
    ) -> Self {
        let core = WatcherStreamCore {
            id,
            stream,
            watch_tx,
        };

        let _task = tokio::spawn(core.watcher_main());

        WatcherStream { id, tx }
    }

    pub fn stop(&mut self, id: WatcherId) {
        if id != self.id {
            return;
        }

        //self.task.abort()
    }

    pub fn send(&self, resp: WatchResponse) {
        let _ = self.tx.send(Ok(resp));
    }
}

impl WatcherStreamCore {
    async fn watcher_main(mut self) {
        while let Some(req) = self.stream.message().await.unwrap() {
            let _ = self.watch_tx.send((self.id, req));
        }
    }
}
