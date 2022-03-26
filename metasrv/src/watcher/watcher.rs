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
use common_base::tokio::sync::mpsc::Sender;
use common_base::tokio::sync::oneshot;
use common_base::tokio::sync::oneshot::Receiver as OneshotReceiver;
use common_base::tokio::sync::oneshot::Sender as OneshotSender;
use common_base::tokio::task;
use common_meta_types::protobuf::FilterType;
use common_meta_types::protobuf::WatchCreateRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::MetaWatcherResult;
use tonic::Status;
use tonic::Streaming;

use super::WatcherId;
use super::WatcherStreamSender;

pub struct Watcher {
    id: WatcherId,

    task: task::JoinHandle<()>,

    key: Vec<u8>,

    key_end: Vec<u8>,

    filter: FilterType,
}

pub struct WatcherCore {
    stream: Streaming<WatchRequest>,
    tx: WatcherStreamSender,
    watch_tx: Arc<mpsc::UnboundedSender<WatchRequest>>,
}

impl Watcher {
    pub fn spawn(
        id: WatcherId,
        create: WatchCreateRequest,
        stream: Streaming<WatchRequest>,
        tx: WatcherStreamSender,
        watch_tx: Arc<mpsc::UnboundedSender<WatchRequest>>,
        resp: WatchResponse,
    ) -> Self {
        let core = WatcherCore {
            stream,
            tx,
            watch_tx,
        };

        let _ = core.tx.send(Ok(resp));

        let task = tokio::spawn(core.watcher_main());

        Watcher {
            id,
            task,
            key: create.clone().key,
            key_end: create.clone().key_end,
            filter: create.filter(),
        }
    }

    pub fn stop(&mut self, id: WatcherId) {
        if id != self.id {
            return;
        }

        self.task.abort()
    }
}

impl WatcherCore {
    async fn watcher_main(mut self) {
        while let Some(req) = self.stream.message().await.unwrap() {
            let _ = self.watch_tx.send(req);
        }
    }
}
