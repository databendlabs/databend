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

use std::io;

use databend_common_meta_raft_store::state_machine_api::SMEventSender;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::Change;
use databend_common_meta_types::SeqV;
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tonic::Status;

use crate::watcher::command::Command;
use crate::watcher::EventSubscriber;

#[derive(Clone, Debug)]
pub struct SubscriberHandle {
    /// For sending event or command to the dispatcher.
    pub(crate) tx: mpsc::UnboundedSender<Command>,
}

impl SMEventSender for SubscriberHandle {
    fn send(&self, change: Change<Vec<u8>, String>) {
        let _ = self.tx.send(Command::KVChange(change));
    }

    fn send_batch(
        &self,
        tx: Sender<Result<WatchResponse, Status>>,
        strm: BoxStream<'static, Result<(String, SeqV), io::Error>>,
    ) {
        self.tx
            .send(Command::RequestAsync {
                req: Box::new(move |_d| EventSubscriber::send_stream(tx, strm)),
            })
            .ok();
    }
}

impl SubscriberHandle {
    pub(crate) fn new(tx: mpsc::UnboundedSender<Command>) -> Self {
        Self { tx }
    }

    /// Send a request to the watch dispatcher.
    pub fn request(&self, req: impl FnOnce(&mut EventSubscriber) + Send + 'static) {
        let _ = self.tx.send(Command::Request { req: Box::new(req) });
    }

    /// Send a request to the watch dispatcher and block until finished
    pub async fn request_blocking<V>(
        &self,
        req: impl FnOnce(&mut EventSubscriber) -> V + Send + 'static,
    ) -> Result<V, RecvError>
    where
        V: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx.send(Command::Request {
            req: Box::new(|dispatcher| {
                let v = req(dispatcher);
                let _ = tx.send(v);
            }),
        });

        rx.await
    }
}
