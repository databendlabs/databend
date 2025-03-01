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

use std::sync::Arc;

use log::error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

use crate::dispatch::Command;
use crate::dispatch::Dispatcher;
use crate::type_config::KVChange;
use crate::type_config::TypeConfig;
use crate::watch_stream::WatchStreamSender;
use crate::EventFilter;
use crate::KeyRange;
use crate::WatchResult;

#[derive(Clone, Debug)]
pub struct DispatcherHandle<C>
where C: TypeConfig
{
    /// For sending event or command to the dispatcher.
    pub(crate) tx: mpsc::UnboundedSender<Command<C>>,
}

impl<C> DispatcherHandle<C>
where C: TypeConfig
{
    pub(crate) fn new(tx: mpsc::UnboundedSender<Command<C>>) -> Self {
        Self { tx }
    }

    pub fn send_command(&self, command: Command<C>) {
        if let Err(_e) = self.tx.send(command) {
            error!("Failed to send command to watch-Dispatcher");
        }
    }

    pub fn send_change(&self, change: KVChange<C>) {
        self.send_command(Command::Update(change));
    }

    pub async fn add_watcher(
        &self,
        key_range: KeyRange<C>,
        filter: EventFilter,
        tx: mpsc::Sender<WatchResult<C>>,
    ) -> Result<Arc<WatchStreamSender<C>>, &'static str> {
        self.request_blocking(move |dispatcher| dispatcher.add_watcher(key_range, filter, tx))
            .await
            .map_err(|_| "Failed to add watcher; watch-Dispatcher may be closed")
    }

    pub async fn remove_watcher(
        &self,
        watcher: Arc<WatchStreamSender<C>>,
    ) -> Result<(), &'static str> {
        self.request_blocking(move |dispatcher| dispatcher.remove_watcher(watcher))
            .await
            .map_err(|_| "Failed to remove watcher; watch-Dispatcher may be closed")
    }

    /// Send a request to the watch dispatcher.
    pub fn request(&self, req: impl FnOnce(&mut Dispatcher<C>) + Send + 'static) {
        let _ = self.tx.send(Command::Func { req: Box::new(req) });
    }

    /// Send a request to the watch dispatcher and block until finished
    pub async fn request_blocking<V>(
        &self,
        req: impl FnOnce(&mut Dispatcher<C>) -> V + Send + 'static,
    ) -> Result<V, RecvError>
    where
        V: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx.send(Command::Func {
            req: Box::new(|dispatcher| {
                let v = req(dispatcher);
                let _ = tx.send(v);
            }),
        });

        rx.await
    }
}
