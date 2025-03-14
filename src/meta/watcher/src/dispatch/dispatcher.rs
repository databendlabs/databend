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
use std::sync::Arc;

use log::info;
use log::warn;
use span_map::SpanMap;
use tokio::sync::mpsc;

use crate::dispatch::command::Command;
use crate::dispatch::dispatcher_handle::DispatcherHandle;
use crate::event_filter::EventFilter;
use crate::type_config::KVChange;
use crate::type_config::TypeConfig;
use crate::watch_stream::WatchStreamSender;
use crate::KeyRange;
use crate::WatchDesc;
use crate::WatchResult;
use crate::WatcherId;

/// Receives events from event sources via `rx` and dispatches them to interested watchers.
///
/// The [`Dispatcher`] acts as a central hub for the watch system, managing
/// subscriptions and ensuring that each watcher receives only the events
/// they have registered interest in. It maintains a mapping of watchers
/// and their watch descriptors to efficiently route events.
pub struct Dispatcher<C>
where C: TypeConfig
{
    rx: mpsc::UnboundedReceiver<Command<C>>,

    watchers: SpanMap<C::Key, Arc<WatchStreamSender<C>>>,

    current_watcher_id: WatcherId,
}

impl<C> Dispatcher<C>
where C: TypeConfig
{
    /// Spawn a dispatcher loop task.
    ///
    /// Creates a new [`Dispatcher`] instance and spawns it as an asynchronous task.
    /// The dispatcher will process incoming commands and route watch events to the
    /// appropriate subscribers.
    ///
    /// Returns a handle that can be used to send commands to the dispatcher.
    pub fn spawn() -> DispatcherHandle<C> {
        let (tx, rx) = mpsc::unbounded_channel();

        let dispatcher = Dispatcher {
            rx,
            watchers: SpanMap::new(),
            current_watcher_id: 1,
        };

        C::spawn(dispatcher.main());

        DispatcherHandle::new(tx)
    }

    #[fastrace::trace]
    async fn main(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Command::Update(kv_change) => {
                    self.dispatch(kv_change).await;
                }
                Command::Func { req } => req(&mut self),
                Command::AsyncFunc { req } => req(&mut self).await,
                Command::Future(fu) => fu.await,
            }
        }

        info!("watch-event-Dispatcher: all event senders are closed. quit.");
    }

    /// Dispatch a kv change event to interested watchers.
    async fn dispatch(&mut self, kv_change: KVChange<C>) {
        let is_delete = kv_change.2.is_none();
        let event_type = if is_delete {
            EventFilter::DELETE
        } else {
            EventFilter::UPDATE
        };

        let mut removed = vec![];

        for sender in self.watchers.get(&kv_change.0) {
            let interested = sender.desc.interested;

            if !interested.accepts_event_type(event_type) {
                continue;
            }

            let resp = C::new_response(kv_change.clone());
            if let Err(_err) = sender.send(resp).await {
                warn!(
                    "watch-event-Dispatcher: fail to send to watcher {}; close this stream",
                    sender.desc.watcher_id
                );
                removed.push(sender.clone());
            };
        }

        for sender in removed {
            self.remove_watcher(sender);
        }
    }

    #[fastrace::trace]
    pub fn add_watcher(
        &mut self,
        rng: KeyRange<C>,
        filter: EventFilter,
        tx: mpsc::Sender<WatchResult<C>>,
    ) -> Arc<WatchStreamSender<C>> {
        info!(
            "watch-event-Dispatcher::add_watcher: range: {:?}, filter: {}",
            rng, filter
        );

        let desc = self.new_watch_desc(rng, filter);

        let stream_sender = Arc::new(WatchStreamSender::new(desc, tx));

        self.watchers
            .insert(stream_sender.desc.key_range.clone(), stream_sender.clone());

        C::update_watcher_metrics(1);

        stream_sender
    }

    fn new_watch_desc(&mut self, key_range: KeyRange<C>, interested: EventFilter) -> WatchDesc<C> {
        self.current_watcher_id += 1;
        let watcher_id = self.current_watcher_id;

        WatchDesc::new(watcher_id, interested, key_range)
    }

    #[fastrace::trace]
    pub fn remove_watcher(&mut self, stream_sender: Arc<WatchStreamSender<C>>) {
        info!(
            "watch-event-Dispatcher::remove_watcher: {:?}",
            stream_sender
        );

        self.watchers.remove(.., stream_sender);

        C::update_watcher_metrics(-1);
    }

    pub fn watch_senders(&self) -> BTreeSet<&Arc<WatchStreamSender<C>>> {
        self.watchers.values(..)
    }
}
