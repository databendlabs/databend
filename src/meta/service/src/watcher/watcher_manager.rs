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

use core::ops::Range;

use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::tokio::sync::oneshot;
use databend_common_base::rangemap::RangeMap;
use databend_common_base::rangemap::RangeMapKey;
use databend_common_meta_raft_store::state_machine::StateMachineSubscriber;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::Event;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::Change;
use log::info;
use log::warn;
use prost::Message;
use tonic::Status;

use super::WatchStreamHandle;
use crate::metrics::network_metrics;
use crate::metrics::server_metrics;
use crate::watcher::Watcher;

pub type WatcherId = i64;

/// A sender for dispatcher to send event to interested watchers.
pub type WatcherSender = mpsc::Sender<Result<WatchResponse, Status>>;

/// A sender for event source, such as raft state machine, to send event to [`EventDispatcher`].
#[derive(Clone, Debug)]
pub(crate) struct DispatcherSender(pub(crate) mpsc::UnboundedSender<WatchEvent>);

/// An event sent to EventDispatcher.
pub(crate) enum WatchEvent {
    /// Submit a kv change event to dispatcher
    KVChange(Change<Vec<u8>, String>),

    /// Send a request to EventDispatcher.
    ///
    /// The function will be called with a mutable reference to the dispatcher.
    Request {
        req: Box<dyn FnOnce(&mut EventDispatcher) + Send + 'static>,
    },
}

#[derive(Clone, Debug)]
pub struct EventDispatcherHandle {
    /// For sending event or command to the dispatcher.
    pub(crate) tx: mpsc::UnboundedSender<WatchEvent>,
}

impl EventDispatcherHandle {
    pub(crate) fn new(tx: mpsc::UnboundedSender<WatchEvent>) -> Self {
        Self { tx }
    }

    /// Send a request to the watch dispatcher.
    pub fn request(&self, req: impl FnOnce(&mut EventDispatcher) + Send + 'static) {
        let _ = self.tx.send(WatchEvent::Request { req: Box::new(req) });
    }

    /// Send a request to the watch dispatcher and block until finished
    pub async fn request_blocking(&self, req: impl FnOnce(&mut EventDispatcher) + Send + 'static) {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx.send(WatchEvent::Request {
            req: Box::new(|dispatcher| {
                req(dispatcher);
                let _ = tx.send(());
            }),
        });

        let _ = rx.await;
    }
}

/// Receives events from event sources, dispatches them to interested watchers.
pub struct EventDispatcher {
    event_rx: mpsc::UnboundedReceiver<WatchEvent>,

    /// map range to WatcherId
    watcher_range_map: RangeMap<String, WatcherId, WatchStreamHandle>,

    current_watcher_id: WatcherId,
}

impl EventDispatcher {
    /// Spawn a dispatcher loop task.
    pub(crate) fn spawn() -> mpsc::UnboundedSender<WatchEvent> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let dispatcher = EventDispatcher {
            event_rx,
            watcher_range_map: RangeMap::new(),
            current_watcher_id: 1,
        };

        let _h = databend_common_base::runtime::spawn(dispatcher.main());

        event_tx
    }

    #[fastrace::trace]
    async fn main(mut self) {
        loop {
            if let Some(event) = self.event_rx.recv().await {
                match event {
                    WatchEvent::KVChange(kv_change) => {
                        self.dispatch_event(kv_change).await;
                    }
                    WatchEvent::Request { req } => req(&mut self),
                }
            } else {
                info!("all event senders are closed. quit.");
                break;
            }
        }
    }

    /// Dispatch a kv change event to interested watchers.
    async fn dispatch_event(&mut self, change: Change<Vec<u8>, String>) {
        let k = change.ident.as_ref().unwrap();
        let set = self.watcher_range_map.get_by_point(k);
        if set.is_empty() {
            return;
        }

        let current = change.result;
        let prev = change.prev;

        let is_delete_event = current.is_none();
        let mut remove_range_keys: Vec<RangeMapKey<String, WatcherId>> = vec![];

        for range_key_stream in set.iter() {
            let filter = range_key_stream.1.watcher.filter_type;

            // filter out event
            if (filter == FilterType::Delete && !is_delete_event)
                || (filter == FilterType::Update && is_delete_event)
            {
                continue;
            }

            let watcher_id = range_key_stream.0.key;
            let stream = range_key_stream.1;
            assert_eq!(stream.watcher.id, watcher_id);
            let resp = WatchResponse {
                event: Some(Event {
                    key: k.to_string(),
                    current: current.clone().map(pb::SeqV::from),
                    prev: prev.clone().map(pb::SeqV::from),
                }),
            };

            network_metrics::incr_sent_bytes(resp.encoded_len() as u64);

            if let Err(err) = stream.send(resp).await {
                warn!(
                    "close watcher stream {:?} cause send err: {:?}",
                    watcher_id, err
                );
                remove_range_keys.push(RangeMapKey::new(
                    stream.watcher.key_range.clone(),
                    watcher_id,
                ));
            };
        }

        // TODO: when a watcher stream is dropped, send a event to remove the watcher explicitly
        for range_key in remove_range_keys {
            self.remove_watcher(&range_key);
        }
    }

    #[fastrace::trace]
    pub fn add_watcher(
        &mut self,
        create: WatchRequest,
        tx: WatcherSender,
    ) -> Result<Watcher, &'static str> {
        info!("add_watcher: {:?}", create);

        let range = match EventDispatcher::build_key_range(create.key.clone(), &create.key_end) {
            Ok(range) => range,
            Err(e) => return Err(e),
        };

        self.current_watcher_id += 1;
        let watcher_id = self.current_watcher_id;
        let filter: FilterType = create.filter_type();

        let watcher = Watcher::new(watcher_id, filter, range.clone());
        let stream_handle = WatchStreamHandle::new(watcher.clone(), tx);

        self.watcher_range_map
            .insert(range, watcher_id, stream_handle);

        server_metrics::incr_watchers(1);

        Ok(watcher)
    }

    #[fastrace::trace]
    pub fn remove_watcher(&mut self, key: &RangeMapKey<String, WatcherId>) {
        info!("remove_watcher: {:?}", key);

        self.watcher_range_map.remove_by_key(key);

        // TODO: decrease it only when the key is actually removed
        server_metrics::incr_watchers(-1);
    }

    fn build_key_range(
        key: String,
        key_end: &Option<String>,
    ) -> Result<Range<String>, &'static str> {
        match key_end {
            Some(key_end) => {
                if &key > key_end {
                    return Err("empty range");
                }
                Ok(key..key_end.to_string())
            }
            None => Ok(key.clone()..key),
        }
    }

    pub fn watchers(&self) -> impl Iterator<Item = &RangeMapKey<String, WatcherId>> {
        self.watcher_range_map.keys()
    }
}

impl StateMachineSubscriber for DispatcherSender {
    fn kv_changed(&self, change: Change<Vec<u8>, String>) {
        let _ = self.0.send(WatchEvent::KVChange(change));
    }
}
