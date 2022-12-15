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
use core::ops::Range;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::rangemap::RangeMap;
use common_base::rangemap::RangeMapKey;
use common_meta_raft_store::state_machine::StateMachineSubscriber;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::Change;
use common_meta_types::PbSeqV;
use prost::Message;
use tonic::Status;
use tracing::info;
use tracing::warn;

use super::WatcherStream;
use crate::metrics::network_metrics;
use crate::metrics::server_metrics;

pub type WatcherId = i64;

/// A sender for dispatcher to send event to interested watchers.
pub type WatcherSender = Sender<Result<WatchResponse, Status>>;

/// A sender for event source, such as raft state machine, to send event to [`EventDispatcher`].
#[derive(Clone, Debug)]
pub(crate) struct DispatcherSender(pub(crate) mpsc::UnboundedSender<WatchEvent>);

#[derive(Clone)]
pub enum WatchEvent {
    AddWatcher((WatchRequest, WatcherSender)),
    KVChange(Change<Vec<u8>, String>),
}

/// Receives events from event sources, dispatches them to interested watchers.
pub(crate) struct EventDispatcher {
    event_rx: mpsc::UnboundedReceiver<WatchEvent>,

    /// map range to WatcherId
    watcher_range_map: RangeMap<String, WatcherId, WatcherStream>,

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

        let _h = tokio::spawn(dispatcher.main());

        event_tx
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn main(mut self) {
        loop {
            if let Some(event) = self.event_rx.recv().await {
                match event {
                    WatchEvent::AddWatcher((req, tx)) => {
                        self.add_watcher(req, tx).await;
                    }
                    WatchEvent::KVChange(kv_change) => {
                        self.dispatch_event(kv_change).await;
                    }
                }
            } else {
                info!("all event senders are closed. quit.");
                break;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn close_stream(&mut self, key: RangeMapKey<String, WatcherId>) {
        self.watcher_range_map.remove_by_key(&key);

        server_metrics::incr_watchers(-1);
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
            let filter = range_key_stream.1.filter_type;

            // filter out event
            if (filter == FilterType::Delete && !is_delete_event)
                || (filter == FilterType::Update && is_delete_event)
            {
                continue;
            }

            let watcher_id = range_key_stream.0.key;
            let stream = range_key_stream.1;
            assert_eq!(stream.id, watcher_id);
            let resp = WatchResponse {
                event: Some(Event {
                    key: k.to_string(),
                    current: current.clone().map(PbSeqV::from),
                    prev: prev.clone().map(PbSeqV::from),
                }),
            };

            network_metrics::incr_sent_bytes(resp.encoded_len() as u64);

            if let Err(err) = stream.send(resp).await {
                warn!(
                    "close watcher stream {:?} cause send err: {:?}",
                    watcher_id, err
                );
                remove_range_keys.push(RangeMapKey::new(
                    stream.key.clone()..stream.key_end.clone(),
                    watcher_id,
                ));
            };
        }

        // TODO: when a watcher stream is dropped, send a event to remove the watcher explicitly
        for range_key in remove_range_keys {
            self.close_stream(range_key);
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_watcher(&mut self, create: WatchRequest, tx: WatcherSender) {
        info!("create_watcher_stream: {:?}", create);

        let range = match EventDispatcher::get_range_key(create.key.clone(), &create.key_end) {
            Ok(range) => range,
            Err(_) => return,
        };

        self.current_watcher_id += 1;
        let watcher_id = self.current_watcher_id;
        let filter: FilterType = create.filter_type();

        let watcher_stream = WatcherStream::new(
            watcher_id,
            filter,
            tx,
            range.start.clone(),
            range.end.clone(),
        );

        self.watcher_range_map
            .insert(range, watcher_id, watcher_stream);

        server_metrics::incr_watchers(1);
    }

    fn get_range_key(key: String, key_end: &Option<String>) -> Result<Range<String>, bool> {
        match key_end {
            Some(key_end) => {
                if &key > key_end {
                    return Err(false);
                }
                Ok(key..key_end.to_string())
            }
            None => Ok(key.clone()..key),
        }
    }
}

impl StateMachineSubscriber for DispatcherSender {
    fn kv_changed(&self, change: Change<Vec<u8>, String>) {
        let _ = self.0.send(WatchEvent::KVChange(change));
    }
}
