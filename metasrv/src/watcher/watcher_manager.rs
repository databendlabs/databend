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
use core::ops::Range;
use std::collections::BTreeMap;

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::mpsc::Sender;
use common_meta_raft_store::state_machine::StateMachineSubscriber;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::SeqV;
use common_range_set::RangeSet;
use common_tracing::tracing;
use tonic::Status;

use super::WatcherKey;
use super::WatcherStream;

pub type WatcherId = i64;
pub type WatcherStreamSender = Sender<Result<WatchResponse, Status>>;
pub type CloseWatcherStream = (WatcherId, String);

type CreateWatcherEvent = (WatchRequest, WatcherStreamSender);

#[derive(Clone, Debug)]
pub struct StateMachineKvData {
    pub key: String,
    pub prev: Option<SeqV>,
    pub current: Option<SeqV>,
}

#[derive(Clone, Debug)]
pub struct WatcherStateMachineSubscriber {
    event_tx: mpsc::UnboundedSender<WatcherEvent>,
}

#[derive(Clone)]
pub enum WatcherEvent {
    CreateWatcherEvent(CreateWatcherEvent),
    StateMachineKvDataEvent(StateMachineKvData),
    CloseWatcherStreamEvent(CloseWatcherStream),
    ShutdownEvent(()),
}

#[derive(Debug)]
pub struct WatcherManager {
    event_tx: mpsc::UnboundedSender<WatcherEvent>,

    pub subscriber: WatcherStateMachineSubscriber,
}

struct WatcherManagerCore {
    event_rx: mpsc::UnboundedReceiver<WatcherEvent>,

    event_tx: mpsc::UnboundedSender<WatcherEvent>,

    watcher_streams: BTreeMap<WatcherId, WatcherStream>,

    /// map range to WatcherId
    watcher_range_set: RangeSet<String, WatcherKey>,

    current_watcher_id: WatcherId,
}

impl WatcherManager {
    pub fn create() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let core = WatcherManagerCore {
            event_rx,
            event_tx: event_tx.clone(),
            watcher_streams: BTreeMap::new(),
            watcher_range_set: RangeSet::new(),
            current_watcher_id: 1,
        };

        let _h = tokio::spawn(core.watcher_manager_main());

        WatcherManager {
            event_tx: event_tx.clone(),
            subscriber: WatcherStateMachineSubscriber { event_tx },
        }
    }

    pub fn stop(&self) {
        let _ = self.event_tx.send(WatcherEvent::ShutdownEvent(()));
    }

    pub fn create_watcher_stream(&self, request: WatchRequest, tx: WatcherStreamSender) {
        let create: CreateWatcherEvent = (request, tx);
        let _ = self.event_tx.send(WatcherEvent::CreateWatcherEvent(create));
    }
}

impl WatcherManagerCore {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn watcher_manager_main(mut self) {
        loop {
            tokio::select! {
                event = self.event_rx.recv() => {
                    if let Some(event) = event {
                        match event {
                            WatcherEvent::CreateWatcherEvent((req, tx)) => {self.create_watcher_stream(req, tx).await;},
                            WatcherEvent::StateMachineKvDataEvent(kv) => {self.recv_kv(kv).await;},
                            WatcherEvent::CloseWatcherStreamEvent((stream_id, err)) => {self.close_stream(stream_id, err);},
                            WatcherEvent::ShutdownEvent(_) => {
                                tracing::info!("watcher manager has been shutdown");
                                break;}
                        }
                    }
                },
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn close_stream(&mut self, id: WatcherId, err: String) {
        tracing::info!("close watcher steam {:?} since {:?}", id, err);

        let watcher = self.watcher_streams.get(&id);
        if let Some(watcher) = watcher {
            let range = watcher.key.clone()..watcher.key_end.clone();
            self.watcher_range_set.remove(range, WatcherKey {
                id,
                filter: FilterType::All,
            });

            self.watcher_streams.remove(&id);
        }
    }

    async fn recv_kv(&mut self, kv: StateMachineKvData) {
        let set = self.watcher_range_set.get_by_point(&kv.key);
        if set.is_empty() {
            return;
        }
        let current = kv.current.as_ref().map(|current| current.data.clone());

        let prev = kv.prev.as_ref().map(|prev| prev.data.clone());

        let is_delete_event = kv.current.is_none();

        for range_key in set.iter() {
            let watcher_id = range_key.key.id;
            let filter = range_key.key.filter;

            // filter out event
            if (filter == FilterType::Delete && !is_delete_event)
                || (filter == FilterType::Update && is_delete_event)
            {
                continue;
            }

            if let Some(stream) = self.watcher_streams.get(&watcher_id) {
                let resp = WatchResponse {
                    event: Some(Event {
                        key: kv.key.clone(),
                        current: current.clone(),
                        prev: prev.clone(),
                    }),
                };

                stream.send(resp).await;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_watcher_stream(&mut self, create: WatchRequest, tx: WatcherStreamSender) {
        tracing::info!("create_watcher_stream: {:?}", create);

        let range = match WatcherManagerCore::get_range_key(create.key.clone(), &create.key_end) {
            Ok(range) => range,
            Err(_) => return,
        };

        let watcher_id = self.current_watcher_id;
        let watcher_stream = WatcherStream::new(
            self.current_watcher_id,
            tx,
            self.event_tx.clone(),
            range.start.clone(),
            range.end.clone(),
        );

        let filter = create.filter_type();

        self.watcher_range_set.insert(range, WatcherKey {
            id: watcher_id,
            filter,
        });

        // send None event to notify stream has been created.
        let _ = watcher_stream.send(WatchResponse { event: None });

        self.watcher_streams
            .insert(self.current_watcher_id, watcher_stream);

        self.current_watcher_id += 1;
    }

    fn get_range_key(key: String, key_end: &Option<String>) -> Result<Range<String>, bool> {
        match key_end {
            Some(key_end) => {
                if &key > key_end {
                    return Err(false);
                }
                Ok(key..format!("{}\x00", key_end))
            }
            None => Ok(key.clone()..key),
        }
    }
}

impl StateMachineSubscriber for WatcherStateMachineSubscriber {
    fn kv_changed(&self, key: &str, prev: Option<SeqV>, current: Option<SeqV>) {
        let _ = self
            .event_tx
            .send(WatcherEvent::StateMachineKvDataEvent(StateMachineKvData {
                key: key.to_string(),
                prev,
                current,
            }));
    }
}
