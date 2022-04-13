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
use std::sync::Arc;

use common_base::tokio;
use common_base::tokio::sync::mpsc;
use common_base::tokio::sync::mpsc::Sender;
use common_base::tokio::time::interval;
use common_base::tokio::time::Duration;
use common_base::tokio::time::Interval;
use common_meta_raft_store::state_machine::StateMachineSubscriber;
use common_meta_types::protobuf::event::EventType;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::SeqV;
use common_range_set::RangeSet;
use common_tracing::tracing;
use tonic::Request;
use tonic::Status;

use super::WatcherConfig;
use super::WatcherKey;
use super::WatcherStream;

pub type WatcherId = i64;
pub type WatcherStreamSender = Sender<Result<WatchResponse, Status>>;
pub type CloseWatcherStreamReq = (WatcherId, String);

type CreateWatcherEvent = (Request<WatchRequest>, WatcherStreamSender);

#[derive(Clone, Debug)]
struct StateMachineKvData {
    pub key: String,
    pub event: EventType,
    pub prev: Option<SeqV>,
    pub current: Option<SeqV>,
}

#[derive(Clone, Debug)]
pub struct WatcherStateMachineSubscriber {
    sm_tx: mpsc::UnboundedSender<StateMachineKvData>,
}

#[derive(Debug)]
pub struct WatcherManager {
    /// A channel for sending create watch request.
    create_tx: mpsc::UnboundedSender<CreateWatcherEvent>,

    shutdown_tx: mpsc::UnboundedSender<()>,

    pub subscriber: WatcherStateMachineSubscriber,
}

#[derive(Debug)]
struct WatcherManagerCore {
    /// A channel for receiving create watcher request from grpc service.
    create_rx: mpsc::UnboundedReceiver<CreateWatcherEvent>,

    sm_rx: mpsc::UnboundedReceiver<StateMachineKvData>,

    /// A channel for sending stop stream request.
    close_stream_tx: Arc<mpsc::UnboundedSender<CloseWatcherStreamReq>>,

    /// A channel for receiving stop stream request.
    close_stream_rx: mpsc::UnboundedReceiver<CloseWatcherStreamReq>,

    /// save subscribe kv data
    kv_vec: Vec<StateMachineKvData>,

    /// update events to watcher internal
    notify_interval: Interval,

    shutdown_rx: mpsc::UnboundedReceiver<()>,

    watcher_streams: BTreeMap<WatcherId, WatcherStream>,

    /// map range to WatcherId
    watcher_range_set: RangeSet<String, WatcherKey>,

    current_watcher_id: WatcherId,
}

impl WatcherManager {
    pub fn create(config: WatcherConfig) -> Self {
        let (create_tx, create_rx) = mpsc::unbounded_channel();
        let (close_stream_tx, close_stream_rx) = mpsc::unbounded_channel::<CloseWatcherStreamReq>();
        let (sm_tx, sm_rx) = mpsc::unbounded_channel::<StateMachineKvData>();
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

        let core = WatcherManagerCore {
            create_rx,
            close_stream_tx: Arc::new(close_stream_tx),
            close_stream_rx,
            sm_rx,
            kv_vec: Vec::new(),
            notify_interval: interval(Duration::from_millis(config.watcher_notify_internal)),
            shutdown_rx,
            watcher_streams: BTreeMap::new(),
            watcher_range_set: RangeSet::new(),
            current_watcher_id: 1,
        };

        let _h = tokio::spawn(core.watcher_manager_main());

        WatcherManager {
            create_tx,
            shutdown_tx,
            subscriber: WatcherStateMachineSubscriber { sm_tx },
        }
    }

    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }

    pub fn create_watcher_stream(&self, request: Request<WatchRequest>, tx: WatcherStreamSender) {
        let _ = self.create_tx.send((request, tx));
    }
}

impl WatcherManagerCore {
    //#[tracing::instrument(level = "trace", skip(self))]
    async fn watcher_manager_main(mut self) {
        loop {
            tokio::select! {
                create = self.create_rx.recv() => {
                    match create {
                        Some((streaming, tx)) => {self.create_watcher_stream(streaming, tx).await;},
                        None => {},
                    }
                },
                close_stream_req = self.close_stream_rx.recv() => {
                    match close_stream_req {
                        Some((stream_id, err)) => {self.close_stream(stream_id, err);},
                        None => {},
                    }
                },
                kv = self.sm_rx.recv() => {
                    if let Some(kv) = kv { self.recv_kv(kv);}
                }
                _ = self.notify_interval.tick() => {
                    self.notify_events().await;
                },
                _ = self.shutdown_rx.recv() => {
                    break;
                }
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

    async fn notify_events(&mut self) {
        let kv_vec = &self.kv_vec;
        if kv_vec.is_empty() {
            return;
        }
        let mut event_maps = BTreeMap::<WatcherId, Vec<Event>>::new();

        // first aggregate events for each watcher
        for kv in kv_vec {
            let current = kv.current.as_ref().map(|current| current.data.clone());

            let prev = kv.prev.as_ref().map(|prev| prev.data.clone());

            let event = kv.event;

            let set = self.watcher_range_set.get_by_point(&kv.key);

            for range_key in set.iter() {
                let watcher_id = range_key.key.id;
                let filter = range_key.key.filter;

                // filter out event
                if (filter == FilterType::Delete && event == EventType::Update)
                    || (filter == FilterType::Update && event == EventType::Delete)
                {
                    continue;
                }

                let events = event_maps.get_mut(&watcher_id);
                match events {
                    Some(events) => {
                        events.push(Event {
                            key: kv.key.clone(),
                            event: kv.event.into(),
                            current: current.clone(),
                            prev: prev.clone(),
                        });
                    }
                    None => {
                        let events = vec![Event {
                            key: kv.key.clone(),
                            event: kv.event.into(),
                            current: current.clone(),
                            prev: prev.clone(),
                        }];
                        event_maps.insert(watcher_id, events);
                    }
                }
            }
        }

        // then send events to watchers
        for (watcher_id, events) in event_maps.iter() {
            if let Some(stream) = self.watcher_streams.get(watcher_id) {
                let resp = WatchResponse {
                    watch_id: *watcher_id,
                    created: None,
                    canceled: None,
                    events: events.to_vec(),
                };

                stream.send(resp).await;
            }
        }

        self.kv_vec = Vec::new();
    }

    fn recv_kv(&mut self, kv: StateMachineKvData) {
        self.kv_vec.push(kv);
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_watcher_stream(
        &mut self,
        req: Request<WatchRequest>,
        tx: WatcherStreamSender,
    ) {
        let create = req.get_ref();

        tracing::info!("create_watcher_stream: {:?}", create);

        let range = match WatcherManagerCore::get_range_key(create.key.clone(), &create.key_end) {
            Ok(range) => range,
            Err(_) => return,
        };

        let watcher_id = self.current_watcher_id;
        let watcher_stream = WatcherStream::new(
            self.current_watcher_id,
            tx,
            self.close_stream_tx.clone(),
            range.start.clone(),
            range.end.clone(),
        );

        let filter = create.filter_type();

        self.watcher_range_set.insert(range, WatcherKey {
            id: watcher_id,
            filter,
        });

        let _ = watcher_stream.send(WatchResponse {
            watch_id: watcher_id,
            created: Some(true),
            canceled: None,
            events: vec![],
        });

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
        let event = match current {
            Some(_) => EventType::Update,
            None => EventType::Delete,
        };
        let _ = self.sm_tx.send(StateMachineKvData {
            key: key.to_string(),
            event,
            prev,
            current,
        });
    }
}
