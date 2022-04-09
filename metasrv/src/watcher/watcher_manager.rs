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
use common_meta_raft_store::state_machine::UpdateType;
use common_meta_types::protobuf::event::EventType;
use common_meta_types::protobuf::watch_request::RequestUnion::CancelRequest;
use common_meta_types::protobuf::watch_request::RequestUnion::CreateRequest;
use common_meta_types::protobuf::Event;
use common_meta_types::protobuf::WatchCancelRequest;
use common_meta_types::protobuf::WatchCreateRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::SeqV;
use common_range_set::RangeSet;
use common_tracing::tracing;
use tonic::Status;
use tonic::Streaming;

use super::Watcher;
use super::WatcherConfig;
use super::WatcherStream;

pub type WatcherId = i64;
pub type WatcherStreamId = i64;
pub type WatcherStreamSender = Sender<Result<WatchResponse, Status>>;
pub type CloseWatcherStreamReq = (WatcherStreamId, String);

type CreateWatcherEvent = (Streaming<WatchRequest>, WatcherStreamSender);

#[derive(Clone, Debug)]
struct StateMachineKvData {
    pub key: String,
    pub update: UpdateType,
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

    /// A channel for sending watch request.
    watch_tx: Arc<mpsc::UnboundedSender<(WatcherStreamId, WatchRequest)>>,

    /// A channel for receiving watch request from grpc client stream.
    watch_rx: mpsc::UnboundedReceiver<(WatcherStreamId, WatchRequest)>,

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

    watcher_streams: BTreeMap<WatcherStreamId, WatcherStream>,

    watchers: BTreeMap<WatcherId, Watcher>,

    /// map range to WatcherId
    watcher_range_set: RangeSet<String, WatcherId>,

    current_stream_id: WatcherStreamId,

    current_watcher_id: WatcherId,
}

impl WatcherManager {
    pub fn create(config: WatcherConfig) -> Self {
        let (create_tx, create_rx) = mpsc::unbounded_channel();
        let (watch_tx, watch_rx) = mpsc::unbounded_channel::<(WatcherStreamId, WatchRequest)>();
        let (close_stream_tx, close_stream_rx) = mpsc::unbounded_channel::<CloseWatcherStreamReq>();
        let (sm_tx, sm_rx) = mpsc::unbounded_channel::<StateMachineKvData>();
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

        let core = WatcherManagerCore {
            create_rx,
            watch_tx: Arc::new(watch_tx),
            watch_rx,
            close_stream_tx: Arc::new(close_stream_tx),
            close_stream_rx,
            sm_rx,
            kv_vec: Vec::new(),
            notify_interval: interval(Duration::from_millis(config.watcher_notify_internal)),
            shutdown_rx,
            watcher_streams: BTreeMap::new(),
            watchers: BTreeMap::new(),
            watcher_range_set: RangeSet::new(),
            current_stream_id: 1,
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

    pub fn create_watcher_stream(&self, stream: Streaming<WatchRequest>, tx: WatcherStreamSender) {
        let _ = self.create_tx.send((stream, tx));
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
                req = self.watch_rx.recv() => {
                    match req {
                        Some((stream_id,req)) => {self.watch_request(stream_id, req).await;},
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
    fn close_stream(&mut self, stream_id: WatcherStreamId, err: String) {
        tracing::info!("close watcher steam {:?} since {:?}", stream_id, err);
        let stream = self.watcher_streams.get(&stream_id).unwrap();
        let watchers = stream.get_watchers().clone();
        self.watcher_streams.remove(&stream_id);
        for watcher_id in watchers.iter() {
            self.remove_watcher(stream_id, *watcher_id);
        }
    }

    fn from_update_type_to_event_type(update: UpdateType) -> EventType {
        return if update == UpdateType::UpInsert {
            EventType::UpInsert
        } else {
            EventType::Delete
        };
    }

    async fn notify_events(&mut self) {
        let kv_vec = &self.kv_vec;

        let mut event_maps = BTreeMap::<WatcherId, Vec<Event>>::new();

        // first aggregate events for each watcher
        for kv in kv_vec {
            let current = match &kv.current {
                Some(current) => current.data.clone(),
                None => vec![],
            };
            let prev = match &kv.prev {
                Some(prev) => prev.data.clone(),
                None => vec![],
            };
            let event: i32 =
                WatcherManagerCore::from_update_type_to_event_type(kv.update.clone()).into();

            let set = self.watcher_range_set.get_by_point(&kv.key);
            for range_key in set.iter() {
                let watcher_id = range_key.key;
                let events = event_maps.get_mut(&watcher_id);
                match events {
                    Some(events) => {
                        events.push(Event {
                            key: kv.key.clone(),
                            event,
                            current: current.clone(),
                            prev: prev.clone(),
                        });
                    }
                    None => {
                        let mut events = Vec::<Event>::new();
                        events.push(Event {
                            key: kv.key.clone(),
                            event,
                            current: current.clone(),
                            prev: prev.clone(),
                        });
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
                    created: false,
                    canceled: false,
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
        mut stream: Streaming<WatchRequest>,
        tx: WatcherStreamSender,
    ) {
        //let Some(req) = stream.message().await.unwrap;
        let req = stream.message().await.unwrap().unwrap();

        match req.request_union {
            Some(CreateRequest(create)) => {
                tracing::info!("create_watcher_stream: {:?}", create);

                let watcher_stream = WatcherStream::spawn(
                    self.current_stream_id,
                    stream,
                    tx,
                    self.watch_tx.clone(),
                    self.close_stream_tx.clone(),
                );

                self.watcher_streams
                    .insert(self.current_stream_id, watcher_stream);

                self.current_stream_id += 1;

                self.create_watcher(self.current_stream_id, create);
            }
            _ => {}
        }
    }

    async fn watch_request(&mut self, stream_id: WatcherStreamId, req: WatchRequest) {
        match req.request_union {
            Some(req) => match req {
                CreateRequest(create) => {
                    self.create_watcher(stream_id, create);
                }
                CancelRequest(cancel) => {
                    self.cancel_watcher(stream_id, cancel).await;
                }
            },
            None => {}
        }
    }

    fn get_range_key(key: &String, key_end: &String) -> Range<String> {
        let key_end = format!("{:?}\x00", key_end);

        return key.clone()..key_end.clone();
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn create_watcher(&mut self, stream_id: WatcherStreamId, create: WatchCreateRequest) {
        if create.key < create.key_end {
            return;
        }
        let stream = self.watcher_streams.get_mut(&stream_id).unwrap();

        let range = WatcherManagerCore::get_range_key(&create.key, &create.key_end);

        let watcher_id = self.current_watcher_id;
        self.current_watcher_id += 1;
        let watcher = Watcher::new(watcher_id, stream_id, create.clone());

        self.watcher_range_set.insert(range, watcher_id);
        self.watchers.insert(watcher_id, watcher);

        stream.add_watcher(watcher_id);
        tracing::info!("create watcher {:?} of stream {:?}", watcher_id, stream_id);
        let _ = stream.send(WatchResponse {
            watch_id: watcher_id,
            created: true,
            canceled: false,
            events: vec![],
        });
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn remove_watcher(&mut self, stream_id: WatcherStreamId, watcher_id: WatcherId) {
        tracing::info!("remove watcher {:?} of stream {:?}", watcher_id, stream_id);
        let watcher = self.watchers.get(&watcher_id).unwrap();
        assert_eq!(watcher.owner_id, stream_id);
        let range = WatcherManagerCore::get_range_key(&watcher.key, &watcher.key_end);
        self.watcher_range_set.remove(range, watcher_id);
        self.watchers.remove(&watcher_id);
    }

    async fn remove_watcher_from_stream(
        &mut self,
        stream_id: WatcherStreamId,
        watcher_id: WatcherId,
    ) {
        let stream = self.watcher_streams.get(&stream_id).unwrap();
        let _ = stream
            .send(WatchResponse {
                watch_id: watcher_id,
                created: false,
                canceled: true,
                events: vec![],
            })
            .await;
    }

    async fn cancel_watcher(&mut self, stream_id: WatcherStreamId, cancel: WatchCancelRequest) {
        self.remove_watcher_from_stream(stream_id, cancel.watcher_id)
            .await;

        self.remove_watcher(stream_id, cancel.watcher_id);
    }
}

#[async_trait::async_trait]
impl StateMachineSubscriber for WatcherStateMachineSubscriber {
    async fn kv_changed(
        &self,
        key: &str,
        update: UpdateType,
        prev: &Option<SeqV>,
        current: &Option<SeqV>,
    ) {
        let _ = self.sm_tx.send(StateMachineKvData {
            key: key.to_string(),
            update,
            prev: prev.clone(),
            current: current.clone(),
        });
    }
}
