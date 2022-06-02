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
use common_meta_types::PbSeqV;
use common_meta_types::SeqV;
use common_tracing::tracing;
use tonic::Status;

use super::WatcherStream;
use crate::metrics::incr_meta_metrics_watchers;

pub type WatcherId = i64;
pub type WatcherStreamSender = Sender<Result<WatchResponse, Status>>;

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
}

#[derive(Debug)]
pub struct WatcherManager {
    event_tx: mpsc::UnboundedSender<WatcherEvent>,

    pub subscriber: WatcherStateMachineSubscriber,
}

struct WatcherManagerCore {
    event_rx: mpsc::UnboundedReceiver<WatcherEvent>,

    /// map range to WatcherId
    watcher_range_map: RangeMap<String, WatcherId, WatcherStream>,

    current_watcher_id: WatcherId,
}

impl WatcherManager {
    pub fn create() -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let core = WatcherManagerCore {
            event_rx,
            watcher_range_map: RangeMap::new(),
            current_watcher_id: 1,
        };

        let _h = tokio::spawn(core.watcher_manager_main());

        WatcherManager {
            event_tx: event_tx.clone(),
            subscriber: WatcherStateMachineSubscriber { event_tx },
        }
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
            if let Some(event) = self.event_rx.recv().await {
                match event {
                    WatcherEvent::CreateWatcherEvent((req, tx)) => {
                        self.create_watcher_stream(req, tx).await;
                    }
                    WatcherEvent::StateMachineKvDataEvent(kv) => {
                        self.notify_event(kv).await;
                    }
                }
            } else {
                tracing::info!("watcher manager has been shutdown");
                break;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn close_stream(&mut self, key: RangeMapKey<String, WatcherId>) {
        self.watcher_range_map.remove_by_key(&key);

        incr_meta_metrics_watchers(-1);
    }

    async fn notify_event(&mut self, kv: StateMachineKvData) {
        let set = self.watcher_range_map.get_by_point(&kv.key);
        if set.is_empty() {
            return;
        }

        let current = kv.current;
        let prev = kv.prev;

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
                    key: kv.key.clone(),
                    current: current.clone().map(PbSeqV::from),
                    prev: prev.clone().map(PbSeqV::from),
                }),
            };

            if let Err(err) = stream.send(resp).await {
                tracing::info!(
                    "close watcher stream {:?} cause send err: {:?}",
                    watcher_id,
                    err
                );
                remove_range_keys.push(RangeMapKey::new(
                    stream.key.clone()..stream.key_end.clone(),
                    watcher_id,
                ));
            };
        }

        for range_key in remove_range_keys {
            self.close_stream(range_key);
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_watcher_stream(&mut self, create: WatchRequest, tx: WatcherStreamSender) {
        tracing::info!("create_watcher_stream: {:?}", create);

        let range = match WatcherManagerCore::get_range_key(create.key.clone(), &create.key_end) {
            Ok(range) => range,
            Err(_) => return,
        };

        self.current_watcher_id += 1;
        let watcher_id = self.current_watcher_id;
        let filter = create.filter_type();

        let watcher_stream = WatcherStream::new(
            watcher_id,
            filter,
            tx,
            range.start.clone(),
            range.end.clone(),
        );

        self.watcher_range_map
            .insert(range, watcher_id, watcher_stream);

        incr_meta_metrics_watchers(1);
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
