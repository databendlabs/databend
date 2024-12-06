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
use std::collections::Bound;
use std::sync::Arc;

use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::Change;
use log::info;
use log::warn;
use prost::Message;
use span_map::SpanMap;
use tokio::sync::mpsc;
use tonic::Status;

use crate::metrics::network_metrics;
use crate::metrics::server_metrics;
use crate::watcher::command::Command;
use crate::watcher::id::WatcherId;
use crate::watcher::subscriber_handle::SubscriberHandle;
use crate::watcher::KeyRange;
use crate::watcher::StreamSender;
use crate::watcher::WatchDesc;

/// Receives events from event sources(such as raft state machine),
/// dispatches them to interested watchers.
pub struct EventSubscriber {
    rx: mpsc::UnboundedReceiver<Command>,

    watchers: SpanMap<String, Arc<StreamSender>>,

    current_watcher_id: WatcherId,
}

impl EventSubscriber {
    /// Spawn a dispatcher loop task.
    pub(crate) fn spawn() -> SubscriberHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        let subscriber = EventSubscriber {
            rx,
            watchers: SpanMap::new(),
            current_watcher_id: 1,
        };

        let _h = databend_common_base::runtime::spawn(subscriber.main());

        SubscriberHandle::new(tx)
    }

    #[fastrace::trace]
    async fn main(mut self) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Command::KVChange(kv_change) => {
                    self.dispatch(kv_change).await;
                }
                Command::Request { req } => req(&mut self),
            }
        }

        info!("EventDispatcher: all event senders are closed. quit.");
    }

    /// Dispatch a kv change event to interested watchers.
    async fn dispatch(&mut self, change: Change<Vec<u8>, String>) {
        let Some(key) = change.ident.clone() else {
            warn!("EventSubscriber: change event without key; ignore it");
            return;
        };

        let is_delete = change.result.is_none();

        let resp = WatchResponse::new(&change).unwrap();
        let resp_size = resp.encoded_len() as u64;

        let mut removed = vec![];

        for sender in self.watchers.get(&key) {
            let interested = sender.desc.interested;

            match interested {
                FilterType::All => {}
                FilterType::Update => {
                    if is_delete {
                        continue;
                    }
                }
                FilterType::Delete => {
                    if !is_delete {
                        continue;
                    }
                }
            }

            if let Err(_err) = sender.send(resp.clone()).await {
                warn!(
                    "EventSubscriber: fail to send to watcher {}; close this stream",
                    sender.desc.watcher_id
                );
                removed.push(sender.clone());
            } else {
                network_metrics::incr_sent_bytes(resp_size);
            };
        }

        for sender in removed {
            self.remove_watcher(sender);
        }
    }

    #[fastrace::trace]
    pub fn add_watcher(
        &mut self,
        req: WatchRequest,
        tx: mpsc::Sender<Result<WatchResponse, Status>>,
    ) -> Result<Arc<StreamSender>, &'static str> {
        info!("EventSubscriber::add_watcher: {:?}", req);

        let interested = req.filter_type();
        let desc = self.new_watch_desc(req.key, req.key_end, interested)?;

        let stream_sender = Arc::new(StreamSender::new(desc, tx));

        self.watchers
            .insert(stream_sender.desc.key_range.clone(), stream_sender.clone());

        server_metrics::incr_watchers(1);

        Ok(stream_sender)
    }

    fn new_watch_desc(
        &mut self,
        key: String,
        key_end: Option<String>,
        interested: FilterType,
    ) -> Result<WatchDesc, &'static str> {
        self.current_watcher_id += 1;
        let watcher_id = self.current_watcher_id;

        let range = Self::build_key_range(key.clone(), &key_end)?;

        let desc = WatchDesc::new(watcher_id, interested, range);
        Ok(desc)
    }

    #[fastrace::trace]
    pub fn remove_watcher(&mut self, stream_sender: Arc<StreamSender>) {
        info!("EventSubscriber::remove_watcher: {:?}", stream_sender);

        self.watchers.remove(.., stream_sender);

        server_metrics::incr_watchers(-1);
    }

    pub(crate) fn build_key_range(
        key: String,
        key_end: &Option<String>,
    ) -> Result<KeyRange, &'static str> {
        let left = Bound::Included(key.clone());

        match key_end {
            Some(key_end) => {
                if &key >= key_end {
                    return Err("empty range");
                }
                Ok((left, Bound::Excluded(key_end.to_string())))
            }
            None => Ok((left.clone(), left)),
        }
    }

    pub fn watch_senders(&self) -> BTreeSet<&Arc<StreamSender>> {
        self.watchers.values(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_build_key_range() -> Result<(), &'static str> {
        let x = EventSubscriber::build_key_range(s("a"), &None)?;
        assert_eq!(x, (Bound::Included(s("a")), Bound::Included(s("a"))));

        let x = EventSubscriber::build_key_range(s("a"), &Some(s("b")))?;
        assert_eq!(x, (Bound::Included(s("a")), Bound::Excluded(s("b"))));

        let x = EventSubscriber::build_key_range(s("a"), &Some(s("a")));
        assert_eq!(x, Err("empty range"));

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }
}
