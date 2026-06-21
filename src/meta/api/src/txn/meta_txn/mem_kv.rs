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

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::KVStream;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::Change;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnReply;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::protobuf::StreamItem;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;

/// An in-memory `KVApi` for unit tests.
///
/// It counts backend reads, records every transaction it receives, and replays
/// scripted commit outcomes — enough to assert what `MetaTxn` reads, caches, and
/// commits without standing up a real meta service.
pub(crate) struct MemKV {
    store: Mutex<BTreeMap<String, SeqV<Vec<u8>>>>,
    reads: AtomicUsize,
    replies: Mutex<VecDeque<bool>>,
    requests: Mutex<Vec<TxnRequest>>,
}

impl MemKV {
    pub(crate) fn new() -> Self {
        Self {
            store: Mutex::new(BTreeMap::new()),
            reads: AtomicUsize::new(0),
            replies: Mutex::new(VecDeque::new()),
            requests: Mutex::new(vec![]),
        }
    }

    pub(crate) fn seed(&self, key: &str, seq: u64, value: Vec<u8>) {
        self.seed_seqv(key, SeqV::new(seq, value));
    }

    /// Seed a full record, including its meta.
    pub(crate) fn seed_seqv(&self, key: &str, seqv: SeqV<Vec<u8>>) {
        self.store.lock().unwrap().insert(key.to_string(), seqv);
    }

    pub(crate) fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    /// Script the success/failure of the next transactions, in order.
    pub(crate) fn script(&self, replies: impl IntoIterator<Item = bool>) {
        *self.replies.lock().unwrap() = replies.into_iter().collect();
    }

    pub(crate) fn last_request(&self) -> TxnRequest {
        self.requests.lock().unwrap().last().cloned().unwrap()
    }
}

#[async_trait]
impl KVApi for MemKV {
    type Error = MetaError;

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let keys: Vec<String> = keys.try_collect().await?;
        self.reads.fetch_add(keys.len(), Ordering::SeqCst);

        let store = self.store.lock().unwrap();
        let items: Vec<Result<StreamItem, Self::Error>> = keys
            .into_iter()
            .map(|k| {
                let v = store.get(&k).cloned();
                Ok(StreamItem::new(k, v.map(|s| s.into())))
            })
            .collect();

        Ok(futures::stream::iter(items).boxed())
    }

    async fn list_kv(
        &self,
        _opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        Ok(futures::stream::empty::<Result<StreamItem, Self::Error>>().boxed())
    }

    async fn upsert_kv(&self, _req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        unimplemented!("MemKV does not support upsert_kv")
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        self.requests.lock().unwrap().push(txn);
        let ok = self.replies.lock().unwrap().pop_front().unwrap_or(true);
        Ok(TxnReply::new(if ok { "then" } else { "else" }))
    }
}
