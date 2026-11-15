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
use std::time::Duration;

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
use databend_meta_client::types::txn_op_response::Response;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;

use super::MetaReadCache;
use super::ttl_cache::Items;
use super::ttl_cache::Read;
use super::ttl_cache::ReadStart;

/// A [`KVApi`] that keeps [`MetaReadCache`] consistent with the writes it forwards, and
/// optionally answers reads from it.
///
/// - [`CachedKvApi::invalidating`] forwards every call; a write drops the results of its range.
///   Use it for everything that may read-modify-write.
/// - [`CachedKvApi::reading`] additionally answers `get`/`list` under cached ranges from memory.
///   Use it only for plain reads: the returned seq may be stale.
pub struct CachedKvApi {
    inner: Arc<dyn KVApi<Error = MetaError>>,
    cache: Arc<MetaReadCache>,
    serve_reads: bool,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Whether `v` is still alive at `now_ms`; a TTL key may outlive its expiry until meta-service
/// deletes it, and a direct read would not return it.
fn is_live(v: &SeqV, now_ms: u64) -> bool {
    v.meta
        .as_ref()
        .and_then(|m| m.get_expire_at_ms())
        .is_none_or(|expire_at| expire_at > now_ms)
}

fn live(items: Items) -> Items {
    let now = now_ms();
    items.into_iter().filter(|(_, v)| is_live(v, now)).collect()
}

impl CachedKvApi {
    pub fn invalidating(
        inner: Arc<dyn KVApi<Error = MetaError>>,
        cache: Arc<MetaReadCache>,
    ) -> Self {
        CachedKvApi {
            inner,
            cache,
            serve_reads: false,
        }
    }

    pub fn reading(inner: Arc<dyn KVApi<Error = MetaError>>, cache: Arc<MetaReadCache>) -> Self {
        CachedKvApi {
            inner,
            cache,
            serve_reads: true,
        }
    }

    /// `None` if reads are not answered from memory.
    fn ttl(&self) -> Option<Duration> {
        if self.serve_reads {
            self.cache.ttl()
        } else {
            None
        }
    }

    fn invalidate(&self, key: &str) {
        if let Some(c) = self.cache.find(key) {
            c.invalidate();
        }
    }

    fn invalidate_txn(&self, reply: &TxnReply) {
        for resp in &reply.responses {
            match &resp.response {
                Some(Response::Put(put)) => self.invalidate(&put.key),
                Some(Response::Delete(del)) => self.invalidate(&del.key),
                Some(Response::DeleteByPrefix(del)) => self.invalidate(&del.prefix),
                _ => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl KVApi for CachedKvApi {
    type Error = MetaError;

    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        let key = req.key.clone();
        let res = self.inner.upsert_kv(req).await;
        // Also after an error: the write may have been applied.
        self.invalidate(&key);
        res
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        let Some(ttl) = self.ttl() else {
            return self.inner.get_many_kv(keys).await;
        };
        let keys: Vec<String> = keys.try_collect().await?;
        // Outer `None`: not answered from memory.
        let mut values: Vec<Option<Option<SeqV>>> = keys
            .iter()
            .map(|k| {
                let c = self.cache.find(k)?;
                let items = live(c.get(&Read::Get(k.clone()), ttl)?);
                Some(items.into_iter().next().map(|(_, v)| v))
            })
            .collect();
        let missing: Vec<usize> = (0..keys.len()).filter(|&i| values[i].is_none()).collect();
        if !missing.is_empty() {
            let starts: Vec<Option<ReadStart>> = missing
                .iter()
                .map(|&i| self.cache.find(&keys[i]).map(|c| c.start_read()))
                .collect();
            let req: Vec<Result<String, MetaError>> =
                missing.iter().map(|&i| Ok(keys[i].clone())).collect();
            let fetched: Vec<StreamItem> = self
                .inner
                .get_many_kv(stream::iter(req).boxed())
                .await?
                .try_collect()
                .await?;
            for ((&i, item), start) in missing.iter().zip(fetched).zip(starts) {
                let value = item.value.map(SeqV::from);
                if let (Some(c), Some(start)) = (self.cache.find(&keys[i]), start) {
                    let items = value.iter().map(|v| (keys[i].clone(), v.clone())).collect();
                    c.put(Read::Get(keys[i].clone()), items, start, ttl);
                }
                values[i] = Some(value);
            }
        }
        let items = keys
            .into_iter()
            .zip(values)
            .map(|(k, v)| Ok(StreamItem::from((k, v.flatten()))));
        Ok(stream::iter(items).boxed())
    }

    async fn list_kv(
        &self,
        opts: ListOptions<'_, str>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        // A limited list is not the whole result of its prefix: never cached.
        let cached = match (self.ttl(), self.cache.find(opts.prefix), opts.limit) {
            (Some(ttl), Some(c), None) => Some((ttl, c)),
            _ => None,
        };
        let Some((ttl, c)) = cached else {
            return self.inner.list_kv(opts).await;
        };
        let read = Read::List(opts.prefix.to_string());
        let items = match c.get(&read, ttl) {
            Some(items) => items,
            None => {
                let start = c.start_read();
                let fetched: Vec<StreamItem> =
                    self.inner.list_kv(opts).await?.try_collect().await?;
                let items: Items = fetched
                    .into_iter()
                    .filter_map(|item| Some((item.key, SeqV::from(item.value?))))
                    .collect();
                c.put(read, items.clone(), start, ttl);
                items
            }
        };
        let items = live(items).into_iter().map(|kv| Ok(StreamItem::from(kv)));
        Ok(stream::iter(items).boxed())
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let res = self.inner.transaction(txn).await;
        match &res {
            Ok(reply) => self.invalidate_txn(reply),
            // The transaction may have been applied, and its keys are not known here.
            Err(_) => self.cache.invalidate_all(),
        }
        res
    }
}
