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
use super::prefix_cache::now_ms;

/// A [`KVApi`] that keeps [`MetaReadCache`] consistent with the writes it forwards, and
/// optionally answers reads from it.
///
/// - [`CachedKvApi::fencing`] forwards every call; writes record fences. Use it for everything
///   that may read-modify-write.
/// - [`CachedKvApi::reading`] additionally answers `get`/`list` under cached prefixes from
///   memory when allowed. Use it only for plain reads.
pub struct CachedKvApi {
    inner: Arc<dyn KVApi<Error = MetaError>>,
    cache: Arc<MetaReadCache>,
    serve_reads: bool,
}

impl CachedKvApi {
    pub fn fencing(inner: Arc<dyn KVApi<Error = MetaError>>, cache: Arc<MetaReadCache>) -> Self {
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

    fn record(&self, key: &str, prev: Option<u64>, result: Option<u64>) {
        let Some(p) = self.cache.find(key) else {
            return;
        };
        match (prev, result) {
            (_, Some(seq)) => p.record_put(key, seq),
            (Some(seq), None) => p.record_delete(key, seq),
            (None, None) => {}
        }
    }

    fn record_txn(&self, reply: &TxnReply) {
        for resp in &reply.responses {
            match &resp.response {
                Some(Response::Put(put)) => {
                    let prev = put.prev_value.as_ref().map(|v| v.seq);
                    self.record(&put.key, prev, put.current.as_ref().map(|v| v.seq));
                }
                Some(Response::Delete(del)) if del.success => {
                    self.record(&del.key, del.prev_value.as_ref().map(|v| v.seq), None);
                }
                Some(Response::DeleteByPrefix(del)) => {
                    if let Some(p) = self.cache.find(&del.prefix) {
                        p.resync("deleted by prefix in this process");
                    }
                }
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
        let change = self.inner.upsert_kv(req).await?;
        let seq = |v: &Option<SeqV>| v.as_ref().map(|v| v.seq);
        self.record(&key, seq(&change.prev), seq(&change.result));
        Ok(change)
    }

    async fn get_many_kv(
        &self,
        keys: BoxStream<'static, Result<String, Self::Error>>,
    ) -> Result<KVStream<Self::Error>, Self::Error> {
        if !self.serve_reads {
            return self.inner.get_many_kv(keys).await;
        }
        let keys: Vec<String> = keys.try_collect().await?;
        let now = now_ms();
        // Outer `None`: not answered from memory.
        let mut values: Vec<Option<Option<SeqV>>> = keys
            .iter()
            .map(|k| self.cache.find(k).and_then(|p| p.get(k, now)))
            .collect();
        let missing: Vec<usize> = (0..keys.len()).filter(|&i| values[i].is_none()).collect();
        if !missing.is_empty() {
            let req: Vec<Result<String, MetaError>> =
                missing.iter().map(|&i| Ok(keys[i].clone())).collect();
            let fetched: Vec<StreamItem> = self
                .inner
                .get_many_kv(stream::iter(req).boxed())
                .await?
                .try_collect()
                .await?;
            for (&i, item) in missing.iter().zip(fetched) {
                values[i] = Some(item.value.map(SeqV::from));
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
        if self.serve_reads {
            if let Some(p) = self.cache.find(opts.prefix) {
                if let Some(mut items) = p.list(opts.prefix, now_ms()) {
                    if let Some(limit) = opts.limit {
                        items.truncate(limit as usize);
                    }
                    let items = items.into_iter().map(|kv| Ok(StreamItem::from(kv)));
                    return Ok(stream::iter(items).boxed());
                }
            }
        }
        self.inner.list_kv(opts).await
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let reply = self.inner.transaction(txn).await?;
        self.record_txn(&reply);
        Ok(reply)
    }
}
