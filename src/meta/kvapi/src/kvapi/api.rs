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

use std::ops::Deref;

use async_trait::async_trait;
use databend_common_meta_types::errors;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::Change;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;

use crate::kvapi;

/// Build an API impl instance or a cluster of API impl
#[async_trait]
pub trait ApiBuilder<T>: Clone {
    /// Create a single node impl
    async fn build(&self) -> T;

    /// Create a cluster of T
    async fn build_cluster(&self) -> Vec<T>;
}

/// A stream of key-value records that are returned by stream based API such as mget and list.
pub type KVStream<E> = BoxStream<'static, Result<StreamItem, E>>;

/// API of a key-value store.
#[async_trait]
pub trait KVApi: Send + Sync {
    /// The Error an implementation returns.
    ///
    /// Depends on the implementation the error could be different.
    /// E.g., a remove kvapi::KVApi impl returns network error or remote storage error.
    /// A local kvapi::KVApi impl just returns storage error.
    type Error: std::error::Error + From<errors::IncompleteStream> + Send + Sync + 'static;

    /// Update or insert a key-value record.
    async fn upsert_kv(&self, req: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error>;

    /// Get single key-value record by key.
    async fn get_kv(&self, key: &str) -> Result<Option<SeqV>, Self::Error> {
        let mut strm = self.get_kv_stream(&[key.to_string()]).await?;

        let strm_item = strm
            .next()
            .await
            .ok_or_else(|| errors::IncompleteStream::new(1, 0).context(" while get_kv"))??;

        let reply = strm_item.value.map(SeqV::from);

        Ok(reply)
    }

    /// Get several key-values by keys.
    async fn mget_kv(&self, keys: &[String]) -> Result<Vec<Option<SeqV>>, Self::Error> {
        let n = keys.len();
        let strm = self.get_kv_stream(keys).await?;

        let seq_values: Vec<Option<SeqV>> = strm
            .map_ok(|item| item.value.map(SeqV::from))
            .try_collect()
            .await?;

        if seq_values.len() != n {
            return Err(
                errors::IncompleteStream::new(n as u64, seq_values.len() as u64)
                    .context(" while mget_kv")
                    .into(),
            );
        }

        debug!("mget: keys: {:?}; values: {:?}", keys, seq_values);

        Ok(seq_values)
    }

    /// Get key-values by keys.
    ///
    /// 2024-01-06: since: 1.2.287
    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error>;

    /// List key-value records that are starts with the specified prefix.
    ///
    /// Same as `prefix_list_kv()`, except it returns a stream.
    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error>;

    /// List key-value starting with the specified prefix and return a [`Vec`]
    ///
    /// Same as [`Self::list_kv`] but return a [`Vec`] instead of a stream.
    async fn list_kv_collect(&self, prefix: &str) -> Result<Vec<(String, SeqV)>, Self::Error> {
        let now = std::time::Instant::now();

        let strm = self.list_kv(prefix).await?;

        debug!("list_kv() took {:?}", now.elapsed());

        let key_seqv_list = strm
            .map_ok(|stream_item| {
                // Safe unwrap(): list_kv() does not return None value
                (stream_item.key, SeqV::from(stream_item.value.unwrap()))
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(key_seqv_list)
    }

    /// Run transaction: update one or more records if specified conditions are met.
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error>;
}

#[async_trait]
impl<U: kvapi::KVApi, T: Deref<Target = U> + Send + Sync> kvapi::KVApi for T {
    type Error = U::Error;

    async fn upsert_kv(&self, act: UpsertKV) -> Result<Change<Vec<u8>>, Self::Error> {
        self.deref().upsert_kv(act).await
    }

    async fn get_kv_stream(&self, keys: &[String]) -> Result<KVStream<Self::Error>, Self::Error> {
        self.deref().get_kv_stream(keys).await
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        self.deref().list_kv(prefix).await
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        self.deref().transaction(txn).await
    }
}

pub trait AsKVApi {
    type Error: std::error::Error;

    fn as_kv_api(&self) -> &dyn kvapi::KVApi<Error = Self::Error>;
}

impl<T: kvapi::KVApi> kvapi::AsKVApi for T {
    type Error = T::Error;

    fn as_kv_api(&self) -> &dyn kvapi::KVApi<Error = Self::Error> {
        self
    }
}
