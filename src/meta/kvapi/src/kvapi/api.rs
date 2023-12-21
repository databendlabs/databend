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
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use log::debug;

use crate::kvapi;
use crate::kvapi::GetKVReply;
use crate::kvapi::ListKVReply;
use crate::kvapi::MGetKVReply;
use crate::kvapi::UpsertKVReply;
use crate::kvapi::UpsertKVReq;

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
    type Error: std::error::Error + Send + Sync + 'static;

    /// Update or insert a key-value record.
    async fn upsert_kv(&self, req: UpsertKVReq) -> Result<UpsertKVReply, Self::Error>;

    /// Get a key-value record by key.
    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error>;

    /// Get several key-values by keys.
    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error>;

    /// List key-value records that are starts with the specified prefix.
    ///
    /// Same as `prefix_list_kv()`, except it returns a stream.
    async fn list_kv(
        &self,
        prefix: &str,
    ) -> Result<BoxStream<'static, Result<StreamItem, Self::Error>>, Self::Error>;

    // TODO: deprecate it:
    // #[deprecated(note = "use list_kv() instead")]
    /// List key-value records that are starts with the specified prefix.
    ///
    /// This method has a default implementation by collecting result from `stream_list_kv()`
    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, Self::Error> {
        let now = std::time::Instant::now();
        let strm = self.list_kv(prefix).await?;

        debug!("list_kv() took {:?}", now.elapsed());

        let v = strm
            .map_ok(|x| {
                // Safe unwrap(): list_kv() does not return None value
                (x.key, SeqV::from(x.value.unwrap()))
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(v)
    }

    /// Run transaction: update one or more records if specified conditions are met.
    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error>;
}

#[async_trait]
impl<U: kvapi::KVApi, T: Deref<Target = U> + Send + Sync> kvapi::KVApi for T {
    type Error = U::Error;

    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        self.deref().upsert_kv(act).await
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        self.deref().get_kv(key).await
    }

    async fn mget_kv(&self, key: &[String]) -> Result<MGetKVReply, Self::Error> {
        self.deref().mget_kv(key).await
    }

    async fn list_kv(&self, prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
        self.deref().list_kv(prefix).await
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, Self::Error> {
        self.deref().prefix_list_kv(prefix).await
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
