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

use std::ops::Deref;

use async_trait::async_trait;
use common_meta_types::DeleteByPrefixReply;
use common_meta_types::DeleteByPrefixRequest;
use common_meta_types::GetKVReply;
use common_meta_types::ListKVReply;
use common_meta_types::MGetKVReply;
use common_meta_types::MetaError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReply;
use common_meta_types::UpsertKVReq;

#[async_trait]
pub trait KVApiBuilder<T>
where T: KVApi
{
    /// Create a KVApi
    async fn build(&self) -> T;

    /// Create a KVApi cluster
    async fn build_cluster(&self) -> Vec<T>;
}

#[async_trait]
pub trait KVApi: Send + Sync {
    async fn upsert_kv(&self, req: UpsertKVReq) -> Result<UpsertKVReply, MetaError>;

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, MetaError>;

    // mockall complains about AsRef... so we use String here
    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, MetaError>;

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, MetaError>;

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError>;

    // remove keys with prefix
    async fn delete_by_prefix(
        &self,
        req: DeleteByPrefixRequest,
    ) -> Result<DeleteByPrefixReply, MetaError>;
}

#[async_trait]
impl<U: KVApi, T: Deref<Target = U> + Send + Sync> KVApi for T {
    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, MetaError> {
        self.deref().upsert_kv(act).await
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, MetaError> {
        self.deref().get_kv(key).await
    }

    async fn mget_kv(&self, key: &[String]) -> Result<MGetKVReply, MetaError> {
        self.deref().mget_kv(key).await
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, MetaError> {
        self.deref().prefix_list_kv(prefix).await
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError> {
        self.deref().transaction(txn).await
    }

    async fn delete_by_prefix(
        &self,
        req: DeleteByPrefixRequest,
    ) -> Result<DeleteByPrefixReply, MetaError> {
        self.deref().delete_by_prefix(req).await
    }
}
