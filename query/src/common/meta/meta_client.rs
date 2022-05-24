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

use std::sync::Arc;

use common_exception::Result;
use common_grpc::RpcClientConf;
use common_meta_api::KVApi;
use common_meta_embedded::MetaEmbedded;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::GetKVReply;
use common_meta_types::MGetKVReply;
use common_meta_types::MetaError;
use common_meta_types::PrefixListReply;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVActionReply;
use common_meta_types::UpsertKVReq;
use common_tracing::tracing;

#[derive(Clone)]
pub struct MetaStoreProvider {
    rpc_conf: RpcClientConf,
}

/// MetaStore is impl with either a local embedded meta store, or a grpc-client of metasrv
#[derive(Clone)]
pub enum MetaStore {
    L(Arc<MetaEmbedded>),
    R(Arc<MetaGrpcClient>),
}

impl MetaStore {
    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn is_local(&self) -> bool {
        match self {
            MetaStore::L(_) => true,
            MetaStore::R(_) => false,
        }
    }
}

#[async_trait::async_trait]
impl KVApi for MetaStore {
    async fn upsert_kv(
        &self,
        act: UpsertKVReq,
    ) -> std::result::Result<UpsertKVActionReply, MetaError> {
        match self {
            MetaStore::L(x) => x.upsert_kv(act).await,
            MetaStore::R(x) => x.upsert_kv(act).await,
        }
    }

    async fn get_kv(&self, key: &str) -> std::result::Result<GetKVReply, MetaError> {
        match self {
            MetaStore::L(x) => x.get_kv(key).await,
            MetaStore::R(x) => x.get_kv(key).await,
        }
    }

    async fn mget_kv(&self, key: &[String]) -> std::result::Result<MGetKVReply, MetaError> {
        match self {
            MetaStore::L(x) => x.mget_kv(key).await,
            MetaStore::R(x) => x.mget_kv(key).await,
        }
    }

    async fn prefix_list_kv(
        &self,
        prefix: &str,
    ) -> std::result::Result<PrefixListReply, MetaError> {
        match self {
            MetaStore::L(x) => x.prefix_list_kv(prefix).await,
            MetaStore::R(x) => x.prefix_list_kv(prefix).await,
        }
    }

    async fn transaction(&self, txn: TxnRequest) -> std::result::Result<TxnReply, MetaError> {
        match self {
            MetaStore::L(x) => x.transaction(txn).await,
            MetaStore::R(x) => x.transaction(txn).await,
        }
    }
}

impl MetaStoreProvider {
    pub fn new(rpc_conf: RpcClientConf) -> Self {
        MetaStoreProvider { rpc_conf }
    }

    pub async fn try_get_meta_store(&self) -> Result<MetaStore> {
        if self.rpc_conf.local_mode() {
            tracing::info!(
                conf = debug(&self.rpc_conf),
                "use embedded meta, data will be removed when process exits"
            );

            // NOTE: This can only be used for test: data will be removed when program quit.
            let meta_store = common_meta_embedded::MetaEmbedded::get_meta().await?;
            Ok(MetaStore::L(meta_store))
        } else {
            tracing::info!(conf = debug(&self.rpc_conf), "use remote meta");
            let client = MetaGrpcClient::try_new(&self.rpc_conf).await?;
            Ok(MetaStore::R(client))
        }
    }
}
