// Copyright 2021 Datafuse Labs.
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

use common_meta_api::KVApi;
use common_meta_types::GetKVReply;
use common_meta_types::GetKVReq;
use common_meta_types::ListKVReply;
use common_meta_types::ListKVReq;
use common_meta_types::MGetKVReply;
use common_meta_types::MGetKVReq;
use common_meta_types::MetaError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReply;
use common_meta_types::UpsertKVReq;

use crate::ClientHandle;
use crate::MetaGrpcClient;

#[tonic::async_trait]
impl KVApi for MetaGrpcClient {
    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, MetaError> {
        let reply = self.do_write(act).await?;
        Ok(reply)
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, MetaError> {
        let reply = self
            .do_read(GetKVReq {
                key: key.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, MetaError> {
        let keys = keys.to_vec();
        let reply = self.do_read(MGetKVReq { keys }).await?;
        Ok(reply)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, MetaError> {
        let reply = self
            .do_read(ListKVReq {
                prefix: prefix.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError> {
        let reply = self.transaction(txn).await?;
        Ok(reply)
    }
}

#[tonic::async_trait]
impl KVApi for ClientHandle {
    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, MetaError> {
        let reply = self.request(act).await?;
        Ok(reply)
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, MetaError> {
        let reply = self
            .request(GetKVReq {
                key: key.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, MetaError> {
        let keys = keys.to_vec();
        let reply = self.request(MGetKVReq { keys }).await?;
        Ok(reply)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, MetaError> {
        let reply = self
            .request(ListKVReq {
                prefix: prefix.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError> {
        let reply = self.request(txn).await?;
        Ok(reply)
    }
}
