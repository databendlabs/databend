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

use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::GetKVReply;
use common_meta_kvapi::kvapi::GetKVReq;
use common_meta_kvapi::kvapi::ListKVReply;
use common_meta_kvapi::kvapi::ListKVReq;
use common_meta_kvapi::kvapi::MGetKVReply;
use common_meta_kvapi::kvapi::MGetKVReq;
use common_meta_kvapi::kvapi::UpsertKVReply;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::MetaError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;

use crate::ClientHandle;

#[tonic::async_trait]
impl kvapi::KVApi for ClientHandle {
    type Error = MetaError;

    async fn upsert_kv(&self, act: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        let reply = self.request(act).await?;
        Ok(reply)
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        let reply = self
            .request(GetKVReq {
                key: key.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error> {
        let keys = keys.to_vec();
        let reply = self.request(MGetKVReq { keys }).await?;
        Ok(reply)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, Self::Error> {
        let reply = self
            .request(ListKVReq {
                prefix: prefix.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        let reply = self.request(txn).await?;
        Ok(reply)
    }
}
