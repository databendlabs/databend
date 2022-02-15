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
use common_meta_types::GetKVActionReply;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MetaError;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;

use crate::grpc_action::GetKVAction;
use crate::grpc_action::MGetKVAction;
use crate::grpc_action::PrefixListReq;
use crate::MetaGrpcClient;

#[tonic::async_trait]
impl KVApi for MetaGrpcClient {
    async fn upsert_kv(&self, act: UpsertKVAction) -> Result<UpsertKVActionReply, MetaError> {
        let reply = self.do_write(act).await?;
        Ok(reply)
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVActionReply, MetaError> {
        let reply = self
            .do_read(GetKVAction {
                key: key.to_string(),
            })
            .await?;
        Ok(reply)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVActionReply, MetaError> {
        let keys = keys.to_vec();
        let reply = self.do_read(MGetKVAction { keys }).await?;
        Ok(reply)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<PrefixListReply, MetaError> {
        let reply = self.do_read(PrefixListReq(prefix.to_string())).await?;
        Ok(reply)
    }
}
