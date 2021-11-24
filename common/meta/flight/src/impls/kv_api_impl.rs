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

use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_types::GetKVActionReply;
use common_meta_types::MGetKVActionReply;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;

use crate::GetKVAction;
use crate::MGetKVAction;
use crate::MetaFlightClient;
use crate::PrefixListReq;

#[async_trait::async_trait]
impl KVApi for MetaFlightClient {
    #[tracing::instrument(level = "debug", skip(self, act))]
    async fn upsert_kv(
        &self,
        act: UpsertKVAction,
    ) -> common_exception::Result<UpsertKVActionReply> {
        self.do_action(act).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_kv(&self, key: &str) -> Result<GetKVActionReply> {
        self.do_action(GetKVAction {
            key: key.to_string(),
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, keys))]
    async fn mget_kv(&self, keys: &[String]) -> common_exception::Result<MGetKVActionReply> {
        let keys = keys.to_vec();
        self.do_action(MGetKVAction { keys }).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        self.do_action(PrefixListReq(prefix.to_string())).await
    }
}
