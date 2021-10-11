// Copyright 2020 Datafuse Labs.
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
use common_meta_types::KVMeta;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MatchSeq;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;

use crate::GetKVAction;
use crate::KVMetaAction;
use crate::MGetKVAction;
use crate::MetaFlightClient;
use crate::PrefixListReq;
use crate::UpsertKVAction;

#[async_trait::async_trait]
impl KVApi for MetaFlightClient {
    #[tracing::instrument(level = "debug", skip(self, value))]
    async fn upsert_kv(
        &self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionReply> {
        self.do_action(UpsertKVAction {
            key: key.to_string(),
            seq,
            value,
            value_meta,
        })
        .await
    }

    async fn update_kv_meta(
        &self,
        key: &str,
        seq: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionReply> {
        self.do_action(KVMetaAction {
            key: key.to_string(),
            seq,
            value_meta,
        })
        .await
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
        //keys.iter().map(|k| k.to_string()).collect();
        self.do_action(MGetKVAction { keys }).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        self.do_action(PrefixListReq(prefix.to_string())).await
    }
}
