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

use async_trait::async_trait;
use common_exception::Result;
use common_meta_api::KVApi;
use common_meta_raft_store::state_machine::AppliedState;
pub use common_meta_sled_store::init_temp_sled_db;
use common_meta_types::Cmd;
use common_meta_types::GetKVActionReply;
use common_meta_types::KVMeta;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVActionReply;

use crate::MetaEmbedded;

#[async_trait]
impl KVApi for MetaEmbedded {
    async fn upsert_kv(
        &self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionReply> {
        let cmd = Cmd::UpsertKV {
            key: key.to_string(),
            seq,
            value: value.into(),
            value_meta,
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cmd).await?;

        match res {
            AppliedState::KV(x) => Ok(x),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn update_kv_meta(
        &self,
        key: &str,
        seq: MatchSeq,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionReply> {
        let cmd = Cmd::UpsertKV {
            key: key.to_string(),
            seq,
            value: Operation::AsIs,
            value_meta,
        };

        let mut sm = self.inner.lock().await;
        let res = sm.apply_cmd(&cmd).await?;

        match res {
            AppliedState::KV(x) => Ok(x),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVActionReply> {
        let sm = self.inner.lock().await;
        let res = sm.get_kv(key)?;
        Ok(GetKVActionReply { result: res })
    }

    async fn mget_kv(&self, key: &[String]) -> Result<MGetKVActionReply> {
        let sm = self.inner.lock().await;
        let res = sm.mget_kv(key)?;
        Ok(MGetKVActionReply { result: res })
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<PrefixListReply> {
        let sm = self.inner.lock().await;
        let res = sm.prefix_list_kv(prefix)?;
        Ok(res)
    }
}
