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
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::GetKVActionReply;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MetaError;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;

use crate::state_machine::StateMachine;

#[async_trait::async_trait]
impl KVApi for StateMachine {
    async fn upsert_kv(&self, act: UpsertKVAction) -> Result<UpsertKVActionReply, MetaError> {
        let cmd = Cmd::UpsertKV {
            key: act.key,
            seq: act.seq,
            value: act.value,
            value_meta: act.value_meta,
        };

        let res = self.sm_tree.txn(true, |t| {
            let r = self.apply_cmd(&cmd, &t).unwrap();
            Ok(r)
        })?;

        match res {
            AppliedState::KV(x) => Ok(x),
            _ => {
                panic!("expect AppliedState::KV");
            }
        }
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVActionReply, MetaError> {
        // TODO(xp) refine get(): a &str is enough for key
        let sv = self.kvs().get(&key.to_string())?;
        tracing::debug!("get_kv sv:{:?}", sv);
        let sv = match sv {
            None => return Ok(None),
            Some(sv) => sv,
        };

        Ok(Self::unexpired(sv))
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVActionReply, MetaError> {
        let kvs = self.kvs();
        let mut res = vec![];
        for x in keys.iter() {
            let v = kvs.get(x)?;
            let v = Self::unexpired_opt(v);
            res.push(v)
        }

        Ok(res)
    }

    async fn prefix_list_kv(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, SeqV<Vec<u8>>)>, MetaError> {
        let kvs = self.kvs();
        let kv_pairs = kvs.scan_prefix(&prefix.to_string())?;

        let x = kv_pairs.into_iter();

        // Convert expired to None
        let x = x.map(|(k, v)| (k, Self::unexpired(v)));
        // Remove None
        let x = x.filter(|(_k, v)| v.is_some());
        // Extract from an Option
        let x = x.map(|(k, v)| (k, v.unwrap()));

        Ok(x.collect())
    }
}
