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

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_meta_api::KVApi;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::GetKVActionReply;
use common_meta_types::LogEntry;
use common_meta_types::MGetKVActionReply;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;
use common_tracing::tracing;

use crate::meta_service::MetaNode;

/// Impl KVApi for MetaNode.
///
/// Write through raft-log.
/// Read through local state machine, which may not be consistent.
/// E.g. Read is not guaranteed to see a write.
#[async_trait]
impl KVApi for MetaNode {
    async fn upsert_kv(
        &self,
        act: UpsertKVAction,
    ) -> common_exception::Result<UpsertKVActionReply> {
        let ent = LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: act.key,
                seq: act.seq,
                value: act.value,
                value_meta: act.value_meta,
            },
        };
        let rst = self
            .write(ent)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            AppliedState::KV(x) => Ok(x),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_kv(&self, key: &str) -> common_exception::Result<GetKVActionReply> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_kv(key).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn mget_kv(&self, keys: &[String]) -> common_exception::Result<MGetKVActionReply> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.mget_kv(keys).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn prefix_list_kv(&self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.prefix_list_kv(prefix).await
    }
}
