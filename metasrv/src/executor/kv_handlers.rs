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
//

use common_exception::ErrorCode;
use common_meta_flight::GetKVAction;
use common_meta_flight::KVMetaAction;
use common_meta_flight::MGetKVAction;
use common_meta_flight::PrefixListReq;
use common_meta_flight::UpsertKVAction;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::GetKVActionReply;
use common_meta_types::LogEntry;
use common_meta_types::MGetKVActionReply;
use common_meta_types::Operation;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVActionReply;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<UpsertKVAction> for ActionHandler {
    async fn handle(&self, act: UpsertKVAction) -> common_exception::Result<UpsertKVActionReply> {
        let cr = LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: act.key,
                seq: act.seq,
                value: act.value.into(),
                value_meta: act.value_meta,
            },
        };
        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            AppliedState::KV(x) => Ok(x),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<KVMetaAction> for ActionHandler {
    async fn handle(&self, act: KVMetaAction) -> common_exception::Result<UpsertKVActionReply> {
        let cr = LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: act.key,
                seq: act.seq,
                value: Operation::AsIs,
                value_meta: act.value_meta,
            },
        };
        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            AppliedState::KV(x) => Ok(x),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetKVAction> for ActionHandler {
    async fn handle(&self, act: GetKVAction) -> common_exception::Result<GetKVActionReply> {
        let result = self.meta_node.get_kv(&act.key).await?;
        Ok(GetKVActionReply { result })
    }
}

#[async_trait::async_trait]
impl RequestHandler<MGetKVAction> for ActionHandler {
    async fn handle(&self, act: MGetKVAction) -> common_exception::Result<MGetKVActionReply> {
        let result = self.meta_node.mget_kv(&act.keys).await?;
        Ok(MGetKVActionReply { result })
    }
}

#[async_trait::async_trait]
impl RequestHandler<PrefixListReq> for ActionHandler {
    async fn handle(&self, act: PrefixListReq) -> common_exception::Result<PrefixListReply> {
        let result = self.meta_node.prefix_list_kv(&(act.0)).await?;
        Ok(result)
    }
}
