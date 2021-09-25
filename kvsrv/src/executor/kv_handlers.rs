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
use common_kv_api_vo::GetKVActionResult;
use common_kv_api_vo::MGetKVActionResult;
use common_kv_api_vo::PrefixListReply;
use common_kv_api_vo::UpsertKVActionResult;
use common_metatypes::Cmd;
use common_metatypes::LogEntry;
use common_metatypes::Operation;
use common_raft_store::state_machine::AppliedState;
use common_store_api_sdk::kv_api_impl::GetKVAction;
use common_store_api_sdk::kv_api_impl::KVMetaAction;
use common_store_api_sdk::kv_api_impl::MGetKVAction;
use common_store_api_sdk::kv_api_impl::PrefixListReq;
use common_store_api_sdk::kv_api_impl::UpsertKVAction;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<UpsertKVAction> for ActionHandler {
    async fn handle(&self, act: UpsertKVAction) -> common_exception::Result<UpsertKVActionResult> {
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
            AppliedState::KV { prev, result } => Ok(UpsertKVActionResult { prev, result }),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<KVMetaAction> for ActionHandler {
    async fn handle(&self, act: KVMetaAction) -> common_exception::Result<UpsertKVActionResult> {
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
            AppliedState::KV { prev, result } => Ok(UpsertKVActionResult { prev, result }),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetKVAction> for ActionHandler {
    async fn handle(&self, act: GetKVAction) -> common_exception::Result<GetKVActionResult> {
        let result = self.meta_node.get_kv(&act.key).await?;
        Ok(GetKVActionResult { result })
    }
}

#[async_trait::async_trait]
impl RequestHandler<MGetKVAction> for ActionHandler {
    async fn handle(&self, act: MGetKVAction) -> common_exception::Result<MGetKVActionResult> {
        let result = self.meta_node.mget_kv(&act.keys).await?;
        Ok(MGetKVActionResult { result })
    }
}

#[async_trait::async_trait]
impl RequestHandler<PrefixListReq> for ActionHandler {
    async fn handle(&self, act: PrefixListReq) -> common_exception::Result<PrefixListReply> {
        let result = self.meta_node.prefix_list_kv(&(act.0)).await?;
        Ok(result)
    }
}
