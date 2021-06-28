// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::ErrorCode;
use common_flights::DeleteKVReply;
use common_flights::DeleteKVReq;
use common_flights::GetKVAction;
use common_flights::MGetKVAction;
use common_flights::MGetKVActionResult;
use common_flights::PrefixListReq;
use common_flights::UpdateByKeyReply;
use common_flights::UpdateKVReq;
use common_flights::UpsertKVAction;
//TODO
// Do not import form common_store_api directly
// we should depends on the comm_flights only, as we are implementing the services requested by
// common_flights, the common_flights component are free to wrap the store_api's parameter / result
use common_store_api::kv_api::PrefixListReply;
use common_store_api::GetKVActionResult;
use common_store_api::UpsertKVActionResult;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;
use crate::meta_service::AppliedState;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;

#[async_trait::async_trait]
impl RequestHandler<UpsertKVAction> for ActionHandler {
    async fn handle(&self, act: UpsertKVAction) -> common_exception::Result<UpsertKVActionResult> {
        let cr = LogEntry {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: act.key,
                seq: act.seq,
                value: act.value,
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
        let result = self.meta_node.get_kv(&act.key).await;
        Ok(GetKVActionResult { result })
    }
}

#[async_trait::async_trait]
impl RequestHandler<MGetKVAction> for ActionHandler {
    async fn handle(&self, act: MGetKVAction) -> common_exception::Result<MGetKVActionResult> {
        let result = self.meta_node.mget_kv(&act.keys).await;
        Ok(MGetKVActionResult { result })
    }
}

#[async_trait::async_trait]
impl RequestHandler<PrefixListReq> for ActionHandler {
    async fn handle(&self, act: PrefixListReq) -> common_exception::Result<PrefixListReply> {
        let result = self.meta_node.prefix_list_kv(&(act.0)).await;
        Ok(result)
    }
}

#[async_trait::async_trait]
impl RequestHandler<DeleteKVReq> for ActionHandler {
    async fn handle(&self, act: DeleteKVReq) -> common_exception::Result<DeleteKVReply> {
        let cr = ClientRequest {
            txid: None,
            cmd: Cmd::DeleteByKeyKV {
                key: act.key,
                seq: act.seq,
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            ClientResponse::KV { prev, result } => Ok(DeleteKVReply { prev, result }),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<UpdateKVReq> for ActionHandler {
    async fn handle(&self, act: UpdateKVReq) -> common_exception::Result<UpdateByKeyReply> {
        let cr = ClientRequest {
            txid: None,
            cmd: Cmd::UpdateByKeyKV {
                key: act.key,
                seq: act.seq,
                value: act.value,
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            ClientResponse::KV { prev, result } => Ok(UpdateByKeyReply { prev, result }),
            _ => Err(ErrorCode::MetaNodeInternalError("not a KV result")),
        }
    }
}
