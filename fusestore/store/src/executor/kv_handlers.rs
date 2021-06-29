// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::ErrorCode;
use common_flights::GetKVAction;
use common_flights::UpsertKVAction;
use common_store_api::GetKVActionResult;
use common_store_api::UpsertKVActionResult;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;
use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;

#[async_trait::async_trait]
impl RequestHandler<UpsertKVAction> for ActionHandler {
    async fn handle(&self, act: UpsertKVAction) -> common_exception::Result<UpsertKVActionResult> {
        let cr = ClientRequest {
            txid: None,
            cmd: Cmd::UpsertKV {
                key: act.key,
                seq: act.seq,
                value: act.value,
            },
        };
        // TODO(xp): raftmeta should use ErrorCode instead of anyhow::Error
        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            ClientResponse::KV { prev, result } => Ok(UpsertKVActionResult { prev, result }),
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
