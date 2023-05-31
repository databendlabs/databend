// Copyright 2021 Datafuse Labs
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

use std::fmt::Display;

use common_meta_app::app_error::AppError;
use common_meta_app::app_error::DatamaskAlreadyExists;
use common_meta_app::app_error::UnknownDatamask;
use common_meta_app::data_mask::CreateDatamaskReply;
use common_meta_app::data_mask::CreateDatamaskReq;
use common_meta_app::data_mask::DatamaskId;
use common_meta_app::data_mask::DatamaskMeta;
use common_meta_app::data_mask::DatamaskNameIdent;
use common_meta_app::data_mask::DropDatamaskReply;
use common_meta_app::data_mask::DropDatamaskReq;
use common_meta_app::data_mask::GetDatamaskReply;
use common_meta_app::data_mask::GetDatamaskReq;
use common_meta_kvapi::kvapi;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::{InvalidReply, MetaError, MetaNetworkError};
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use tracing::debug;
use common_meta_app::background::{BackgroundTaskId, BackgroundTaskInfo, GetBackgroundTaskReply, GetBackgroundTaskReq, ListBackgroundTasksReq, UpdateBackgroundTaskReply, UpdateBackgroundTaskReq};
use common_meta_kvapi::kvapi::Key;
use crate::background_task_api::BackgroundTaskApi;

use crate::data_mask_api::DatamaskApi;
use crate::{deserialize_struct, fetch_id, txn_op_put_with_expire};
use crate::get_pb_value;
use crate::get_u64_value;
use crate::id_generator::IdGenerator;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::util::txn_trials;

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> BackgroundTaskApi for KV {
    async fn update_background_task(&self, req: UpdateBackgroundTaskReq) -> Result<UpdateBackgroundTaskReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundTaskApi: {}", func_name!());
        let id = req.task_info.task_id;
        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        let reply = loop {
            trials.next().unwrap()?;
            let (seq, _) = get_pb_value(self, &id).await?;
            debug!(seq, id, "update_background_task");
            let meta = req.task_info.clone();
            let condition = vec![txn_cond_seq(&id, Eq, seq)];
            let if_then = vec![txn_op_put_with_expire(
                &id,
                serialize_struct(&meta)?,
                req.expire_at,
            )];
            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };
            let (succ, _responses) = send_txn(self, txn_req).await?;
            if succ {
                break Ok(UpdateBackgroundTaskReply {
                    task_id: id,
                    last_updated: meta.last_updated.unwrap_or(Default::default()),
                    expire_at: req.expire_at,
                });
            }

        };
        reply
    }

    async fn list_background_tasks(&self, req: ListBackgroundTasksReq) -> Result<Vec<(u64, BackgroundTaskInfo)>, KVAppError> {
        let prefix = format!("{}/{}", BackgroundTaskId::PREFIX, req.tenant);
        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (k, v) in reply {
            let table_id = BackgroundTaskId::from_str_key(&k).map_err(|e| {
                let inv = InvalidReply::new("list_background_tasks", &e);
                let meta_net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(meta_net_err)
            })?;
            let table_meta: BackgroundTaskInfo = deserialize_struct(&vv.data)?;
            res.push((table_id.id, table_meta));
        }
        Ok(res)

    }

    async fn get_background_task(&self, req: GetBackgroundTaskReq) -> Result<GetBackgroundTaskReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundTaskApi: {}", func_name!());
        let id = &req.task_id;
        let (_, resp) = get_background_task_or_none(self, id).await?;
        Ok(GetBackgroundTaskReply {
            task_info: resp,
        })
    }
}

// Returns (id_seq, id, data_mask_seq, data_mask)
async fn get_background_task_or_none(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    task_id: &BackgroundTaskId,
) -> Result<(u64, Option<BackgroundTaskInfo>), KVAppError> {

    let (seq, res) = get_pb_value(kv_api, &task_id).await?;

    Ok((
        seq,
        res,
    ))
}