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

use common_meta_kvapi::kvapi;
use common_meta_types::ConditionResult::Eq;
use common_meta_types:: MetaError;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use tracing::debug;
use common_meta_app::background::{BackgroundTaskId, BackgroundTaskIdent, BackgroundTaskInfo, GetBackgroundTaskReply, GetBackgroundTaskReq, ListBackgroundTasksReq, UpdateBackgroundTaskReply, UpdateBackgroundTaskReq};
use common_meta_kvapi::kvapi::Key;
use crate::background_task_api::BackgroundTaskApi;

use crate::{fetch_id, serialize_u64, txn_op_put_with_expire};
use crate::get_pb_value;
use crate::get_u64_value;
use crate::id_generator::IdGenerator;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
use crate::txn_cond_seq;
use crate::util::{deserialize_u64, txn_trials};

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> BackgroundTaskApi for KV {
    async fn update_background_task(&self, req: UpdateBackgroundTaskReq) -> Result<UpdateBackgroundTaskReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundTaskApi: {}", func_name!());
        let name_key = &req.task_name;
        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        let reply = loop {
            trials.next().unwrap()?;
            let (seq, id) = get_u64_value(self, name_key).await?;
            debug!(seq, id, ?name_key, "update_background_task");
            let id_key = match seq {
                0 => {
                    let id = fetch_id(self, IdGenerator::background_task_id()).await?;
                    BackgroundTaskId { id }
                }
                _ => {
                    BackgroundTaskId { id }
                }
            };
            let meta = req.task_info.clone();
            let condition = vec![txn_cond_seq(name_key, Eq, seq)];
            let if_then = vec![txn_op_put_with_expire(
                name_key,
                serialize_u64(id_key.id)?,
                req.expire_at,
            ), txn_op_put_with_expire(
                &id_key,
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
                    id: id_key.id.clone(),
                    last_updated: meta.last_updated.unwrap_or(Default::default()),
                    expire_at: req.expire_at,
                });
            }

        };
        reply
    }

    async fn list_background_tasks(&self, req: ListBackgroundTasksReq) -> Result<Vec<(u64, BackgroundTaskInfo)>, KVAppError> {
        let prefix = format!("{}/{}", BackgroundTaskIdent::PREFIX, req.tenant);
        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (_, v) in reply {
            let task_id = deserialize_u64(&v.data)?;
            let r = get_background_task_by_id(self, &BackgroundTaskId { id: task_id.0 }).await?;
            // filter none and get the task info
            if let Some(task_info) = r.1 {
                res.push((r.0, task_info));
            }
        }
        Ok(res)

    }

    async fn get_background_task(&self, req: GetBackgroundTaskReq) -> Result<GetBackgroundTaskReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundTaskApi: {}", func_name!());
        let name = &req.name;
        let (_, resp) = get_background_task_or_none(self, name).await?;
        Ok(GetBackgroundTaskReply {
            task_info: resp,
        })
    }
}

// Returns (id_seq, id, data_mask_seq, data_mask)
async fn get_background_task_or_none(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &BackgroundTaskIdent,
) -> Result<(u64, Option<BackgroundTaskInfo>), KVAppError> {
    let (id_seq, id) = get_u64_value(kv_api, name_key).await?;
    let id_key = match id_seq {
        0 => return Ok((0, None)),
        _ => BackgroundTaskId { id },
    };
    get_background_task_by_id(kv_api, &id_key).await
}

async fn get_background_task_by_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    id: &BackgroundTaskId,
) -> Result<(u64, Option<BackgroundTaskInfo>), KVAppError> {
    let (seq, res) = get_pb_value(kv_api, id).await?;
    Ok((
        seq,
        res,
    ))
}