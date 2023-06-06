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
use common_meta_kvapi::kvapi;
use common_meta_types::ConditionResult::Eq;
use common_meta_types:: MetaError;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use tracing::debug;
use common_meta_app::app_error::{AppError, BackgroundJobAlreadyExists, UnknownBackgroundJob};
use common_meta_app::background::{BackgroundJobId, BackgroundJobIdent, BackgroundJobInfo, BackgroundTaskId, BackgroundTaskIdent, BackgroundTaskInfo, CreateBackgroundJobReply, CreateBackgroundJobReq, DeleteBackgroundJobReply, DeleteBackgroundJobReq, GetBackgroundJobReply, GetBackgroundJobReq, GetBackgroundTaskReply, GetBackgroundTaskReq, ListBackgroundJobsReq, ListBackgroundTasksReq, UpdateBackgroundJobReply, UpdateBackgroundJobReq, UpdateBackgroundTaskReply, UpdateBackgroundTaskReq};
use common_meta_kvapi::kvapi::Key;
use crate::background_api::BackgroundApi;

use crate::{fetch_id, serialize_u64, txn_op_del, txn_op_put, txn_op_put_with_expire};
use crate::get_pb_value;
use crate::get_u64_value;
use crate::id_generator::IdGenerator;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
use crate::txn_cond_seq;
use crate::util::{deserialize_u64, txn_trials};

/// BackgroundApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls BackgroundApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> BackgroundApi for KV {
    async fn create_background_job(&self, req: CreateBackgroundJobReq) -> Result<CreateBackgroundJobReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundApi: {}", func_name!());

        let name_key = &req.job_name;

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        let id = loop {
            trials.next().unwrap()?;
            // Get db mask by name to ensure absence
            let (seq, id) = get_u64_value(self, name_key).await?;
            debug!(seq, id, ?name_key, "create_background_job");

            if seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateBackgroundJobReply { id })
                } else {
                    Err(KVAppError::AppError(AppError::BackgroundJobAlreadyExists(
                        BackgroundJobAlreadyExists::new(
                            &name_key.name,
                            format!("create background job: {:?}", req.job_name),
                        ),
                    )))
                };
            }

            let id = fetch_id(self, IdGenerator::background_job_id()).await?;
            let id_key = BackgroundJobId { id };

            debug!(
                id = debug(&id_key),
                name_key = debug(&name_key),
                "new backgroundjob id"
            );

            {
                let meta: BackgroundJobInfo = req.job_info.clone().into();
                let condition = vec![txn_cond_seq(name_key, Eq, 0)];
                let if_then = vec![
                    txn_op_put(name_key, serialize_u64(id)?), // name -> background_job_id
                    txn_op_put(&id_key, serialize_struct(&meta)?), // id -> meta
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "create_background_job"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateBackgroundJobReply { id })
    }

    // TODO(zhihanz): needs to drop both background job and related background tasks, also needs to gracefully shutdown running queries
    async fn drop_background_job(&self, req: DeleteBackgroundJobReq) -> Result<DeleteBackgroundJobReply, KVAppError> {
        todo!()
    }

    async fn update_background_job(&self, req: UpdateBackgroundJobReq) -> Result<UpdateBackgroundJobReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundApi: {}", func_name!());
        let name_key = &req.job_name;
        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);

        let reply = loop {
            trials.next().unwrap()?;
            let (seq, id) = get_u64_value(self, name_key).await?;
            debug!(seq, id, ?name_key, "update_background_job");
            let id_key = match seq {
                0 => {
                    let id = fetch_id(self, IdGenerator::background_job_id()).await?;
                    BackgroundJobId { id }
                }
                _ => {
                    BackgroundJobId { id }
                }
            };
            let meta = req.info.clone();
            let condition = vec![txn_cond_seq(name_key, Eq, seq)];
            let if_then = vec![txn_op_put(
                name_key,
                serialize_u64(id_key.id)?,
            ), txn_op_put(
                &id_key,
                serialize_struct(&meta)?,
            )];
            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };
            let (succ, _responses) = send_txn(self, txn_req).await?;
            if succ {
                break Ok(UpdateBackgroundJobReply {
                    id: id_key.id.clone(),
                });
            }

        };
        reply

    }

    async fn get_background_job(&self, req: GetBackgroundJobReq) -> Result<GetBackgroundJobReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundApi: {}", func_name!());

        let name_key = &req.name;

        let (id, job) =
            get_background_job_or_error(self, name_key, format!("get_: {:?}", name_key)).await?;

        Ok(GetBackgroundJobReply { id, info: job })
    }

    async fn list_background_jobs(&self, req: ListBackgroundJobsReq) -> Result<Vec<(u64, BackgroundJobInfo)>, KVAppError> {
        let prefix = format!("{}/{}", BackgroundJobIdent::PREFIX, req.tenant);
        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (_, v) in reply {
            let job_id = deserialize_u64(&v.data)?;
            let r = get_background_job_by_id(self, &BackgroundJobId { id: job_id.0 }).await?;
            // filter none and get the task info
            if let Some(task_info) = r.1 {
                res.push((r.0, task_info));
            }
        }
        Ok(res)
    }

    async fn update_background_task(&self, req: UpdateBackgroundTaskReq) -> Result<UpdateBackgroundTaskReply, KVAppError> {
        debug!(req = debug(&req), "BackgroundApi: {}", func_name!());
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


async fn get_background_job_or_error(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &BackgroundJobIdent,
    msg: impl Display,
) -> Result<(u64, BackgroundJobInfo), KVAppError> {
    let (id_seq, id) = get_u64_value(kv_api, name_key).await?;
    background_job_has_to_exist(id_seq, name_key, &msg)?;
    let id_key = BackgroundJobId { id };

    let (id_seq, job_info) = get_pb_value(kv_api, &id_key).await?;
    background_job_has_to_exist(id_seq, name_key, msg)?;

    Ok((
        id_seq,
        // Safe unwrap(): background_job_seq > 0 implies background_job is not None.
        job_info.unwrap(),
    ))
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownBackgroundJob error
pub fn background_job_has_to_exist(
    seq: u64,
    name_ident: &BackgroundJobIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq, ?name_ident, "background job does not exist");
        Err(KVAppError::AppError(AppError::UnknownBackgroundJob(
            UnknownBackgroundJob::new(&name_ident.name, format!("{}: {:?}", msg, name_ident)),
        )))
    } else {
        Ok(())
    }
}

async fn get_background_job_by_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    id: &BackgroundJobId,
) -> Result<(u64, Option<BackgroundJobInfo>), KVAppError> {
    let (seq, res) = get_pb_value(kv_api, id).await?;
    Ok((
        seq,
        res,
    ))
}

// Returns (id_seq, background_task_info)
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