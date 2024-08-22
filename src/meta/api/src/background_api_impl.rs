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

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::BackgroundJobAlreadyExists;
use databend_common_meta_app::app_error::UnknownBackgroundJob;
use databend_common_meta_app::background::BackgroundJobIdIdent;
use databend_common_meta_app::background::BackgroundJobIdent;
use databend_common_meta_app::background::BackgroundJobInfo;
use databend_common_meta_app::background::BackgroundTaskIdent;
use databend_common_meta_app::background::BackgroundTaskInfo;
use databend_common_meta_app::background::CreateBackgroundJobReply;
use databend_common_meta_app::background::CreateBackgroundJobReq;
use databend_common_meta_app::background::DeleteBackgroundJobReply;
use databend_common_meta_app::background::DeleteBackgroundJobReq;
use databend_common_meta_app::background::GetBackgroundJobReply;
use databend_common_meta_app::background::GetBackgroundJobReq;
use databend_common_meta_app::background::GetBackgroundTaskReply;
use databend_common_meta_app::background::GetBackgroundTaskReq;
use databend_common_meta_app::background::ListBackgroundJobsReq;
use databend_common_meta_app::background::ListBackgroundTasksReq;
use databend_common_meta_app::background::UpdateBackgroundJobParamsReq;
use databend_common_meta_app::background::UpdateBackgroundJobReply;
use databend_common_meta_app::background::UpdateBackgroundJobStatusReq;
use databend_common_meta_app::background::UpdateBackgroundTaskReply;
use databend_common_meta_app::background::UpdateBackgroundTaskReq;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::seq_value::SeqValue;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MatchSeq::Any;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::background_api::BackgroundApi;
use crate::deserialize_struct;
use crate::fetch_id;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_seq;
use crate::txn_op_put;
use crate::util::deserialize_u64;

/// BackgroundApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls BackgroundApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> BackgroundApi for KV {
    #[fastrace::trace]
    async fn create_background_job(
        &self,
        req: CreateBackgroundJobReq,
    ) -> Result<CreateBackgroundJobReply, KVAppError> {
        debug!(req :? =(&req); "BackgroundApi: {}", func_name!());

        let name_key = &req.job_name;

        let mut trials = txn_backoff(None, func_name!());
        let id = loop {
            trials.next().unwrap()?.await;

            // Get db mask by name to ensure absence
            let (seq, id) = get_u64_value(self, name_key).await?;
            debug!(seq = seq, id = id, name_key :? =(name_key); "create_background_job");

            if seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateBackgroundJobReply { id })
                } else {
                    Err(KVAppError::AppError(AppError::BackgroundJobAlreadyExists(
                        BackgroundJobAlreadyExists::new(
                            name_key.name(),
                            format!("create background job: {:?}", req.job_name),
                        ),
                    )))
                };
            }

            let id = fetch_id(self, IdGenerator::background_job_id()).await?;
            let id_key = BackgroundJobIdIdent::new(name_key.tenant(), id);

            debug!(
                id :? =(&id_key),
                name_key :? =(name_key);
                "new backgroundjob id"
            );

            {
                let meta: BackgroundJobInfo = req.job_info.clone();
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
                    name :? =(name_key),
                    id :? =(&id_key),
                    succ = succ;
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
    #[fastrace::trace]
    async fn drop_background_job(
        &self,
        _req: DeleteBackgroundJobReq,
    ) -> Result<DeleteBackgroundJobReply, KVAppError> {
        todo!()
    }

    #[fastrace::trace]
    async fn update_background_job_status(
        &self,
        req: UpdateBackgroundJobStatusReq,
    ) -> Result<UpdateBackgroundJobReply, KVAppError> {
        let name = &req.job_name;

        update_background_job(self, name, |info| {
            if info.job_status.as_ref() == Some(&req.status) {
                return false;
            }
            info.job_status = Some(req.status);
            info.last_updated = Some(Utc::now());
            true
        })
        .await
    }

    #[fastrace::trace]
    async fn update_background_job_params(
        &self,
        req: UpdateBackgroundJobParamsReq,
    ) -> Result<UpdateBackgroundJobReply, KVAppError> {
        let name = &req.job_name;
        update_background_job(self, name, |info| {
            if info.job_params.as_ref() == Some(&req.params) {
                return false;
            }
            info.job_params = Some(req.params);
            info.last_updated = Some(Utc::now());
            true
        })
        .await
    }

    #[fastrace::trace]
    async fn get_background_job(
        &self,
        req: GetBackgroundJobReq,
    ) -> Result<GetBackgroundJobReply, KVAppError> {
        debug!(req :? =(&req); "BackgroundApi: {}", func_name!());

        let name_key = &req.name;

        let (id_ident, seq_joq) =
            get_background_job_or_error(self, name_key, format!("get_: {:?}", name_key)).await?;

        Ok(GetBackgroundJobReply::new(id_ident, seq_joq))
    }

    #[fastrace::trace]
    async fn list_background_jobs(
        &self,
        req: ListBackgroundJobsReq,
    ) -> Result<Vec<(u64, String, BackgroundJobInfo)>, KVAppError> {
        let ident = BackgroundJobIdent::new(&req.tenant, "dummy");
        let prefix = ident.tenant_prefix();

        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (k, v) in reply {
            let ident = BackgroundJobIdent::from_str_key(k.as_str())
                .map_err(|e| MetaError::from(InvalidReply::new("list_background_jobs", &e)))?;

            let job_id = deserialize_u64(&v.data)?;

            let req = BackgroundJobIdIdent::new(&req.tenant, *job_id);
            let seq_info = self.get_pb(&req).await?;

            // filter none and get the task info
            if let Some(sv) = seq_info {
                res.push((sv.seq(), ident.job_name().to_string(), sv.data));
            }
        }
        Ok(res)
    }

    #[fastrace::trace]
    async fn update_background_task(
        &self,
        req: UpdateBackgroundTaskReq,
    ) -> Result<UpdateBackgroundTaskReply, KVAppError> {
        debug!(req :? =(&req); "BackgroundApi: {}", func_name!());
        let name_key = &req.task_name;
        debug!(name_key :? =(name_key); "update_background_task");

        let meta = req.task_info.clone();

        let resp = self
            .upsert_kv(UpsertKVReq::new(
                name_key.to_string_key().as_str(),
                Any,
                Operation::Update(serialize_struct(&meta)?),
                Some(MetaSpec::new_expire(req.expire_at)),
            ))
            .await?;
        // confirm a successful update
        assert!(resp.is_changed());
        Ok(UpdateBackgroundTaskReply {
            last_updated: Utc::now(),
            expire_at: req.expire_at,
        })
    }

    #[fastrace::trace]
    async fn list_background_tasks(
        &self,
        req: ListBackgroundTasksReq,
    ) -> Result<Vec<(u64, String, BackgroundTaskInfo)>, KVAppError> {
        let ident = BackgroundTaskIdent::new(&req.tenant, "dummy");
        let prefix = ident.tenant_prefix();

        let reply = self.prefix_list_kv(&prefix).await?;
        let mut res = vec![];
        for (k, v) in reply {
            let ident = BackgroundTaskIdent::from_str_key(k.as_str()).map_err(|e| {
                KVAppError::MetaError(MetaError::from(InvalidReply::new(
                    "list_background_tasks",
                    &e,
                )))
            })?;
            let val: BackgroundTaskInfo = deserialize_struct(&v.data)?;
            res.push((v.seq, ident.name().to_string(), val));
        }
        Ok(res)
    }

    #[fastrace::trace]
    async fn get_background_task(
        &self,
        req: GetBackgroundTaskReq,
    ) -> Result<GetBackgroundTaskReply, KVAppError> {
        debug!(
            req :? =(&req);
            "BackgroundTaskApi: {}",
            func_name!()
        );

        let resp = self.get_pb(&req.name).await?;
        Ok(GetBackgroundTaskReply {
            task_info: resp.into_value(),
        })
    }
}

async fn get_background_job_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &BackgroundJobIdent,
) -> Result<BackgroundJobIdIdent, KVAppError> {
    let (id_seq, id) = get_u64_value(kv_api, name_key).await?;
    assert_background_job_exist(id_seq, name_key)?;

    Ok(BackgroundJobIdIdent::new(name_key.tenant(), id))
}

async fn get_background_job_or_error(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &BackgroundJobIdent,
    _msg: impl Display,
) -> Result<kvapi::Pair<BackgroundJobIdIdent>, KVAppError> {
    let id_ident = get_background_job_id(kv_api, name_ident).await?;

    let seq_job = kv_api
        .get_pb(&id_ident)
        .await?
        .ok_or_else(|| unknown_background_job(name_ident))?;

    Ok((id_ident, seq_job))
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownBackgroundJob error
pub fn assert_background_job_exist(
    seq: u64,
    name_ident: &BackgroundJobIdent,
) -> Result<(), AppError> {
    if seq == 0 {
        debug!(seq = seq, name_ident :? =(name_ident); "background job does not exist");
        let err = unknown_background_job(name_ident);
        Err(err)
    } else {
        Ok(())
    }
}

pub fn unknown_background_job(name_ident: &BackgroundJobIdent) -> AppError {
    AppError::UnknownBackgroundJob(UnknownBackgroundJob::new(
        name_ident.job_name(),
        format!("{:?}", name_ident),
    ))
}

async fn update_background_job<F: FnOnce(&mut BackgroundJobInfo) -> bool>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name: &BackgroundJobIdent,
    mutation: F,
) -> Result<UpdateBackgroundJobReply, KVAppError> {
    debug!(req :? =(name); "BackgroundApi: {}", func_name!());
    let (id_ident, mut seq_job) =
        get_background_job_or_error(kv_api, name, "update_background_job").await?;

    let should_update = mutation(&mut seq_job.data);
    if !should_update {
        return Ok(UpdateBackgroundJobReply::new(id_ident.clone()));
    }

    let req = UpsertPB::update_exact(id_ident.clone(), seq_job);
    let resp = kv_api.upsert_pb(&req).await?;

    assert!(resp.is_changed());

    Ok(UpdateBackgroundJobReply { id_ident })
}
