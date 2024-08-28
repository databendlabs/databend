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

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::background::background_job_id_ident::BackgroundJobId;
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
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MatchSeq::Any;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::Operation;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::background_api::BackgroundApi;
use crate::deserialize_struct;
use crate::fetch_id;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::send_txn;
use crate::serialize_struct;
use crate::txn_backoff::txn_backoff;
use crate::util::deserialize_u64;
use crate::util::txn_op_put_pb;
use crate::util::txn_replace_exact;

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
            let job_id = self.get_pb(name_key).await?;

            debug!(res :? = job_id, name_key :? =(name_key); "get existing, when create_background_job");

            if let Some(seq_id) = job_id {
                return if req.if_not_exists {
                    Ok(CreateBackgroundJobReply { id: *seq_id.data })
                } else {
                    Err(
                        AppError::BackgroundJobAlreadyExists(name_key.exist_error(func_name!()))
                            .into(),
                    )
                };
            };

            let id = fetch_id(self, IdGenerator::background_job_id()).await?;
            let id = BackgroundJobId::new(id);
            let id_key = BackgroundJobIdIdent::new_generic(name_key.tenant(), id);

            debug!(id :? =(&id_key),name_key :? =(name_key); "{}", func_name!());

            {
                let mut txn = TxnRequest::default();

                let meta: BackgroundJobInfo = req.job_info.clone();

                txn_replace_exact(&mut txn, name_key, 0, &id)?; // name -> background_job_id
                txn.if_then.push(txn_op_put_pb(&id_key, &meta)?); // id -> meta

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(name :? =(name_key),id :? =(&id_key),succ = succ;"{}", func_name!());

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateBackgroundJobReply { id: *id })
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

        let (seq_id, seq_job) = self
            .get_id_and_value(name_key)
            .await?
            .ok_or_else(|| AppError::from(name_key.unknown_error("get_background_job")))?;

        Ok(GetBackgroundJobReply::new(
            seq_id.data.into_t_ident(name_key.tenant()),
            seq_job,
        ))
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

async fn update_background_job<F: FnOnce(&mut BackgroundJobInfo) -> bool>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &BackgroundJobIdent,
    mutation: F,
) -> Result<UpdateBackgroundJobReply, KVAppError> {
    debug!(req :? =(name_ident); "BackgroundApi: {}", func_name!());

    let (seq_id, mut seq_meta) = kv_api
        .get_id_and_value(name_ident)
        .await?
        .ok_or_else(|| AppError::from(name_ident.unknown_error("update_background_job")))?;

    let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

    let should_update = mutation(&mut seq_meta.data);
    if !should_update {
        return Ok(UpdateBackgroundJobReply::new(id_ident.clone()));
    }

    let req = UpsertPB::update_exact(id_ident.clone(), seq_meta);
    let resp = kv_api.upsert_pb(&req).await?;

    assert!(resp.is_changed());

    Ok(UpdateBackgroundJobReply { id_ident })
}
