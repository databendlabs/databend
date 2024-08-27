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
use std::sync::Arc;

use databend_common_meta_api::deserialize_struct;
use databend_common_meta_api::fetch_id;
use databend_common_meta_api::get_pb_value;
use databend_common_meta_api::get_u64_value;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_api::list_u64_value;
use databend_common_meta_api::procedure_has_to_exist;
use databend_common_meta_api::send_txn;
use databend_common_meta_api::serialize_struct;
use databend_common_meta_api::serialize_u64;
use databend_common_meta_api::txn_backoff::txn_backoff;
use databend_common_meta_api::txn_cond_seq;
use databend_common_meta_api::txn_op_del;
use databend_common_meta_api::txn_op_put;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::ProcedureAlreadyExists;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::principal::procedur_name_ident::ProcedureNameIdentRaw;
use databend_common_meta_app::principal::procedure::ProcedureIdent;
use databend_common_meta_app::principal::procedure::ProcedureInfo;
use databend_common_meta_app::principal::procedure_id_ident::ProcedureIdIdent;
use databend_common_meta_app::principal::CreateProcedureReply;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::principal::DropProcedureReply;
use databend_common_meta_app::principal::DropProcedureReq;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ListProcedureReq;
use databend_common_meta_app::principal::ProcedureId;
use databend_common_meta_app::principal::ProcedureIdList;
use databend_common_meta_app::principal::ProcedureIdToName;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;
use log::info;
use log::warn;

pub struct ProcedureMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl ProcedureMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        ProcedureMgr {
            kv_api,
            tenant: tenant.clone(),
        }
    }

    /// Add a PROCEDURE to /tenant/procedure-name.
    #[async_backtrace::framed]
    pub async fn create_procedure(
        &self,
        req: CreateProcedureReq,
    ) -> Result<CreateProcedureReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get procedure by name to ensure absence
            let (procedure_id_seq, procedure_id) =
                get_u64_value(self.kv_api.as_ref(), name_key).await?;
            debug!(procedure_id_seq = procedure_id_seq, procedure_id = procedure_id, name_key :? =(name_key); "get_procedure");

            let mut condition = vec![];
            let mut if_then = vec![];

            if procedure_id_seq > 0 {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(KVAppError::AppError(AppError::ProcedureAlreadyExists(
                            ProcedureAlreadyExists::new(
                                name_key.procedure_name(),
                                format!("create procedure: tenant: {}", self.tenant.tenant_name()),
                            ),
                        )));
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(CreateProcedureReply { procedure_id });
                    }
                    CreateOption::CreateOrReplace => {
                        // TODO
                        // let (_, share_specs) = drop_procedure_meta(
                        //     self,
                        //     name_key,
                        //     false,
                        //     false,
                        //     &mut condition,
                        //     &mut if_then,
                        // )
                        //     .await?;
                        // share_specs
                    }
                }
            }

            // get procedure id list from _fd_procedure_id_list/procedure_id
            let procedureid_idlist =
                ProcedureIdIdent::new(name_key.tenant(), name_key.procedure_name());
            let (procedure_id_list_seq, procedure_id_list_opt): (_, Option<ProcedureIdList>) =
                get_pb_value(self.kv_api.as_ref(), &procedureid_idlist).await?;

            let mut procedure_id_list = if procedure_id_list_seq == 0 {
                ProcedureIdList::new()
            } else {
                procedure_id_list_opt.unwrap_or(ProcedureIdList::new())
            };

            // Create procedure by inserting these record:
            // (tenant, procedure_name) -> procedure_id
            // (procedure_id) -> procedure_meta
            // append procedure_id into _fd_procedure_id_list/<tenant>/<procedure_name>
            // (procedure_id) -> (tenant,procedure_name)

            let procedure_id = fetch_id(self.kv_api.as_ref(), IdGenerator::procedure_id()).await?;
            let id_key = ProcedureId { procedure_id };
            let id_to_name_key = ProcedureIdToName { procedure_id };

            debug!(procedure_id = procedure_id, name_key :? =(name_key); "new procedure id");

            {
                // append procedure_id into procedure_id_list
                procedure_id_list.append(procedure_id);

                condition.extend(vec![
                    txn_cond_seq(name_key, Eq, procedure_id_seq),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                    txn_cond_seq(&procedureid_idlist, Eq, procedure_id_list_seq),
                ]);
                if_then.extend(vec![
                    txn_op_put(name_key, serialize_u64(procedure_id)?), // (tenant, procedure_name) -> procedure_id
                    txn_op_put(&id_key, serialize_struct(&req.meta)?), // (procedure_id) -> procedure_meta
                    txn_op_put(&procedureid_idlist, serialize_struct(&procedure_id_list)?), /* _fd_procedure_id_list/<tenant>/<procedure_name> -> procedure_id_list */
                    txn_op_put(&id_to_name_key, serialize_struct(&ProcedureNameIdentRaw::from(name_key))?), /* __fd_procedure_id_to_name/<procedure_id> -> (tenant,procedure_name) */
                ]);

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self.kv_api.as_ref(), txn_req).await?;

                debug!(
                    name :? =(name_key),
                    id :? =(&id_key),
                    succ = succ;
                    "create_procedure"
                );

                if succ {
                    info!(
                        "procedure name: {}, meta: {}",
                        req.name_ident.procedure_name(),
                        &req.meta
                    );
                    return Ok(CreateProcedureReply { procedure_id });
                }
            }
        }
    }

    /// Drop the tenant's PROCEDURE by name, return the dropped one or None if nothing is dropped.
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn drop_procedure(
        &self,
        req: DropProcedureReq,
    ) -> Result<DropProcedureReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_procedure_name = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut condition = vec![];
            let mut if_then = vec![];

            let procedure_id = drop_procedure_meta(
                self.kv_api.as_ref(),
                tenant_procedure_name,
                req.if_exists,
                true,
                &mut condition,
                &mut if_then,
            )
            .await?;
            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self.kv_api.as_ref(), txn_req).await?;

            debug!(
                name :? =(tenant_procedure_name),
                succ = succ;
                "drop_procedure"
            );

            if succ {
                return Ok(DropProcedureReply { procedure_id });
            }
        }
    }

    #[fastrace::trace]
    pub async fn get_procedure(&self, req: GetProcedureReq) -> Result<ProcedureInfo, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.inner;

        let (_, procedure_id, procedure_meta_seq, procedure_meta) =
            get_procedure_or_err(self.kv_api.as_ref(), name_key, "get_procedure").await?;

        let procedure = ProcedureInfo {
            ident: ProcedureIdent {
                procedure_id,
                seq: procedure_meta_seq,
            },
            name_ident: name_key.clone(),
            meta: procedure_meta,
        };

        Ok(procedure)
    }
    #[fastrace::trace]
    pub async fn list_procedures(
        &self,
        req: ListProcedureReq,
    ) -> Result<Vec<Arc<ProcedureInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // Using a empty procedure to to list all
        let name_key = ProcedureNameIdent::new(req.tenant(), "");

        // Pairs of procedure-name and procedure_id with seq
        let (tenant_procedure_names, procedure_ids) =
            list_u64_value(self.kv_api.as_ref(), &name_key).await?;

        // Keys for fetching serialized ProcedureMeta from kvapi::KVApi
        let mut kv_keys = Vec::with_capacity(procedure_ids.len());

        for procedure_id in procedure_ids.iter() {
            let k = ProcedureId {
                procedure_id: *procedure_id,
            }
            .to_string_key();
            kv_keys.push(k);
        }

        // Batch get all procedure-metas.
        // - A procedure-meta may be already deleted. It is Ok. Just ignore it.
        let seq_metas = self.kv_api.mget_kv(&kv_keys).await?;
        let mut procedure_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let procedure_meta: ProcedureMeta = deserialize_struct(&seq_meta.data)?;

                let procedure_info = ProcedureInfo {
                    ident: ProcedureIdent {
                        procedure_id: procedure_ids[i],
                        seq: seq_meta.seq,
                    },
                    name_ident: ProcedureNameIdent::new(
                        name_key.tenant(),
                        tenant_procedure_names[i].procedure_name(),
                    ),
                    meta: procedure_meta,
                };
                procedure_infos.push(Arc::new(procedure_info));
            } else {
                debug!(
                    k = &kv_keys[i];
                    "procedure_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(procedure_infos)
    }
}

/// Returns (procedure_id_seq, procedure_id, procedure_meta_seq, procedure_meta)
pub(crate) async fn get_procedure_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &ProcedureNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, ProcedureMeta), KVAppError> {
    let (procedure_id_seq, procedure_id) = get_u64_value(kv_api, name_key).await?;
    procedure_has_to_exist(procedure_id_seq, name_key, &msg)?;

    let id_key = ProcedureId { procedure_id };

    let (procedure_meta_seq, procedure_meta) = get_pb_value(kv_api, &id_key).await?;
    procedure_has_to_exist(procedure_meta_seq, name_key, msg)?;

    Ok((
        procedure_id_seq,
        procedure_id,
        procedure_meta_seq,
        // Safe unwrap(): procedure_meta_seq > 0 implies procedure_meta is not None.
        procedure_meta.unwrap(),
    ))
}

async fn drop_procedure_meta(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant_procedurename: &ProcedureNameIdent,
    if_exists: bool,
    drop_name_key: bool,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<u64, KVAppError> {
    let res = get_procedure_or_err(
        kv_api,
        tenant_procedurename,
        format!("drop_procedure: {}", tenant_procedurename.display()),
    )
    .await;

    let (procedure_id_seq, procedure_id, _, _) = match res {
        Ok(x) => x,
        Err(e) => {
            if let KVAppError::AppError(AppError::UnknownProcedure(_)) = e {
                if if_exists {
                    return Ok(0);
                }
            }

            return Err(e);
        }
    };

    // remove procedure_name -> procedure id
    if drop_name_key {
        condition.push(txn_cond_seq(tenant_procedurename, Eq, procedure_id_seq));
        if_then.push(txn_op_del(tenant_procedurename)); // (tenant, procedure_name) -> procedure_id
    }

    {
        // Delete procedure by these operations:
        // del (tenant, procedure_name) -> procedure_id
        debug!(
            procedure_id = procedure_id,
            name_key :? =(tenant_procedurename);
            "drop_procedure"
        );

        // add procedureIdListKey if not exists
        let procedureid_idlist = ProcedureIdIdent::new(
            tenant_procedurename.tenant(),
            tenant_procedurename.procedure_name(),
        );
        let (procedure_id_list_seq, procedure_id_list_opt): (_, Option<ProcedureIdList>) =
            get_pb_value(kv_api, &procedureid_idlist).await?;

        if procedure_id_list_seq == 0 || procedure_id_list_opt.is_none() {
            warn!(
                "drop procedure:{:?}, procedure_id:{:?} has no procedureIdListKey",
                tenant_procedurename, procedure_id
            );

            let mut procedure_id_list = ProcedureIdList::new();
            procedure_id_list.append(procedure_id);

            condition.push(txn_cond_seq(&procedureid_idlist, Eq, procedure_id_list_seq));
            // _fd_procedure_id_list/<tenant>/<procedure_name> -> procedure_id_list
            if_then.push(txn_op_put(
                &procedureid_idlist,
                serialize_struct(&procedure_id_list)?,
            ));
        };
    }

    Ok(procedure_id)
}
