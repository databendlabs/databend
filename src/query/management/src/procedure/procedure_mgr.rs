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

use std::sync::Arc;

use databend_common_meta_api::fetch_id;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::send_txn;
use databend_common_meta_api::txn_backoff::txn_backoff;
use databend_common_meta_api::txn_op_del;
use databend_common_meta_api::util::txn_delete_exact;
use databend_common_meta_api::util::txn_op_put_pb;
use databend_common_meta_api::util::txn_replace_exact;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::principal::procedure::ProcedureInfo;
use databend_common_meta_app::principal::procedure_id_ident::ProcedureId;
use databend_common_meta_app::principal::procedure_id_ident::ProcedureIdIdent;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::principal::DropProcedureReply;
use databend_common_meta_app::principal::DropProcedureReq;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ListProcedureReq;
use databend_common_meta_app::principal::ProcedureIdToNameIdent;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

pub struct ProcedureMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
}

impl ProcedureMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>) -> Self {
        ProcedureMgr { kv_api }
    }

    /// Add a PROCEDURE to /tenant/procedure-name.
    #[async_backtrace::framed]
    pub async fn create_procedure(
        &self,
        req: CreateProcedureReq,
    ) -> Result<Result<ProcedureId, SeqV<ProcedureId>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        let name_ident = req.name_ident;
        let meta = req.meta;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = self.kv_api.get_id_and_value(&name_ident).await?;
            debug!(res :? = res,name_ident :? =(name_ident);"get_procedure");

            if let Some((seq_id, _seq_meta)) = res {
                return Ok(Err(seq_id));
            }

            // Create procedure by inserting these record:
            // (tenant, procedure_name) -> procedure_id
            // (procedure_id) -> procedure_meta
            // (procedure_id) -> (tenant, procedure_name)
            let id = fetch_id(self.kv_api.as_ref(), IdGenerator::procedure_id()).await?;
            let procedure_id = ProcedureId::new(id);
            let id_ident = ProcedureIdIdent::new_generic(name_ident.tenant(), procedure_id);
            let id_to_name_ident = ProcedureIdToNameIdent::new_from(id_ident.clone());

            debug!(procedure_id :? = procedure_id, name_ident :? =(name_ident); "new procedure id");

            let mut txn = TxnRequest::default();

            txn_replace_exact(&mut txn, &name_ident, 0, &procedure_id)?; /* (tenant, procedure_name) -> procedure_id */
            txn.if_then.extend([
                txn_op_put_pb(&id_ident, &meta)?, /* (procedure_id) -> procedure_meta */
                txn_op_put_pb(&id_to_name_ident, &name_ident.to_raw())?, /* __fd_procedure_id_to_name/<procedure_id> -> (tenant,procedure_name) */

            ]);

            let (succ, _) = send_txn(self.kv_api.as_ref(), txn).await?;
            debug!(name :? =(name_ident),id :? =(&id_ident),succ = succ;"create_procedure");

            if succ {
                return Ok(Ok(procedure_id));
            }
        }
    }

    /// Drop the tenant's PROCEDURE by name, return the dropped one or None if nothing is dropped.
    #[async_backtrace::framed]
    pub async fn drop_procedure(
        &self,
        req: DropProcedureReq,
    ) -> Result<Option<DropProcedureReply>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_ident = req.name_ident;
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = self.kv_api.get_id_and_value(&name_ident).await?;

            let Some((seq_id, seq_meta)) = res else {
                return Ok(None);
            };

            // Delete procedure by deleting these record:
            // (tenant, procedure_name) -> procedure_id
            // (procedure_id) -> procedure_meta
            // (procedure_id) -> (tenant, procedure_name)
            let id_ident = seq_id.data.into_t_ident(name_ident.tenant());
            let id_to_name_ident = ProcedureIdToNameIdent::new_from(id_ident.clone());

            debug!(seq_id :? = seq_id, name_ident :? =(&name_ident); "{}", func_name!());

            let mut txn = TxnRequest::default();

            txn_delete_exact(&mut txn, &name_ident, seq_id.seq); // (tenant, procedure_name) -> procedure_id
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq); // (procedure_id) -> procedure_meta
            txn.if_then.push(txn_op_del(&id_to_name_ident)); /* __fd_procedure_id_to_name/<procedure_id> -> (tenant,procedure_name) */

            let (succ, _) = send_txn(self.kv_api.as_ref(), txn).await?;

            debug!(name_ident :? =(&name_ident),id :? =(&id_ident),succ = succ; "{}", func_name!());

            if succ {
                return Ok(Some(DropProcedureReply {
                    procedure_id: seq_id.seq,
                }));
            }
        }
    }

    #[fastrace::trace]
    pub async fn get_procedure(&self, req: GetProcedureReq) -> Result<ProcedureInfo, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_ident = req.inner;
        let (seq_id, seq_meta) = self
            .kv_api
            .get_id_and_value(&name_ident)
            .await?
            .ok_or_else(|| AppError::unknown(&name_ident, func_name!()))?;

        let procedure = ProcedureInfo {
            ident: ProcedureIdIdent::new_generic(name_ident.tenant(), seq_id.data),
            name_ident: name_ident.clone(),
            meta: seq_meta.data,
        };

        Ok(procedure)
    }
    #[fastrace::trace]
    pub async fn list_procedures(
        &self,
        req: ListProcedureReq,
    ) -> Result<Vec<Arc<ProcedureInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant = req.tenant;
        let name_key = ProcedureNameIdent::new(&tenant, "dummy");

        let dir = DirName::new(name_key);

        // Pairs of procedure-name and procedure_id with seq
        let items = self
            .kv_api
            .list_pb(&dir)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        // Keys for fetching ProcedureMeta from kvapi::KVApi
        let kv_keys = items.iter().map(|x| x.seqv.data.into_t_ident(&tenant));
        let kv_keys = kv_keys.collect::<Vec<_>>();

        // Batch get all procedure-metas.
        // - A procedure-meta may be already deleted. It is Ok. Just ignore it.
        #[allow(deprecated)]
        let seq_metas = self.kv_api.get_pb_values(kv_keys.clone()).await?;
        let seq_metas = seq_metas.try_collect::<Vec<_>>().await?;

        if seq_metas.len() != kv_keys.len() {
            let err = InvalidReply::new(
                "list_procedures",
                &AnyError::error(format!(
                    "mismatched procedure-meta count: got: {}, expect: {}",
                    seq_metas.len(),
                    kv_keys.len()
                )),
            );
            let meta_net_err = MetaNetworkError::from(err);
            return Err(KVAppError::MetaError(meta_net_err.into()));
        }

        let mut procedure_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.into_iter().enumerate() {
            let item = &items[i];

            if let Some(seq_meta) = seq_meta_opt {
                let procedure_info = ProcedureInfo {
                    ident: ProcedureIdIdent::new(&tenant, *item.seqv.data),
                    name_ident: ProcedureNameIdent::new(&tenant, item.key.name()),
                    meta: seq_meta.data,
                };
                procedure_infos.push(Arc::new(procedure_info));
            } else {
                debug!(
                    k :% =(&kv_keys[i]);
                    "procedure_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(procedure_infos)
    }
}
