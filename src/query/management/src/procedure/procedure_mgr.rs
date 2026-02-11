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

use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::meta_txn_error::MetaTxnError;
use databend_common_meta_api::name_id_value_api::NameIdValueApi;
use databend_common_meta_api::serialize_struct;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::data_id::DataId;
use databend_common_meta_app::principal::CreateProcedureReply;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::principal::GetProcedureReply;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ListProcedureReq;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::ProcedureId;
use databend_common_meta_app::principal::ProcedureIdToNameIdent;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::principal::procedure::ProcedureInfo;
use databend_common_meta_app::principal::procedure_id_ident;
use databend_common_meta_app::principal::procedure_id_ident::ProcedureIdIdent;
use databend_common_meta_app::principal::procedure_name_ident::ProcedureName;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

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
    /// Returns `ExistError` if the procedure already exists and `overriding` is false.
    #[async_backtrace::framed]
    pub async fn create_procedure(
        &self,
        req: CreateProcedureReq,
        overriding: bool,
    ) -> Result<
        Result<CreateProcedureReply, ExistError<ProcedureName, ProcedureIdentity>>,
        MetaTxnError,
    > {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        let name_ident = &req.name_ident;
        let meta = &req.meta;
        let name_ident_raw = serialize_struct(name_ident.procedure_name())?;

        let tenant = &self.tenant;
        let create_res = self
            .kv_api
            .create_id_value(
                name_ident,
                meta,
                overriding,
                |id| {
                    vec![(
                        ProcedureIdToNameIdent::new_generic(name_ident.tenant(), id)
                            .to_string_key(),
                        name_ident_raw.clone(),
                    )]
                },
                |_, _| Ok(vec![]),
                |old_id: DataId<procedure_id_ident::Resource>, txn: &mut TxnRequest| {
                    // Add ownership key deletion to transaction when overriding
                    let key = TenantOwnershipObjectIdent::new(
                        tenant.clone(),
                        OwnershipObject::Procedure {
                            procedure_id: *old_id,
                        },
                    )
                    .to_string_key();
                    txn.if_then.push(TxnOp::delete(key));
                },
            )
            .await?;

        match create_res {
            Ok(id) => Ok(Ok(CreateProcedureReply { procedure_id: *id })),
            Err(_) => Ok(Err(name_ident.exist_error(func_name!()))),
        }
    }

    /// Drop the tenant's PROCEDURE by name, return the dropped one or None if nothing is dropped.
    #[async_backtrace::framed]
    pub async fn drop_procedure(
        &self,
        name_ident: &ProcedureNameIdent,
    ) -> Result<Option<(SeqV<ProcedureId>, SeqV<ProcedureMeta>)>, MetaTxnError> {
        debug!(name_ident :? =(name_ident); "SchemaApi: {}", func_name!());
        let dropped = self
            .kv_api
            .remove_id_value(name_ident, |id| {
                vec![ProcedureIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key()]
            })
            .await?;
        Ok(dropped)
    }

    #[fastrace::trace]
    pub async fn get_procedure(
        &self,
        req: &GetProcedureReq,
    ) -> Result<Option<GetProcedureReply>, MetaError> {
        debug!(req :? =(req); "SchemaApi: {}", func_name!());

        let res = self.kv_api.get_id_value(&req.inner).await?;

        let Some((seq_id, seq_meta)) = res else {
            return Ok(None);
        };

        Ok(Some(GetProcedureReply {
            id: *seq_id.data,
            procedure_meta: seq_meta.data,
        }))
    }

    #[fastrace::trace]
    pub async fn get_procedure_by_id(
        &self,
        procedure_id: u64,
    ) -> Result<Option<SeqV<ProcedureMeta>>, MetaError> {
        debug!(req :? =(&procedure_id); "SchemaApi: {}", func_name!());

        let id = ProcedureIdIdent::new(&self.tenant, procedure_id);
        let meta = self.kv_api.get_pb(&id).await?;
        Ok(meta)
    }

    #[fastrace::trace]
    pub async fn get_procedure_name_by_id(
        &self,
        procedure_id: u64,
    ) -> Result<Option<String>, MetaError> {
        debug!(req :? =(&procedure_id); "SchemaApi: {}", func_name!());

        let ident = ProcedureIdToNameIdent::new_generic(
            self.tenant.clone(),
            ProcedureId::new(procedure_id),
        );
        let seq_meta = self.kv_api.get_pb(&ident).await?;

        debug!(ident :% =(&ident); "get_procedure_name_by_id");

        let name = seq_meta.map(|s| s.data.to_string());

        Ok(name)
    }

    #[fastrace::trace]
    pub async fn list_procedures(
        &self,
        req: ListProcedureReq,
    ) -> Result<Vec<ProcedureInfo>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // Get procedure id list by `prefix_list` "<prefix>/<tenant>"
        let ident = ProcedureNameIdent::new(&req.tenant, ProcedureIdentity::new("", ""));
        let dir = DirName::new_with_level(ident, 2);

        let name_id_metas = self.kv_api.list_id_value(&dir).await?;

        let procedure_infos = name_id_metas
            .map(|(k, id_seqv, seq_meta)| ProcedureInfo {
                ident: ProcedureIdIdent::new(&req.tenant, *id_seqv.data),
                name_ident: k,
                meta: seq_meta.data,
            })
            .collect::<Vec<_>>();

        Ok(procedure_infos)
    }

    #[fastrace::trace]
    pub async fn list_procedures_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<ProcedureInfo>, MetaError> {
        debug!(name = (name); "SchemaApi: {}", func_name!());
        let ident = ProcedureNameIdent::new(&self.tenant, ProcedureIdentity::new(name, ""));
        let dir = DirName::new_with_level(ident, 1);

        let name_id_metas = self.kv_api.list_id_value(&dir).await?;
        let procedure_infos = name_id_metas
            .map(|(k, id_seqv, seq_meta)| ProcedureInfo {
                ident: ProcedureIdIdent::new(&self.tenant, *id_seqv.data),
                name_ident: k,
                meta: seq_meta.data,
            })
            .collect::<Vec<_>>();

        Ok(procedure_infos)
    }
}
