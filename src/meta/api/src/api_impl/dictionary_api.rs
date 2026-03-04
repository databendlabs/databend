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

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::schema::CreateDictionaryReply;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::RenameDictionaryReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::dictionary_id_ident::DictionaryId;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameRsc;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use super::name_id_value_api::NameIdValueApi;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

/// DictionaryApi defines APIs for dictionary management.
///
/// This trait handles:
/// - Dictionary creation, update, and deletion
/// - Dictionary metadata queries and listing
/// - Dictionary renaming operations
#[async_trait::async_trait]
pub trait DictionaryApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_dictionary(
        &self,
        req: CreateDictionaryReq,
    ) -> Result<CreateDictionaryReply, KVAppError> {
        debug!(req :? = (&req); "DictionaryApi: {}", func_name!());

        let name_ident = &req.dictionary_ident;

        let create_res = self
            .create_id_value(
                name_ident,
                &req.dictionary_meta,
                false,
                |_| vec![],
                |_, _| Ok(vec![]),
                |_, _| {},
            )
            .await?;

        match create_res {
            Ok(id) => Ok(CreateDictionaryReply { dictionary_id: *id }),
            Err(_existent) => Err(AppError::from(name_ident.exist_error(func_name!())).into()),
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn update_dictionary(
        &self,
        req: UpdateDictionaryReq,
    ) -> Result<UpdateDictionaryReply, KVAppError> {
        debug!(req :? = (&req); "DictionaryApi: {}", func_name!());

        let res = self
            .update_id_value(&req.dictionary_ident, req.dictionary_meta)
            .await?;

        if let Some((id, _meta)) = res {
            Ok(UpdateDictionaryReply { dictionary_id: *id })
        } else {
            Err(AppError::from(req.dictionary_ident.unknown_error(func_name!())).into())
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_dictionary(
        &self,
        name_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>, MetaTxnError> {
        debug!(dict_ident :? =(&name_ident); "DictionaryApi: {}", func_name!());

        let removed = self.remove_id_value(&name_ident, |_| vec![]).await?;
        Ok(removed.map(|(_, meta)| meta))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_dictionary(
        &self,
        name_ident: DictionaryNameIdent,
    ) -> Result<Option<(SeqV<DictionaryId>, SeqV<DictionaryMeta>)>, MetaError> {
        debug!(dict_ident :? =(&name_ident); "DictionaryApi: {}", func_name!());

        let got = self.get_id_value(&name_ident).await?;
        Ok(got)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>, KVAppError> {
        debug!(req :? =(&req); "DictionaryApi: {}", func_name!());

        let dictionary_ident = DictionaryNameIdent::new(
            req.tenant.clone(),
            DictionaryIdentity::new(req.db_id, "dummy".to_string()),
        );
        let dir = DirName::new(dictionary_ident);
        let name_id_values = self.list_id_value(&dir).await?;
        Ok(name_id_values
            .map(|(name, _seq_id, seq_meta)| (name.dict_name(), seq_meta.data))
            .collect())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn rename_dictionary(&self, req: RenameDictionaryReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "DictionaryApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let dict_id = self
                .get_pb(&req.name_ident)
                .await?
                .ok_or_else(|| AppError::from(req.name_ident.unknown_error(func_name!())))?;

            let new_name_ident = DictionaryNameIdent::new(req.tenant(), req.new_dict_ident.clone());
            let new_dict_id_seq = self.get_seq(&new_name_ident).await?;
            let _ = dict_has_to_not_exist(new_dict_id_seq, &new_name_ident, "rename_dictionary")
                .map_err(|_| AppError::from(new_name_ident.exist_error(func_name!())))?;

            let condition = vec![
                txn_cond_seq(&req.name_ident, Eq, dict_id.seq),
                txn_cond_seq(&new_name_ident, Eq, 0),
            ];
            let if_then = vec![
                txn_del(&req.name_ident), // del old dict name
                txn_put_pb_with_ttl(&new_name_ident, &dict_id.data, None)?, // put new dict name
            ];

            let txn_req = TxnRequest::new(condition, if_then);

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name :? =(req.name_ident),
                to :? =(&new_name_ident),
                succ = succ;
                "rename_dictionary"
            );

            if succ {
                return Ok(());
            }
        }
    }
}

/// Check dictionary does not exist by checking the seq number.
///
/// seq == 0 means does not exist.
/// seq > 0 means exist.
///
/// If dict does not exist, return Ok(());
/// Otherwise returns DictionaryAlreadyExists error
fn dict_has_to_not_exist(
    seq: u64,
    name_ident: &DictionaryNameIdent,
    _ctx: impl std::fmt::Display,
) -> Result<(), ExistError<DictionaryNameRsc, DictionaryIdentity>> {
    if seq == 0 {
        Ok(())
    } else {
        debug!(seq = seq, name_ident :? =(name_ident); "exist");
        Err(name_ident.exist_error(func_name!()))
    }
}

#[async_trait::async_trait]
impl<KV> DictionaryApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
