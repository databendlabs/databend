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

use databend_common_exception::ErrorCode;
use databend_common_meta_app::KeyExistsBuilder;
use databend_common_meta_app::KeyUnknownBuilder;
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
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use fastrace::func_name;
use log::debug;

use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::name_id_value_api::CreateIdValueMode;
use crate::name_id_value_api::CreateIdValueResult;
use crate::name_id_value_api::NameIdValueApi;
use crate::txn::meta_txn;

pub type DictionaryMoveKeyError = meta_txn::MoveKeyError<
    MetaError,
    UnknownError<DictionaryNameRsc, DictionaryIdentity>,
    ExistError<DictionaryNameRsc, DictionaryIdentity>,
>;

impl From<DictionaryMoveKeyError> for KVAppError {
    fn from(error: DictionaryMoveKeyError) -> Self {
        match error {
            meta_txn::MoveKeyError::KvApi(error) => KVAppError::MetaError(error),
            meta_txn::MoveKeyError::TxnRetryMaxTimes(error) => {
                KVAppError::AppError(AppError::from(error))
            }
            meta_txn::MoveKeyError::Unknown(error) => KVAppError::AppError(AppError::from(error)),
            meta_txn::MoveKeyError::Exists(error) => KVAppError::AppError(AppError::from(error)),
        }
    }
}

impl From<DictionaryMoveKeyError> for ErrorCode {
    fn from(error: DictionaryMoveKeyError) -> Self {
        KVAppError::from(error).into()
    }
}

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
                CreateIdValueMode::CreateOnly,
                |_| vec![],
                |_, _| Ok(vec![]),
                |_, _| {},
            )
            .await?;

        match create_res {
            CreateIdValueResult::Created(id) => Ok(CreateDictionaryReply { dictionary_id: *id }),
            CreateIdValueResult::Existing(_existent) => {
                Err(AppError::from(name_ident.exist_error(func_name!())).into())
            }
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

        let got = self.get_id_and_value(&name_ident).await?;
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
    async fn rename_dictionary(
        &self,
        req: RenameDictionaryReq,
    ) -> Result<(), DictionaryMoveKeyError> {
        debug!(req :? =(&req); "DictionaryApi: {}", func_name!());

        let ctx = func_name!();
        let new_name_ident = DictionaryNameIdent::new(req.tenant(), req.new_dict_ident.clone());
        meta_txn::MetaTxnManager::new(self, ctx)
            .move_key(req.name_ident, new_name_ident)
            .await
    }
}

#[async_trait::async_trait]
impl<KV> DictionaryApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
