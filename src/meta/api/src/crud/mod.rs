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

//! A generic CRUD interface for meta data operations.

mod errors;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_common_meta_app::tenant_key::ident::TIdent;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::ValueWithName;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::With;
use databend_common_proto_conv::FromToProto;
pub use errors::CrudError;
use futures::TryStreamExt;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;

/// A generic CRUD interface for meta data operations.
///
/// - It provide `add`, `update`, `get`, `remove` and `list` operations.
/// - The key space it operates on is defined by the type [`TIdent<R>`],
///   which contains a `Tenant` and a `Name`.
///
/// One `CrudMgr` instance can only access keys of exactly one [`Tenant`].
///
/// [`TIdent<R>`]: TIdent
pub struct CrudMgr<R, N = String> {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
    _p: PhantomData<(R, N)>,
}

impl<R> CrudMgr<R> {
    /// Create a new `CrudMgr` instance providing CRUD access for a key space defined by `R`: [`TenantResource`].
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        CrudMgr {
            kv_api,
            tenant: tenant.clone(),
            _p: Default::default(),
        }
    }

    /// Create a structured key for the given name.
    fn ident(&self, name: &str) -> TIdent<R> {
        TIdent::<R, String>::new(self.tenant.clone(), name)
    }
}

/// A shortcut
type ValueOf<R> = <TIdent<R> as kvapi::Key>::ValueType;

impl<R> CrudMgr<R>
where
    R: TenantResource + Send + 'static,
    // As a kvapi::Key, the corresponding value contains a name.
    ValueOf<R>: ValueWithName + FromToProto + Clone,
{
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn add(
        &self,
        value: ValueOf<R>,
        create_option: &CreateOption,
    ) -> Result<(), CrudError<ExistError<R>>> {
        let ident = self.ident(value.name());

        let seq = MatchSeq::from(*create_option);
        let upsert = UpsertPB::insert(ident, value.clone()).with(seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;

        if let CreateOption::Create = create_option {
            if res.prev.is_some() {
                return Err(ExistError::new(value.name(), "Exist when add").into());
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn update(
        &self,
        value: ValueOf<R>,
        match_seq: MatchSeq,
    ) -> Result<u64, CrudError<UnknownError<R>>> {
        let ident = self.ident(value.name());
        let upsert = UpsertPB::update(ident, value.clone()).with(match_seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;

        match res.result {
            Some(SeqV { seq, .. }) => Ok(seq),
            None => Err(UnknownError::new(value.name(), "NotFound when update").into()),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn remove(
        &self,
        name: &str,
        seq: MatchSeq,
    ) -> Result<(), CrudError<UnknownError<R>>> {
        let ident = self.ident(name);

        let upsert = UpsertPB::delete(ident).with(seq);

        let res = self.kv_api.upsert_pb(&upsert).await?;
        res.removed_or_else(|e| {
            UnknownError::new(
                name,
                format_args!("NotFound when remove, seq of existing record: {}", e.seq()),
            )
        })?;

        Ok(())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get(
        &self,
        name: &str,
        seq: MatchSeq,
    ) -> Result<SeqV<ValueOf<R>>, CrudError<UnknownError<R>>> {
        let ident = self.ident(name);

        let res = self.kv_api.get_pb(&ident).await?;

        let seq_value = res.ok_or_else(|| UnknownError::new(name, "NotFound when get"))?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(seq_value),
            Err(e) => Err(UnknownError::new(name, format_args!("NotFound when get: {}", e)).into()),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn list(&self) -> Result<Vec<ValueOf<R>>, MetaError> {
        let dir_name = DirName::new(self.ident("dummy"));

        let values = self.kv_api.list_pb_values(&dir_name).await?;
        let values = values.try_collect().await?;

        Ok(values)
    }
}
