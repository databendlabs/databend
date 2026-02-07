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

use std::fmt;
use std::time::Duration;

use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::tenant_key::ident::TIdent;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_kvapi::kvapi::KeyCodec;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::With;
use fastrace::func_name;
use log::debug;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::meta_txn_error::MetaTxnError;

/// NameValueApi provide generic meta-service access pattern implementations for `name -> value` mapping.
///
/// `K` is the key type for name.
/// `K::ValueType` is the value type.
#[tonic::async_trait]
pub trait NameValueApi<R, N>: KVApi<Error = MetaError>
where
    R: TenantResource + Send + Sync + 'static,
    R::ValueType: FromToProto + Clone + Send + Sync + 'static,
    N: KeyCodec,
    N: fmt::Debug + Clone + Send + Sync + 'static,
{
    /// Create a `name -> value` mapping.
    async fn insert_name_value(
        &self,
        name_ident: &TIdent<R, N>,
        value: R::ValueType,
        ttl: Option<Duration>,
    ) -> Result<Result<(), ExistError<R, N>>, MetaTxnError> {
        debug!(name_ident :? =name_ident; "NameValueApi: {}", func_name!());

        self.crud_try_insert(name_ident, value, ttl, || {
            Err(name_ident.exist_error(func_name!()))
        })
        .await
    }

    /// Create a `name -> value` mapping, with `CreateOption` support
    async fn insert_name_value_with_create_option(
        &self,
        name_ident: TIdent<R, N>,
        value: R::ValueType,
        create_option: CreateOption,
    ) -> Result<Result<(), ExistError<R, N>>, MetaTxnError> {
        debug!(name_ident :? =name_ident; "NameValueApi: {}", func_name!());

        let match_seq: MatchSeq = create_option.into();
        let upsert = UpsertPB::insert(name_ident.clone(), value).with(match_seq);

        let transition = self.upsert_pb(&upsert).await?;

        #[allow(clippy::collapsible_if)]
        if !transition.is_changed() {
            if create_option == CreateOption::Create {
                return Ok(Err(name_ident.exist_error(func_name!())));
            }
        }
        Ok(Ok(()))
    }
}

impl<R, N, T> NameValueApi<R, N> for T
where
    T: KVApi<Error = MetaError> + ?Sized,
    R: TenantResource + Send + Sync + 'static,
    R::ValueType: FromToProto + Clone + Send + Sync + 'static,
    N: KeyCodec,
    N: fmt::Debug + Clone + Send + Sync + 'static,
{
}
