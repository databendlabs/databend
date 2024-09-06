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
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::KeyCodec;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::With;
use databend_common_proto_conv::FromToProto;
use fastrace::func_name;
use log::debug;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::meta_txn_error::MetaTxnError;
use crate::send_txn;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::util::txn_op_put_pb;

/// NameValueApi provide generic meta-service access pattern implementations for `name -> value` mapping.
///
/// `K` is the key type for name.
/// `K::ValueType` is the value type.
#[tonic::async_trait]
pub trait NameValueApi<R, N>: KVApi<Error = MetaError>
where
    R: TenantResource + Send + Sync + 'static,
    R::ValueType: FromToProto + Send + Sync + 'static,
    N: KeyCodec,
    N: fmt::Debug + Clone + Send + Sync + 'static,
{
    /// Create a `name -> value` mapping.
    async fn create_name_value(
        &self,
        name_ident: TIdent<R, N>,
        value: R::ValueType,
    ) -> Result<Result<(), ExistError<R, N>>, MetaTxnError> {
        debug!(name_ident :? =name_ident; "NameValueApi: {}", func_name!());

        let upsert = UpsertPB::insert(name_ident.clone(), value);

        let transition = self.upsert_pb(&upsert).await?;

        if !transition.is_changed() {
            return Ok(Err(name_ident.exist_error(func_name!())));
        }
        Ok(Ok(()))
    }

    /// Create a `name -> value` mapping, with `CreateOption` support
    async fn create_name_value_with_create_option(
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

    /// Update an existent `name -> value` mapping.
    ///
    /// The `update` function is called with the previous value
    /// and should output the updated to write back,
    /// with an optional time-to-last value.
    ///
    /// `not_found` is called when the name does not exist.
    /// And this function decide to:
    /// - cancel update by returning `Ok(())`
    /// - or return an error when the name does not exist.
    async fn update_existent_name_value<E>(
        &self,
        name_ident: &TIdent<R, N>,
        update: impl Fn(R::ValueType) -> Option<(R::ValueType, Option<Duration>)> + Send,
        not_found: impl Fn() -> Result<(), E> + Send,
    ) -> Result<Result<(), E>, MetaTxnError> {
        debug!(name_ident :? =name_ident; "NameValueApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let seq_meta = self.get_pb(name_ident).await?;
            let seq = seq_meta.seq();

            let updated = match seq_meta.into_value() {
                Some(x) => update(x),
                None => return Ok(not_found()),
            };

            let Some((updated, ttl)) = updated else {
                // update is cancelled
                return Ok(Ok(()));
            };

            txn.condition.push(txn_cond_eq_seq(name_ident, seq));
            txn.if_then.push(txn_op_put_pb(name_ident, &updated, ttl)?);

            let (succ, _responses) = send_txn(self, txn).await?;

            if succ {
                return Ok(Ok(()));
            }
        }
    }

    /// Remove the `name -> id -> value` mapping by name, along with associated records, such `id->name` reverse index.
    ///
    /// Returns the removed `SeqV<id>` and `SeqV<value>`, if the name exists.
    /// Otherwise, returns None.
    ///
    /// `associated_records` is used to generate additional key-values to remove along with the main operation.
    /// Such operations do not have any condition constraints.
    /// For example, a `name -> id` mapping can have a reverse `id -> name` mapping.
    async fn remove_name_value<E>(
        &self,
        name_ident: &TIdent<R, N>,
        not_found: impl Fn() -> Result<(), E> + Send,
    ) -> Result<Result<(), E>, MetaTxnError> {
        debug!(key :? =name_ident; "NameValueApi: {}", func_name!());

        let upsert = UpsertPB::delete(name_ident.clone());
        let transition = self.upsert_pb(&upsert).await?;
        if !transition.is_changed() {
            return Ok(not_found());
        }

        Ok(Ok(()))
    }
}

impl<R, N, T> NameValueApi<R, N> for T
where
    T: KVApi<Error = MetaError> + ?Sized,
    R: TenantResource + Send + Sync + 'static,
    R::ValueType: FromToProto + Send + Sync + 'static,
    N: KeyCodec,
    N: fmt::Debug + Clone + Send + Sync + 'static,
{
}
