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

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::Change;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::SeqValue;
use databend_common_meta_types::With;
use databend_common_proto_conv::FromToProto;
use fastrace::func_name;
use log::debug;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;

/// [`KVPbCrudApi`] provide generic meta-service access pattern implementations for `name -> value` mapping.
///
/// `K` is the key type for name.
/// `K::ValueType` is the value type.
#[tonic::async_trait]
pub trait KVPbCrudApi<K>: KVApi<Error = MetaError>
where
    K: kvapi::Key + Clone + Send + Sync + 'static,
    K::ValueType: FromToProto + Clone + Send + Sync + 'static,
{
    /// Update or insert a `name -> value` mapping.
    ///
    /// The `update` function is called with the previous value and should output the updated to write back.
    /// - Ok(Some(x)): write back `x`.
    /// - Ok(None): cancel the update.
    /// - Err(e): return error.
    ///
    /// This function returns an embedded result,
    /// - the outer result is for underlying kvapi error,
    /// - the inner result is for business logic error.
    async fn crud_upsert_with<E>(
        &self,
        name_ident: &K,
        update: impl Fn(Option<SeqV<K::ValueType>>) -> Result<Option<K::ValueType>, E> + Send,
    ) -> Result<Result<Change<K::ValueType>, E>, MetaTxnError> {
        debug!(name_ident :? =name_ident; "KVPbCrudApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(name_ident).await?;
            let seq = seq_meta.seq();

            let updated = match update(seq_meta.clone()) {
                Ok(Some(x)) => x,
                Ok(None) => return Ok(Ok(Change::new(seq_meta.clone(), seq_meta))),
                Err(err) => return Ok(Err(err)),
            };

            let transition = self
                .upsert_pb(
                    &UpsertPB::insert(name_ident.clone(), updated).with(MatchSeq::Exact(seq)),
                )
                .await?;

            if transition.is_changed() {
                return Ok(Ok(transition));
            }
        }
    }

    /// Remove the `name -> value` mapping by name.
    ///
    /// `not_found` is called when the name does not exist.
    /// And this function decide to:
    /// - cancel update by returning `Ok(())`
    /// - or return an error when the name does not exist.
    async fn crud_remove<E>(
        &self,
        name_ident: &K,
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

impl<K, T> KVPbCrudApi<K> for T
where
    T: KVApi<Error = MetaError> + ?Sized,
    K: kvapi::Key + Clone + Send + Sync + 'static,
    K::ValueType: FromToProto + Clone + Send + Sync + 'static,
{
}
