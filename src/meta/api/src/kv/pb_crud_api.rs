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

use std::time::Duration;

use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_types::Change;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use databend_meta_types::With;
use fastrace::func_name;
use log::debug;
use seq_marked::SeqValue;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

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
    /// Attempts to insert a new key-value pair in it does not exist, without CAS loop.
    ///
    /// See: [`KVPbCrudApi::crud_try_upsert`]
    async fn crud_try_insert<E>(
        &self,
        key: &K,
        value: K::ValueType,
        ttl: Option<Duration>,
        on_exist: impl FnOnce() -> Result<(), E> + Send,
    ) -> Result<Result<(), E>, MetaTxnError> {
        self.crud_try_upsert(key, MatchSeq::Exact(0), value, ttl, on_exist)
            .await
    }

    /// Attempts to insert or update a new key-value pair, without CAS loop.
    ///
    /// # Arguments
    /// * `key` - The identifier for the new entry.
    /// * `value` - The value to be associated with the key.
    /// * `ttl` - Optional time-to-live for the entry.
    /// * `on_exist` - Callback function invoked if the key already exists.
    ///
    /// # Returns
    /// * `Ok(Ok(()))` if the insertion was successful.
    /// * `Ok(Err(E))` if the key already exists and `on_exist` returned an error.
    /// * `Err(MetaTxnError)` for transaction-related or meta-service errors.
    async fn crud_try_upsert<E>(
        &self,
        key: &K,
        match_seq: MatchSeq,
        value: K::ValueType,
        ttl: Option<Duration>,
        on_exist: impl FnOnce() -> Result<(), E> + Send,
    ) -> Result<Result<(), E>, MetaTxnError> {
        debug!(name_ident :? =key; "KVPbCrudApi: {}", func_name!());

        let upsert = UpsertPB::insert(key.clone(), value).with(match_seq);
        let upsert = if let Some(ttl) = ttl {
            upsert.with_ttl(ttl)
        } else {
            upsert
        };

        let transition = self.upsert_pb(&upsert).await?;

        if transition.is_changed() {
            Ok(Ok(()))
        } else {
            Ok(on_exist())
        }
    }

    /// Updates an existing key-value mapping with CAS loop.
    ///
    /// # Arguments
    /// * `name_ident` - The identifier of the key to update.
    /// * `update` - A function that takes the current value and returns an optional tuple of
    ///   (new_value, ttl). If None is returned, the update is cancelled.
    /// * `not_found` - A function called when the key doesn't exist. It should either return
    ///   an error or Ok(()) to cancel the update.
    ///
    /// # Returns
    /// * `Ok(Ok(()))` if the update was successful or cancelled.
    /// * `Ok(Err(E))` if `not_found` returned an error.
    /// * `Err(MetaTxnError)` for transaction-related errors.
    ///
    /// # Note
    /// This method uses optimistic locking and will retry on conflicts.
    async fn crud_update_existing<E>(
        &self,
        name_ident: &K,
        update: impl Fn(K::ValueType) -> Option<(K::ValueType, Option<Duration>)> + Send,
        not_found: impl Fn() -> Result<(), E> + Send,
    ) -> Result<Result<(), E>, MetaTxnError> {
        debug!(name_ident :? =name_ident; "KVPbCrudApi: {}", func_name!());

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
            txn.if_then
                .push(txn_put_pb_with_ttl(name_ident, &updated, ttl)?);

            let (succ, _responses) = send_txn(self, txn).await?;

            if succ {
                return Ok(Ok(()));
            }
        }
    }

    /// Update or insert a `name -> value` mapping, with CAS loop.
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
