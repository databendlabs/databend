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

use std::future::Future;

use databend_common_meta_app::data_id::DataId;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::tenant_key::ident::TIdent;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::Change;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_proto_conv::FromToProto;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_op_del;
use crate::util::fetch_id;
use crate::util::send_txn;
use crate::util::txn_cond_eq_seq;
use crate::util::txn_delete_exact;
use crate::util::txn_op_put_pb;

/// NameIdValueApi provide generic meta-service access pattern implementations for `name -> id -> value` mapping.
///
/// Such a two level mapping provides a consistent id `id` for internal use,
/// which will not be affected by renaming the `name`.
/// For example, using `TableId` to update a table metadata won't conflict
/// with another transaction that renaming a table `name -> id`
///
/// `K` is the key type for name.
/// `IdRsc` is the Resource definition for id.
/// `IdRsc::ValueType` is the value type.
#[tonic::async_trait]
pub trait NameIdValueApi<K, IdRsc>: KVApi<Error = MetaError>
where
    K: kvapi::Key<ValueType = DataId<IdRsc>>,
    K: KeyWithTenant,
    K: Send + Sync + 'static,
    IdRsc: TenantResource + Send + Sync + 'static,
    IdRsc::ValueType: FromToProto + Send + Sync + 'static,
{
    /// Create a two level `name -> id -> value` mapping.
    ///
    /// `associated_records` is used to generate additional key-values to add or remove along with the main operation.
    /// Such operations do not have any condition constraints.
    /// For example, a `name -> id` mapping can have a reverse `id -> name` mapping.
    ///
    /// If there is already a `name_ident` exists, return the existing id in a `Ok(Err(exist))`.
    /// Otherwise, create `name -> id -> value` and returns the created id in a `Ok(Ok(created))`.
    async fn create_id_value<A>(
        &self,
        name_ident: &K,
        value: &IdRsc::ValueType,
        override_exist: bool,
        associated_records: A,
    ) -> Result<Result<DataId<IdRsc>, SeqV<DataId<IdRsc>>>, MetaTxnError>
    where
        A: Fn(DataId<IdRsc>) -> Vec<(String, Vec<u8>)> + Send,
    {
        debug!(name_ident :? =name_ident; "NameIdValueApi: {}", func_name!());

        let tenant = name_ident.tenant();

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let mut current_id_seq = 0;

            {
                let get_res = self.get_id_and_value(name_ident).await?;

                if let Some((seq_id, seq_meta)) = get_res {
                    if override_exist {
                        // Override take place only when the id -> value does not change.
                        // If it does not override, no such condition is required.
                        let id_ident = seq_id.data.into_t_ident(tenant);
                        txn.condition.push(txn_cond_eq_seq(&id_ident, seq_meta.seq));
                        txn.if_then.push(txn_op_del(&id_ident));

                        // Following txn must match this seq to proceed.
                        current_id_seq = seq_id.seq;

                        // Remove existing associated.
                        let kvs = associated_records(seq_id.data);
                        for (k, _v) in kvs {
                            txn.if_then.push(TxnOp::delete(k));
                        }
                    } else {
                        return Ok(Err(seq_id));
                    }
                };
            }

            let idu64 = fetch_id(self, IdGenerator::generic()).await?;
            let id = DataId::<IdRsc>::new(idu64);
            let id_ident = id.into_t_ident(name_ident.tenant());
            debug!(id :? = id,name_ident :? =name_ident; "new id");

            txn.condition
                .extend(vec![txn_cond_eq_seq(name_ident, current_id_seq)]);

            txn.if_then.push(txn_op_put_pb(name_ident, &id)?); // (tenant, name) -> id
            txn.if_then.push(txn_op_put_pb(&id_ident, value)?); // (id) -> value

            // Add associated
            let kvs = associated_records(id);
            for (k, v) in kvs {
                txn.if_then.push(TxnOp::put(k, v));
            }

            let (succ, _responses) = send_txn(self, txn).await?;
            debug!(name_ident :? =name_ident, id :? =&id_ident,succ = succ; "{}", func_name!());

            if succ {
                return Ok(Ok(id));
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
    async fn remove_id_value<A>(
        &self,
        name_ident: &K,
        associated_keys: A,
    ) -> Result<Option<(SeqV<DataId<IdRsc>>, SeqV<IdRsc::ValueType>)>, MetaTxnError>
    where
        A: Fn(DataId<IdRsc>) -> Vec<String> + Send,
    {
        debug!(key :? =name_ident; "NameIdValueApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let get_res = self.get_id_value(name_ident).await?;
            let Some((seq_id, seq_meta)) = get_res else {
                return Ok(None);
            };

            let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

            txn_delete_exact(&mut txn, name_ident, seq_id.seq);
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);

            // Remove associated
            let ks = associated_keys(seq_id.data);
            for k in ks {
                txn.if_then.push(TxnOp::delete(k));
            }

            let (succ, _responses) = send_txn(self, txn).await?;
            debug!(key :? =name_ident, id :? =&id_ident,succ = succ; "{}", func_name!());

            if succ {
                return Ok(Some((seq_id, seq_meta)));
            }
        }
    }

    /// Update the value part of `name -> id -> value`, identified by name.
    ///
    /// Returns the value before update if success, otherwise None.
    ///
    /// This function does not modify the `name -> id` mapping.
    async fn update_id_value(
        &self,
        key: &K,
        value: IdRsc::ValueType,
    ) -> Result<Option<(DataId<IdRsc>, IdRsc::ValueType)>, MetaError> {
        let got = self.get_id_value(key).await?;

        let Some((seq_id, seq_meta)) = got else {
            return Ok(None);
        };

        let tenant = key.tenant();
        let id_ident = seq_id.data.into_t_ident(tenant);
        let transition = self.update_by_id(id_ident, value).await?;

        if transition.is_changed() {
            Ok(Some((seq_id.data, seq_meta.data)))
        } else {
            // update_by_id always succeed, unless the id->value mapping is removed.
            Ok(None)
        }
    }

    /// Update the value part of `id -> value` mapping by id.
    ///
    /// It returns the state transition of the update operation: `(prev, result)`.
    async fn update_by_id(
        &self,
        id_ident: TIdent<IdRsc, DataId<IdRsc>>,
        value: IdRsc::ValueType,
    ) -> Result<Change<IdRsc::ValueType>, MetaError> {
        let reply = self
            .upsert_pb(&UpsertPB::new(
                id_ident,
                MatchSeq::GE(1),
                Operation::Update(value),
                None,
            ))
            .await?;

        Ok(reply)
    }

    /// Get the `name -> id -> value` mapping by name.
    ///
    /// Returning `None` means the name does not exist.
    /// Otherwise, returns the `SeqV<id>` and `SeqV<value>`.
    async fn get_id_value(
        &self,
        key: &K,
    ) -> Result<Option<(SeqV<DataId<IdRsc>>, SeqV<IdRsc::ValueType>)>, MetaError> {
        self.get_id_and_value(key).await
    }

    /// list by name prefix, returns a list of `(name, id, value)`
    ///
    /// Returns an iterator of `(name, id, SeqV<value>)` tuples.
    /// This function list all the ids by a name prefix,
    /// then get the `id->value` mapping and finally zip these two together.
    // Using `async fn` does not allow using `impl Iterator` in the return type.
    // Thus we use `impl Future` instead.
    fn list_id_value(
        &self,
        prefix: &DirName<K>,
    ) -> impl Future<
        Output = Result<
            impl Iterator<Item = (K, DataId<IdRsc>, SeqV<IdRsc::ValueType>)>,
            MetaError,
        >,
    > + Send {
        async move {
            let tenant = prefix.key().tenant();
            let strm = self.list_pb(prefix).await?;
            let name_ids = strm.try_collect::<Vec<_>>().await?;

            let id_idents = name_ids.iter().map(|itm| {
                let id = itm.seqv.data;
                id.into_t_ident(tenant)
            });

            let strm = self.get_pb_values(id_idents).await?;
            let seq_metas = strm.try_collect::<Vec<_>>().await?;

            let name_id_values =
                name_ids
                    .into_iter()
                    .zip(seq_metas)
                    .filter_map(|(itm, opt_seq_meta)| {
                        opt_seq_meta.map(|seq_meta| (itm.key, itm.seqv.data, seq_meta))
                    });

            Ok(name_id_values)
        }
    }
}

impl<K, IdRsc, T> NameIdValueApi<K, IdRsc> for T
where
    T: KVApi<Error = MetaError> + ?Sized,
    K: kvapi::Key<ValueType = DataId<IdRsc>>,
    K: KeyWithTenant,
    K: Send + Sync + 'static,
    IdRsc: TenantResource + Send + Sync + 'static,
    IdRsc::ValueType: FromToProto + Send + Sync + 'static,
{
}
