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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::data_id::DataId;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::primitive::Id;
use databend_common_meta_app::tenant_key::ident::TIdent;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::Change;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::Operation;
use databend_meta_types::SeqV;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::kv_fetch_util::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_del;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

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
    /// Create a two level `name -> id -> value` mapping with cleanup support for old resources.
    ///
    /// `associated_records` is used to generate additional key-values to add or remove along with the main operation.
    /// Such operations do not have any condition constraints.
    /// For example, a `name -> id` mapping can have a reverse `id -> name` mapping.
    ///
    /// `mark_delete_records` is used to generate additional key-values for implementing `mark_delete` operation.
    /// For example, when an index is dropped by `override_exist`,  `__fd_marked_deleted_index/<table_id>/<index_id> -> marked_deleted_index_meta` will be added.
    ///
    /// If there is already a `name_ident` exists, return the existing id in a `Ok(Err(exist))`.
    /// Otherwise, create `name -> id -> value` and returns the created id in a `Ok(Ok(created))`.
    ///
    /// `on_override_fn` is called with the old id and txn when override_exist is true and an existing
    /// resource is being replaced. It can add custom operations to the transaction.
    ///
    /// This eliminates the need for callers to manually handle cleanup logic after the fact.
    /// For example, when a procedure is dropped by `override_exist`, `__fd_object_owners/tenant1/procedure-by-id/<procedure_id>` will be delete
    async fn create_id_value<A, M, O>(
        &self,
        name_ident: &K,
        value: &IdRsc::ValueType,
        override_exist: bool,
        associated_records: A,
        mark_delete_records: M,
        on_override_fn: O,
    ) -> Result<Result<DataId<IdRsc>, SeqV<DataId<IdRsc>>>, MetaTxnError>
    where
        A: Fn(DataId<IdRsc>) -> Vec<(String, Vec<u8>)> + Send,
        M: Fn(DataId<IdRsc>, &IdRsc::ValueType) -> Result<Vec<(String, Vec<u8>)>, MetaError> + Send,
        O: Fn(DataId<IdRsc>, &mut TxnRequest) + Send,
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
                        txn.if_then.push(txn_del(&id_ident));

                        // Following txn must match this seq to proceed.
                        current_id_seq = seq_id.seq;

                        // Remove existing associated.
                        let kvs = associated_records(seq_id.data);
                        for (k, _v) in kvs {
                            txn.if_then.push(TxnOp::delete(k));
                        }

                        let kvs = mark_delete_records(seq_id.data, &seq_meta.data)?;
                        for (k, v) in kvs {
                            txn.if_then.push(TxnOp::put(k, v));
                        }
                        // Apply override function if provided
                        on_override_fn(seq_id.data, &mut txn);
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

            txn.if_then
                .push(txn_put_pb_with_ttl(name_ident, &id, None)?); // (tenant, name) -> id
            txn.if_then
                .push(txn_put_pb_with_ttl(&id_ident, value, None)?); // (id) -> value

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

    /// list by name prefix, returns a list of `(name, SeqV<id>, SeqV<value>)`
    ///
    /// Returns an iterator of `(name, SeqV<id>, SeqV<value>)` tuples.
    /// This function list all the ids by a name prefix,
    /// then get the `id->value` mapping and finally zip these two together.
    // Using `async fn` does not allow using `impl Iterator` in the return type.
    // Thus we use `impl Future` instead.
    fn list_id_value(
        &self,
        prefix: &DirName<K>,
    ) -> impl Future<
        Output = Result<
            impl Iterator<Item = (K, SeqV<DataId<IdRsc>>, SeqV<IdRsc::ValueType>)>,
            MetaError,
        >,
    > + Send {
        async move {
            let tenant = prefix.key().tenant();
            let strm = self.list_pb(ListOptions::unlimited(prefix)).await?;
            let name_ids = strm.try_collect::<Vec<_>>().await?;

            let id_idents = name_ids
                .iter()
                .map(|itm| itm.seqv.data.into_t_ident(tenant));

            let strm = self.get_pb_values(id_idents).await?;
            let seq_metas = strm.try_collect::<Vec<_>>().await?;

            let name_id_values =
                name_ids
                    .into_iter()
                    .zip(seq_metas)
                    .filter_map(|(itm, opt_seq_meta)| {
                        opt_seq_meta.map(|seq_meta| (itm.key, itm.seqv, seq_meta))
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

/// Similar to `NameIdValueApi`, but is compatibility with non-DataId ids.
#[tonic::async_trait]
pub trait NameIdValueApiCompat<K, IdTyp>: KVApi<Error = MetaError>
where
    K: kvapi::Key<ValueType = Id<IdTyp>>,
    K: KeyWithTenant,
    K: Send + Sync + 'static,
    Id<IdTyp>: FromToProto + Send + Sync + 'static,
    IdTyp: kvapi::Key + Clone + Send + Sync + 'static,
    IdTyp::ValueType: FromToProto + Send + Sync + 'static,
{
    /// mget by names, returns a list of `(name, id, value)`
    ///
    /// Returns an iterator of `(name, id, SeqV<value>)` tuples.
    /// This function mget all the ids by names,
    /// then get the `id->value` mapping and finally zip these two together.
    ///
    /// If `name->id` or `id->value` mapping does not exist, it will be skipped.
    /// Thus, the returned iterator may have less items than the input names.
    // Using `async fn` does not allow using `impl Iterator` in the return type.
    // Thus we use `impl Future` instead.
    fn mget_id_value_compat(
        &self,
        names: impl IntoIterator<Item = K> + Send,
    ) -> impl Future<
        Output = Result<impl Iterator<Item = (K, IdTyp, SeqV<IdTyp::ValueType>)>, MetaError>,
    > + Send {
        async move {
            let strm = self.get_pb_stream(names).await?;
            let name_ids = strm.try_collect::<Vec<_>>().await?;

            let name_ids = name_ids
                .into_iter()
                .filter_map(|(k, seq_v)| seq_v.map(|x| (k, x.data.into_inner())))
                .collect::<Vec<_>>();

            let id_idents = name_ids
                .iter()
                .map(|(_k, id)| id.clone())
                .collect::<Vec<_>>();

            let strm = self.get_pb_values(id_idents).await?;
            let seq_metas = strm.try_collect::<Vec<_>>().await?;

            let name_id_values =
                name_ids
                    .into_iter()
                    .zip(seq_metas)
                    .filter_map(|((k, id), opt_seq_meta)| {
                        opt_seq_meta.map(|seq_meta| (k, id, seq_meta))
                    });

            Ok(name_id_values)
        }
    }
}

impl<K, IdTyp, T> NameIdValueApiCompat<K, IdTyp> for T
where
    T: KVApi<Error = MetaError> + ?Sized,
    K: kvapi::Key<ValueType = Id<IdTyp>>,
    K: KeyWithTenant,
    K: Send + Sync + 'static,
    Id<IdTyp>: FromToProto + Send + Sync + 'static,
    IdTyp: kvapi::Key + Clone + Send + Sync + 'static,
    IdTyp::ValueType: FromToProto + Send + Sync + 'static,
{
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use databend_common_meta_app::schema::DatabaseId;
    use databend_common_meta_app::schema::DatabaseMeta;
    use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_proto_conv::FromToProto;
    use databend_meta_kvapi::kvapi::KVApi;
    use databend_meta_kvapi::kvapi::KVStream;
    use databend_meta_kvapi::kvapi::ListOptions;
    use databend_meta_kvapi::kvapi::UpsertKVReply;
    use databend_meta_types::MetaError;
    use databend_meta_types::SeqV;
    use databend_meta_types::TxnReply;
    use databend_meta_types::TxnRequest;
    use databend_meta_types::UpsertKV;
    use databend_meta_types::protobuf::StreamItem;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use prost::Message;

    use super::NameIdValueApiCompat;

    struct Foo {
        kvs: BTreeMap<String, SeqV>,
    }

    #[async_trait]
    impl KVApi for Foo {
        type Error = MetaError;

        async fn upsert_kv(&self, _req: UpsertKV) -> Result<UpsertKVReply, Self::Error> {
            unimplemented!()
        }

        async fn get_many_kv(
            &self,
            keys: BoxStream<'static, Result<String, Self::Error>>,
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            use databend_meta_kvapi::kvapi::fail_fast;
            use futures::TryStreamExt;

            let kvs = self.kvs.clone();

            let strm = fail_fast(keys).map_ok(move |key| {
                let v = kvs.get(&key).cloned();
                StreamItem::new(key, v.map(|v| v.into()))
            });
            Ok(strm.boxed())
        }

        async fn list_kv(
            &self,
            _opts: ListOptions<'_, str>,
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            unimplemented!()
        }

        async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
            unimplemented!()
        }
    }

    /// If the backend stream returns early, the returned stream should be filled with error item at the end.
    #[tokio::test]
    async fn test_mget_id_values() -> anyhow::Result<()> {
        let db_meta = |i: u64| DatabaseMeta {
            engine: i.to_string(),
            engine_options: Default::default(),
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            comment: "".to_string(),
            drop_on: None,
            gc_in_progress: false,
        };

        let v = db_meta(1).to_pb()?.encode_to_vec();

        let db_id = |i| DatabaseId::new(i).to_string().as_bytes().to_vec();

        let foo = Foo {
            kvs: vec![
                (s("__fd_database/tenant_foo/a"), SeqV::new(1, db_id(1))),
                (s("__fd_database/tenant_foo/b"), SeqV::new(2, db_id(2))),
                (s("__fd_database/tenant_foo/c"), SeqV::new(3, db_id(3))),
                (s("__fd_database_by_id/1"), SeqV::new(4, v.clone())),
                (s("__fd_database_by_id/2"), SeqV::new(5, v.clone())),
            ]
            .into_iter()
            .collect(),
        };

        // MGet key value pairs
        {
            let tenant = Tenant::new_literal("tenant_foo");
            let it = foo
                .mget_id_value_compat([
                    DatabaseNameIdent::new(&tenant, "a"),
                    DatabaseNameIdent::new(&tenant, "b"),
                    DatabaseNameIdent::new(&tenant, "c"),
                    DatabaseNameIdent::new(&tenant, "d"), // No such key
                ])
                .await?;

            let got = it.collect::<Vec<_>>();
            assert_eq!(got, vec![
                (
                    DatabaseNameIdent::new(&tenant, "a"),
                    DatabaseId::new(1),
                    SeqV::new(4, db_meta(1))
                ),
                (
                    DatabaseNameIdent::new(&tenant, "b"),
                    DatabaseId::new(2),
                    SeqV::new(5, db_meta(1))
                ),
            ]);
        }

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }
}
