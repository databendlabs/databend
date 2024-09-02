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
use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use databend_common_proto_conv::FromToProto;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_op_del;
use crate::util::fetch_id;
use crate::util::send_txn;
use crate::util::txn_cond_eq_seq;
use crate::util::txn_op_put_pb;

/// BaseApi provide several generic meta-service access pattern implementations
///
/// These implementations are used by other meta-service APIs.
#[tonic::async_trait]
pub trait BaseApi: KVApi<Error = MetaError> {
    /// Create a two level `name -> id -> value` mapping.
    ///
    /// `IdResource` is the Resource definition for id.
    /// `associated_ops` is used to generate additional key-values to add or remove along with the main operation.
    /// Such operations do not have any condition constraints.
    /// For example, a `name -> id` mapping can have a reverse `id -> name` mapping.
    ///
    /// If there is already a `name_ident` exists, return the existing id in a `Ok(Err(exist))`.
    /// Otherwise, create `name -> id -> value` and returns the created id in a `Ok(Ok(created))`.
    async fn create_id_value<K, V, IdResource, A>(
        &self,
        name_ident: &K,
        value: &V,
        override_exist: bool,
        associated_records: A,
    ) -> Result<Result<DataId<IdResource>, SeqV<DataId<IdResource>>>, MetaTxnError>
    where
        K: kvapi::Key<ValueType = DataId<IdResource>>,
        K: KeyWithTenant,
        K: Sync,
        IdResource: TenantResource + Send + Sync + 'static,
        IdResource::ValueType: FromToProto,
        TIdent<IdResource, DataId<IdResource>>: kvapi::Key<ValueType = V>,
        V: FromToProto + Sync + 'static,
        A: Fn(DataId<IdResource>) -> Vec<(String, Vec<u8>)> + Send,
    {
        debug!(name_ident :? =name_ident; "SchemaApi: {}", func_name!());

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
            let id = DataId::<IdResource>::new(idu64);
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

    async fn get_id_value<K, IdResource>(
        &self,
        key: &K,
    ) -> Result<Option<(SeqV<DataId<IdResource>>, SeqV<IdResource::ValueType>)>, MetaError>
    where
        K: kvapi::Key<ValueType = DataId<IdResource>> + KeyWithTenant + Sync,
        IdResource: TenantResource + Send + Sync,
        IdResource::ValueType: FromToProto,
    {
        self.get_id_and_value(key).await
    }

    /// list by key prefix, returns a list of `(key, id, value)`
    ///
    /// This function list all the ids by a key prefix,
    /// then get the `id->value` mapping and finally zip these two together.
    // Using `async fn` does not allow using `impl Iterator` in the return type.
    // Thus we use `impl Future` instead.
    fn list_id_value<K, IdResource>(
        &self,
        prefix: &DirName<K>,
        // ) -> Result<impl Drop, MetaError>
    ) -> impl Future<
        Output = Result<
            impl Iterator<Item = (K, DataId<IdResource>, SeqV<IdResource::ValueType>)>,
            MetaError,
        >,
    > + Send
    where
        K: kvapi::Key<ValueType = DataId<IdResource>> + KeyWithTenant + Send + Sync + 'static,
        IdResource: TenantResource + Send + Sync,
        IdResource::ValueType: FromToProto + Send + Sync + 'static,
    {
        async move {
            let tenant = prefix.key().tenant();
            let strm = self.list_pb(prefix).await?;
            let name_ids = strm.try_collect::<Vec<_>>().await?;

            let id_idents = name_ids.iter().map(|itm| {
                let id = itm.seqv.data;
                id.into_t_ident(tenant)
            });

            #[allow(deprecated)]
            let strm = self.get_pb_values(id_idents).await?;
            let seq_metas = strm.try_collect::<Vec<_>>().await?;

            if seq_metas.len() != name_ids.len() {
                return Err(InvalidReply::new(
                    "seq_metas.len() {} != name_ids.len() {}",
                    &AnyError::error(format!(
                        "seq_metas.len() {} != name_ids.len() {}",
                        seq_metas.len(),
                        name_ids.len()
                    )),
                )
                .into());
            }

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

impl<T> BaseApi for T where T: KVApi<Error = MetaError> + ?Sized {}
