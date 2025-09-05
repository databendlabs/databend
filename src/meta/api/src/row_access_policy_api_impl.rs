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

use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::row_access_policy::row_access_policy_name_ident;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReply;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyIdIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::row_access_policy_api::RowAccessPolicyApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_op_builder_util::txn_op_put_pb;

/// RowAccessPolicyApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls RowAccessPolicyApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> RowAccessPolicyApi for KV {
    async fn create_row_access(
        &self,
        req: CreateRowAccessPolicyReq,
    ) -> Result<
        Result<CreateRowAccessPolicyReply, ExistError<row_access_policy_name_ident::Resource>>,
        MetaTxnError,
    > {
        debug!(req :? =(&req); "RowAccessPolicyApi: {}", func_name!());

        let name_ident = &req.name;

        let id = fetch_id(self, IdGenerator::row_access_id()).await?;
        let mut trials = txn_backoff(None, func_name!());
        let id = loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let res = self.get_id_and_value(name_ident).await?;
            debug!(res :? = res, name_key :? =(name_ident); "create_row_access");

            let mut curr_seq = 0;

            if let Some((seq_id, seq_meta)) = res {
                if req.can_replace {
                    let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

                    txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);

                    // TODO(eason): need to remove row policy from table meta

                    curr_seq = seq_id.seq;
                } else {
                    return Ok(Err(name_ident.exist_error(func_name!())));
                }
            }

            // Create row policy by inserting these record:
            // name -> id
            // id -> policy

            let id = RowAccessPolicyId::new(id);
            let id_ident = RowAccessPolicyIdIdent::new_generic(name_ident.tenant(), id);

            debug!(
                id :? =(&id_ident),
                name_key :? =(name_ident);
                "new RowAccessPolicy id"
            );

            {
                let meta: RowAccessPolicyMeta = req.row_access_policy_meta.clone();
                txn.condition.push(txn_cond_eq_seq(name_ident, curr_seq));
                txn.if_then.extend(vec![
                    txn_op_put_pb(name_ident, &id, None)?,  // name -> policy_id
                    txn_op_put_pb(&id_ident, &meta, None)?, // id -> meta
                ]);

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(name_ident),
                    id :? =(&id_ident),
                    succ = succ;
                    "create_row_access"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(Ok(CreateRowAccessPolicyReply { id: *id }))
    }

    async fn drop_row_access(
        &self,
        name_ident: &RowAccessPolicyNameIdent,
    ) -> Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, MetaTxnError> {
        debug!(name_ident :? =(name_ident); "RowAccessPolicyApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let res = self.get_id_and_value(name_ident).await?;
            debug!(res :? = res, name_key :? =(name_ident); "{}", func_name!());

            let Some((seq_id, seq_meta)) = res else {
                return Ok(None);
            };

            let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

            txn_delete_exact(&mut txn, name_ident, seq_id.seq);
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);

            // TODO(eason): need to remove row policy from table meta

            let (succ, _responses) = send_txn(self, txn).await?;
            debug!(succ = succ;"{}", func_name!());

            if succ {
                return Ok(Some((seq_id, seq_meta)));
            }
        }
    }

    async fn get_row_access(
        &self,
        name_ident: &RowAccessPolicyNameIdent,
    ) -> Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, MetaError> {
        debug!(req :? =(&name_ident); "RowAccessPolicyApi: {}", func_name!());

        let res = self.get_id_and_value(name_ident).await?;

        Ok(res)
    }
}
