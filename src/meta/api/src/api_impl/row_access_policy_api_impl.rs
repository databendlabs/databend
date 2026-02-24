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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReply;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyIdIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyIdToNameIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdentRaw;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_name_ident;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use super::errors::RowAccessPolicyError;
use super::row_access_policy_api::RowAccessPolicyApi;
use crate::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_keys_with_prefix;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_op_builder_util::txn_del;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

/// RowAccessPolicyApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls RowAccessPolicyApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> RowAccessPolicyApi for KV {
    async fn create_row_access_policy(
        &self,
        req: CreateRowAccessPolicyReq,
    ) -> Result<
        Result<CreateRowAccessPolicyReply, ExistError<row_access_policy_name_ident::Resource>>,
        MetaError,
    > {
        debug!(req :? =(&req); "RowAccessPolicyApi: {}", func_name!());

        let name_ident = &req.name;

        let mask_name_ident = DataMaskNameIdent::new(
            name_ident.tenant().clone(),
            name_ident.row_access_name().to_string(),
        );

        let row_access_id = fetch_id(self, IdGenerator::row_access_id()).await?;
        let policy_id = RowAccessPolicyId::new(row_access_id);
        let mut txn = TxnRequest::default();

        // Create row policy by inserting these record:
        // name -> id
        // id -> policy
        // id -> name

        let id_ident = RowAccessPolicyIdIdent::new_generic(name_ident.tenant(), policy_id);
        let id_to_name_ident =
            RowAccessPolicyIdToNameIdent::new_generic(name_ident.tenant(), policy_id);
        let name_raw = RowAccessPolicyNameIdentRaw::from(name_ident.clone());

        debug!(
            id :? =(&id_ident),
            name_key :? =(name_ident);
            "new RowAccessPolicy id"
        );

        {
            let meta: RowAccessPolicyMeta = req.row_access_policy_meta.clone();
            txn.condition.push(txn_cond_eq_seq(name_ident, 0));
            txn.condition.push(txn_cond_eq_seq(&mask_name_ident, 0));
            txn.if_then.extend(vec![
                txn_put_pb_with_ttl(name_ident, &policy_id, None)?, // name -> policy_id
                txn_put_pb_with_ttl(&id_ident, &meta, None)?,       // id -> meta
                txn_put_pb_with_ttl(&id_to_name_ident, &name_raw, None)?, // id -> name
            ]);
        }

        let (succ, _responses) = send_txn(self, txn).await?;

        debug!(
            name :? =(name_ident),
            id :? =(&id_ident),
            succ = succ;
            "create_row_access"
        );

        if succ {
            Ok(Ok(CreateRowAccessPolicyReply { id: row_access_id }))
        } else {
            Ok(Err(
                name_ident.exist_error(format!("{} already exists", req.name))
            ))
        }
    }

    async fn drop_row_access_policy(
        &self,
        name_ident: &RowAccessPolicyNameIdent,
    ) -> Result<
        Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, RowAccessPolicyError>,
        MetaTxnError,
    > {
        debug!(name_ident :? =(name_ident); "RowAccessPolicyApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Check if policy exists
            let res = self.get_id_and_value(name_ident).await?;
            let Some((seq_id, seq_meta)) = res else {
                return Ok(Ok(None));
            };

            let policy_id = *seq_id.data;
            let tenant = name_ident.tenant();
            let table_policy_ref_prefix = DirName::new(RowAccessPolicyTableIdIdent::new_generic(
                tenant.clone(),
                RowAccessPolicyIdTableId {
                    policy_id,
                    table_id: 0,
                },
            ));

            // List all table-policy references
            let table_policy_refs = self
                .list_pb_vec(ListOptions::unlimited(&table_policy_ref_prefix))
                .await?;

            // Policy is in use - cannot drop
            if !table_policy_refs.is_empty() {
                return Ok(Err(RowAccessPolicyError::policy_in_use(
                    name_ident.row_access_name().to_string(),
                )));
            }

            // No references - drop the policy
            let id_ident = seq_id.data.into_t_ident(tenant);
            let id_to_name_ident = RowAccessPolicyIdToNameIdent::new_generic(
                tenant.clone(),
                RowAccessPolicyId::new(policy_id),
            );
            let mut txn = TxnRequest::default();

            // Ensure no new references were created
            txn.condition
                .push(txn_cond_eq_keys_with_prefix(&table_policy_ref_prefix, 0));

            txn_delete_exact(&mut txn, name_ident, seq_id.seq);
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);
            txn.if_then.push(txn_del(&id_to_name_ident));

            let (succ, _responses) = send_txn(self, txn).await?;
            if succ {
                return Ok(Ok(Some((seq_id, seq_meta))));
            }
            // Transaction failed, retry
        }
    }

    async fn get_row_access_policy(
        &self,
        name_ident: &RowAccessPolicyNameIdent,
    ) -> Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, MetaError> {
        debug!(req :? =(&name_ident); "RowAccessPolicyApi: {}", func_name!());

        let res = self.get_id_and_value(name_ident).await?;

        Ok(res)
    }

    async fn get_row_access_policy_name_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<String>, MetaError> {
        let ident = RowAccessPolicyIdToNameIdent::new_generic(
            tenant.clone(),
            RowAccessPolicyId::new(policy_id),
        );
        let seq_meta = self.get_pb(&ident).await?;

        debug!(ident :% =(&ident); "get_row_access_policy_name_by_id");

        let name = seq_meta.map(|s| s.data.row_access_name().to_string());

        Ok(name)
    }

    async fn get_row_access_policy_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<SeqV<RowAccessPolicyMeta>>, MetaError> {
        debug!(req :? =(policy_id); "RowAccessPolicyApi: {}", func_name!());

        let id = RowAccessPolicyId::new(policy_id);
        let id_ident = RowAccessPolicyIdIdent::new_generic(tenant, id);

        let res = self.get_pb(&id_ident).await?;
        Ok(res)
    }
}
