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
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReply;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyIdIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::errors::RowAccessPolicyError;
use crate::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::row_access_policy_api::RowAccessPolicyApi;
use crate::security_policy_usage::collect_policy_usage;
use crate::security_policy_usage::PolicyBinding;
use crate::security_policy_usage::PolicyDropTxnBatch;
use crate::security_policy_usage::PolicyUsage;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_keys_with_prefix;
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
    ) -> Result<
        Result<Option<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>, RowAccessPolicyError>,
        MetaTxnError,
    > {
        debug!(name_ident :? =(name_ident); "RowAccessPolicyApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap().map_err(MetaTxnError::from)?.await;

            let res = self
                .get_id_and_value(name_ident)
                .await
                .map_err(MetaTxnError::from)?;
            debug!(res :? = res, name_key :? =(name_ident); "{}", func_name!());

            let Some((seq_id, seq_meta)) = res else {
                return Ok(Ok(None));
            };

            let policy_id = *seq_id.data;
            let usage =
                collect_row_access_policy_usage(self, name_ident.tenant(), policy_id).await?;
            if !usage.active_tables.is_empty() {
                let tenant = name_ident.tenant().tenant_name().to_string();
                let policy_name = name_ident.row_access_name().to_string();
                let err = RowAccessPolicyError::policy_in_use(tenant, policy_name);
                return Ok(Err(err));
            }

            let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

            let PolicyDropTxnBatch {
                prefix,
                binding_count,
                bindings,
                table_updates,
                finalize_policy,
            } = usage.prepare_drop_batch(name_ident.tenant(), policy_id);

            let mut txn = TxnRequest::default();
            txn.condition
                .push(txn_cond_eq_keys_with_prefix(&prefix, binding_count));

            if finalize_policy {
                txn_delete_exact(&mut txn, name_ident, seq_id.seq);
                txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);
            }

            for binding in bindings {
                txn_delete_exact(&mut txn, &binding.ident, binding.seq);
            }

            for update in table_updates {
                txn.condition
                    .push(txn_cond_eq_seq(&update.table_id, update.seq));
                let op = txn_op_put_pb(&update.table_id, &update.meta, None)
                    .map_err(MetaTxnError::from)?;
                txn.if_then.push(op);
            }

            let (succ, _responses) = send_txn(self, txn).await.map_err(MetaTxnError::from)?;

            if finalize_policy {
                debug!(succ = succ;"{}", func_name!());
                if succ {
                    return Ok(Ok(Some((seq_id, seq_meta))));
                }
            } else {
                debug!(succ = succ, cleanup = true;"{}", func_name!());
                continue;
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

    async fn get_row_access_by_id(
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

type RowAccessPolicyUsage = PolicyUsage<RowAccessPolicyTableIdIdent>;

impl PolicyBinding for RowAccessPolicyTableIdIdent {
    fn prefix_for(tenant: &Tenant, policy_id: u64) -> DirName<Self> {
        DirName::new(RowAccessPolicyTableIdIdent::new_generic(
            tenant.clone(),
            RowAccessPolicyIdTableId {
                policy_id,
                table_id: 0,
            },
        ))
    }

    fn table_id(&self) -> u64 {
        self.name().table_id
    }

    fn remove_security_policy_from_table_meta(table_meta: &mut TableMeta, policy_id: u64) -> bool {
        match &table_meta.row_access_policy_columns_ids {
            Some(policy) if policy.policy_id == policy_id => {
                table_meta.row_access_policy_columns_ids = None;
                if table_meta.row_access_policy.is_some() {
                    table_meta.row_access_policy = None;
                }
                true
            }
            _ => false,
        }
    }
}

async fn collect_row_access_policy_usage(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
    policy_id: u64,
) -> Result<RowAccessPolicyUsage, MetaTxnError> {
    collect_policy_usage(kv_api, tenant, policy_id).await
}
