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

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskId;
use databend_common_meta_app::data_mask::DataMaskIdIdent;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::data_mask_api::DatamaskApi;
use crate::errors::MaskingPolicyError;
use crate::fetch_id;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
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

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> DatamaskApi for KV {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<CreateDatamaskReply, KVAppError> {
        debug!(req :? =(&req); "DatamaskApi: {}", func_name!());

        let name_ident = &req.name;

        let mut trials = txn_backoff(None, func_name!());
        let id = loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let res = self.get_id_and_value(name_ident).await?;
            debug!(res :? = res, name_key :? =(name_ident); "create_data_mask");

            let mut curr_seq = 0;

            if let Some((seq_id, seq_meta)) = res {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(AppError::DatamaskAlreadyExists(
                            name_ident.exist_error(func_name!()),
                        )
                        .into());
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(CreateDatamaskReply { id: *seq_id.data });
                    }
                    CreateOption::CreateOrReplace => {
                        let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

                        txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);

                        curr_seq = seq_id.seq;
                    }
                };
            }

            // Create data mask by inserting these record:
            // name -> id
            // id -> policy
            // data mask name -> data mask table id list

            let id = fetch_id(self, IdGenerator::data_mask_id()).await?;

            let id = DataMaskId::new(id);
            let id_ident = DataMaskIdIdent::new_generic(name_ident.tenant(), id);
            let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

            debug!(
                id :? =(&id_ident),
                name_key :? =(name_ident);
                "new datamask id"
            );

            {
                let meta: DatamaskMeta = req.data_mask_meta.clone();
                let id_list = MaskpolicyTableIdList::default();
                txn.condition.push(txn_cond_eq_seq(name_ident, curr_seq));
                txn.if_then.extend(vec![
                    txn_op_put_pb(name_ident, &id, None)?,  // name -> db_id
                    txn_op_put_pb(&id_ident, &meta, None)?, // id -> meta
                    // TODO: Tentative retention for compatibility MaskPolicyTableIdListIdent related logic. It can be directly deleted later
                    txn_op_put_pb(&id_list_key, &id_list, None)?, // data mask name -> id_list
                ]);

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(name_ident),
                    id :? =(&id_ident),
                    succ = succ;
                    "create_data_mask"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateDatamaskReply { id: *id })
    }

    async fn drop_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<
        Result<Option<(SeqV<DataMaskId>, SeqV<DatamaskMeta>)>, MaskingPolicyError>,
        MetaTxnError,
    > {
        debug!(name_ident :? =(name_ident); "DatamaskApi: {}", func_name!());

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
            let usage = collect_mask_policy_usage(self, name_ident.tenant(), policy_id).await?;
            if !usage.active_tables.is_empty() {
                let tenant = name_ident.tenant().tenant_name().to_string();
                let policy_name = name_ident.data_mask_name().to_string();
                let err = MaskingPolicyError::policy_in_use(tenant, policy_name);
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

            if finalize_policy {
                // TODO: Tentative retention for compatibility MaskPolicyTableIdListIdent related logic. It can be directly deleted later
                clear_table_column_mask_policy(self, name_ident, &mut txn).await?;
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

    async fn get_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<Option<SeqV<DatamaskMeta>>, MetaError> {
        debug!(req :? =(&name_ident); "DatamaskApi: {}", func_name!());

        let res = self.get_id_and_value(name_ident).await?;

        Ok(res.map(|(_, seq_meta)| seq_meta))
    }

    async fn get_data_mask_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<SeqV<DatamaskMeta>>, MetaError> {
        debug!(req :? =(policy_id); "DatamaskApi: {}", func_name!());

        let id = DataMaskId::new(policy_id);
        let id_ident = DataMaskIdIdent::new_generic(tenant, id);

        let res = self.get_pb(&id_ident).await?;
        Ok(res)
    }
}

async fn clear_table_column_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &DataMaskNameIdent,
    txn: &mut TxnRequest,
) -> Result<(), MetaTxnError> {
    let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

    let seq_id_list = kv_api
        .get_pb(&id_list_key)
        .await
        .map_err(MetaTxnError::from)?;

    let Some(seq_id_list) = seq_id_list else {
        return Ok(());
    };

    txn_delete_exact(txn, &id_list_key, seq_id_list.seq);
    Ok(())
}

type MaskPolicyUsage = PolicyUsage<MaskPolicyTableIdIdent>;

impl PolicyBinding for MaskPolicyTableIdIdent {
    fn prefix_for(tenant: &Tenant, policy_id: u64) -> DirName<Self> {
        DirName::new(MaskPolicyTableIdIdent::new_generic(
            tenant.clone(),
            MaskPolicyIdTableId {
                policy_id,
                table_id: 0,
            },
        ))
    }

    fn table_id(&self) -> u64 {
        self.name().table_id
    }

    fn remove_security_policy_from_table_meta(table_meta: &mut TableMeta, policy_id: u64) -> bool {
        let before = table_meta.column_mask_policy_columns_ids.len();
        table_meta
            .column_mask_policy_columns_ids
            .retain(|_, policy| policy.policy_id != policy_id);

        let removed = before != table_meta.column_mask_policy_columns_ids.len();
        if removed {
            table_meta.column_mask_policy = None;
        }

        removed
    }
}

async fn collect_mask_policy_usage(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
    policy_id: u64,
) -> Result<MaskPolicyUsage, MetaTxnError> {
    collect_policy_usage(kv_api, tenant, policy_id).await
}
