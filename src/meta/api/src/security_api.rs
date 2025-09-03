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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::SetTableRowAccessPolicyAction;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReply;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::errors::TableError;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::serialize_struct;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_replace_exact;
use crate::txn_op_del;
use crate::txn_op_put;

/// SecurityApi defines APIs for table security policy management.
///
/// This trait handles:
/// - Column masking policy operations
/// - Row access policy operations
#[async_trait::async_trait]
pub trait SecurityApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply, KVAppError> {
        debug!(req :? =(&req); "SecurityApi: {}", func_name!());
        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(&tbid).await?;

            debug!(ident :% =(&tbid); "set_table_column_mask_policy");

            let Some(seq_meta) = seq_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "set_table_column_mask_policy"),
                )));
            };

            if req_seq.match_seq(&seq_meta.seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        seq_meta.seq,
                        "set_table_column_mask_policy",
                    ),
                )));
            }

            // upsert column mask policy
            let table_meta = seq_meta.data;

            let mut new_table_meta = table_meta.clone();
            if new_table_meta.column_mask_policy.is_none() {
                let column_mask_policy = BTreeMap::default();
                new_table_meta.column_mask_policy = Some(column_mask_policy);
            }

            match &req.action {
                SetTableColumnMaskPolicyAction::Set(new_mask_name, _old_mask_name) => {
                    new_table_meta
                        .column_mask_policy
                        .as_mut()
                        .unwrap()
                        .insert(req.column.clone(), new_mask_name.clone());
                }
                SetTableColumnMaskPolicyAction::Unset(_) => {
                    new_table_meta
                        .column_mask_policy
                        .as_mut()
                        .unwrap()
                        .remove(&req.column);
                }
            }

            let mut txn_req = TxnRequest::new(
                vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, seq_meta.seq),
                ],
                vec![
                    txn_op_put(&tbid, serialize_struct(&new_table_meta)?), // tb_id -> tb_meta
                ],
            );

            let _ = update_mask_policy(self, &req.action, &mut txn_req, &req.tenant, req.table_id)
                .await;

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id :? =(&tbid),
                succ = succ;
                "set_table_column_mask_policy"
            );

            if succ {
                return Ok(SetTableColumnMaskPolicyReply {});
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_table_row_access_policy(
        &self,
        req: SetTableRowAccessPolicyReq,
    ) -> Result<Result<SetTableRowAccessPolicyReply, TableError>, MetaTxnError> {
        debug!(req :? =(&req); "SecurityApi: {}", func_name!());
        let tbid = TableId {
            table_id: req.table_id,
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(&tbid).await?;

            debug!(ident :% =(&tbid); "set_table_row_access_policy");

            let Some(seq_meta) = seq_meta else {
                return Ok(Err(TableError::UnknownTableId {
                    tenant: req.tenant.tenant_name().to_string(),
                    table_id: req.table_id,
                    context: "set_table_row_access_policy".to_string(),
                }));
            };

            // upsert row access policy
            let table_meta = seq_meta.data;
            let mut new_table_meta = table_meta.clone();
            let id = RowAccessPolicyIdTableId {
                policy_id: req.policy_id,
                table_id: req.table_id,
            };
            let ident = RowAccessPolicyTableIdIdent::new_generic(req.tenant.clone(), id);

            let mut txn_req = TxnRequest::default();

            txn_req
                .condition
                .push(txn_cond_seq(&tbid, Eq, seq_meta.seq));
            match &req.action {
                SetTableRowAccessPolicyAction::Set(new_mask_name) => {
                    if table_meta.row_access_policy.is_some() {
                        return Ok(Err(TableError::AlterTableError {
                            tenant: req.tenant.tenant_name().to_string(),
                            context: "Table already has a ROW_ACCESS_POLICY. Only one ROW_ACCESS_POLICY is allowed at a time.".to_string(),
                        }));
                    }
                    new_table_meta.row_access_policy = Some(new_mask_name.to_string());
                    txn_req.if_then = vec![
                        txn_op_put(&tbid, serialize_struct(&new_table_meta)?), /* tb_id -> tb_meta row access policy Some */
                        txn_op_put(&ident, serialize_struct(&RowAccessPolicyTableId {})?), /* add policy_tb_id */
                    ];
                }
                SetTableRowAccessPolicyAction::Unset(old_policy) => {
                    // drop row access policy and table does not have row access policy
                    if let Some(policy) = &table_meta.row_access_policy {
                        if policy != old_policy {
                            return Ok(Err(TableError::AlterTableError {
                                tenant: req.tenant.tenant_name().to_string(),
                                context: format!("Unknown row access policy {} on table", policy),
                            }));
                        }
                    } else {
                        return Ok(Ok(SetTableRowAccessPolicyReply {}));
                    }
                    new_table_meta.row_access_policy = None;
                    txn_req.if_then = vec![
                        txn_op_put(&tbid, serialize_struct(&new_table_meta)?), /* tb_id -> tb_meta row access policy None */
                        txn_op_del(&ident), // table drop row access policy, del policy_tb_id
                    ];
                }
            }

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id :? =(&tbid),
                succ = succ;
                "set_table_row_access_policy"
            );

            if succ {
                return Ok(Ok(SetTableRowAccessPolicyReply {}));
            }
        }
    }
}

async fn update_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    action: &SetTableColumnMaskPolicyAction,
    txn_req: &mut TxnRequest,
    tenant: &Tenant,
    table_id: u64,
) -> Result<(), KVAppError> {
    /// Fetch and update the table id list with `f`, and fill in the txn preconditions and operations.
    async fn update_table_ids(
        kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
        txn_req: &mut TxnRequest,
        key: MaskPolicyTableIdListIdent,
        f: impl FnOnce(&mut BTreeSet<u64>),
    ) -> Result<(), KVAppError> {
        let Some(mut seq_list) = kv_api.get_pb(&key).await? else {
            return Ok(());
        };

        f(&mut seq_list.data.id_list);

        txn_replace_exact(txn_req, &key, seq_list.seq, &seq_list.data)?;

        Ok(())
    }

    match action {
        SetTableColumnMaskPolicyAction::Set(new_mask_name, old_mask_name_opt) => {
            update_table_ids(
                kv_api,
                txn_req,
                MaskPolicyTableIdListIdent::new(tenant.clone(), new_mask_name),
                |list: &mut BTreeSet<u64>| {
                    list.insert(table_id);
                },
            )
            .await?;

            if let Some(old) = old_mask_name_opt {
                update_table_ids(
                    kv_api,
                    txn_req,
                    MaskPolicyTableIdListIdent::new(tenant.clone(), old),
                    |list: &mut BTreeSet<u64>| {
                        list.remove(&table_id);
                    },
                )
                .await?;
            }
        }
        SetTableColumnMaskPolicyAction::Unset(mask_name) => {
            update_table_ids(
                kv_api,
                txn_req,
                MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name),
                |list: &mut BTreeSet<u64>| {
                    list.remove(&table_id);
                },
            )
            .await?;
        }
    }

    Ok(())
}

#[async_trait::async_trait]
impl<KV> SecurityApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
