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
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::SetSecurityPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReply;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_kvapi::kvapi;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::MetaError;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use super::errors::TableError;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_put_pb;

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

            match &req.action {
                SetSecurityPolicyAction::Set(policy_id, column_ids) => {
                    // Get policy name from policy ID by finding the name ident that maps to this ID
                    // use databend_common_meta_app::data_mask::DataMaskNameIdent;
                    // We need to iterate through mask policies to find the name that corresponds to this ID
                    // For now, let's use the policy ID as a string since that's what's stored in column_mask_policy
                    new_table_meta.column_mask_policy = None;
                    let policy_map = SecurityPolicyColumnMap {
                        policy_id: *policy_id,
                        columns_ids: column_ids.clone(),
                    };

                    if let Some(column_id) = column_ids.first() {
                        new_table_meta
                            .column_mask_policy_columns_ids
                            .insert(*column_id, policy_map.clone());
                    }
                }
                SetSecurityPolicyAction::Unset(policy_id) => {
                    new_table_meta.column_mask_policy = None;
                    // Remove entries where the policy ID matches
                    new_table_meta
                        .column_mask_policy_columns_ids
                        .retain(|_, policy| policy.policy_id != *policy_id);
                }
            }

            let mut txn_req = TxnRequest::new(
                vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, seq_meta.seq),
                ],
                vec![
                    txn_put_pb(&tbid, &new_table_meta)?, // tb_id -> tb_meta
                ],
            );

            let _ = update_mask_policy(&req.action, &mut txn_req, &req.tenant, req.table_id).await;

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
            let policy_id = match req.action {
                SetSecurityPolicyAction::Set(id, _) => id,
                SetSecurityPolicyAction::Unset(id) => id,
            };
            let id = RowAccessPolicyIdTableId {
                policy_id,
                table_id: req.table_id,
            };
            let ident = RowAccessPolicyTableIdIdent::new_generic(req.tenant.clone(), id);

            let mut txn_req = TxnRequest::default();

            txn_req
                .condition
                .push(txn_cond_seq(&tbid, Eq, seq_meta.seq));
            match &req.action {
                SetSecurityPolicyAction::Set(new_policy_id, columns_ids) => {
                    if table_meta.row_access_policy_columns_ids.is_some() {
                        return Ok(Err(TableError::AlterTableError {
                            tenant: req.tenant.tenant_name().to_string(),
                            context: "Table already has a ROW_ACCESS_POLICY. Only one ROW_ACCESS_POLICY is allowed at a time.".to_string(),
                        }));
                    }
                    new_table_meta.row_access_policy_columns_ids = Some(
                        SecurityPolicyColumnMap::new(*new_policy_id, columns_ids.clone()),
                    );
                    // Compatibility, can be deleted in the future
                    new_table_meta.row_access_policy = None;
                    txn_req.if_then = vec![
                        txn_put_pb(&tbid, &new_table_meta)?, /* tb_id -> tb_meta row access policy Some */
                        txn_put_pb(&ident, &RowAccessPolicyTableId {})?, // add policy_tb_id
                    ];
                }
                SetSecurityPolicyAction::Unset(old_policy) => {
                    // drop row access policy and table does not have row access policy
                    match (
                        table_meta.row_access_policy_columns_ids,
                        table_meta.row_access_policy,
                    ) {
                        (Some(policy), _) => {
                            if &policy.policy_id != old_policy {
                                return Ok(Err(TableError::AlterTableError {
                                    tenant: req.tenant.tenant_name().to_string(),
                                    context: format!(
                                        "Unknown row access policy {} on table",
                                        policy.policy_id
                                    ),
                                }));
                            } else {
                                new_table_meta.row_access_policy_columns_ids = None;
                                // Compatibility, can be deleted in the future
                                new_table_meta.row_access_policy = None;
                            }
                        }
                        // Compatibility, can be deleted in the future
                        (None, Some(_)) => {
                            new_table_meta.row_access_policy = None;
                        }
                        (None, None) => {
                            return Ok(Ok(SetTableRowAccessPolicyReply {}));
                        }
                    }

                    txn_req.if_then = vec![
                        txn_put_pb(&tbid, &new_table_meta)?, /* tb_id -> tb_meta row access policy None */
                        txn_del(&ident), // table drop row access policy, del policy_tb_id
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
    action: &SetSecurityPolicyAction,
    txn_req: &mut TxnRequest,
    tenant: &Tenant,
    table_id: u64,
) -> Result<(), KVAppError> {
    match action {
        SetSecurityPolicyAction::Set(policy_id, _column_ids) => {
            let id = MaskPolicyIdTableId {
                policy_id: *policy_id,
                table_id,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), id);
            txn_req
                .if_then
                .push(txn_put_pb(&ident, &MaskPolicyTableId)?);
        }
        SetSecurityPolicyAction::Unset(policy_id) => {
            let id = MaskPolicyIdTableId {
                policy_id: *policy_id,
                table_id,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), id);
            txn_req.if_then.push(txn_del(&ident));
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
