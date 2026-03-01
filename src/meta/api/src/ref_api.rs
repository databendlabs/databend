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

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TableAlreadyExists;
use databend_common_meta_app::app_error::TableSnapshotExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::BranchIdHistoryIdent;
use databend_common_meta_app::schema::CreateTableBranchReq;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::DroppedTableId;
use databend_common_meta_app::schema::GetTableBranchReq;
use databend_common_meta_app::schema::GetTableTagReq;
use databend_common_meta_app::schema::ListDroppedTableBranchesReq;
use databend_common_meta_app::schema::ListTableBranchMetaItem;
use databend_common_meta_app::schema::ListTableBranchesReq;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_meta_app::schema::ObjectTagIdRef;
use databend_common_meta_app::schema::ObjectTagIdRefIdent;
use databend_common_meta_app::schema::TableBranch;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdBranchName;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdTagName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TableTag;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UndropTableBranchReq;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use databend_meta_types::txn_op_response::Response;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::database_util::get_db_or_err;
use crate::fetch_id;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::serialize_struct;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_op_del;
use crate::txn_op_builder_util::txn_op_put;

/// Resolve table_id from TableNameIdent: db_name -> db_id, then (db_id, table_name) -> table_id.
async fn resolve_table_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &TableNameIdent,
    ctx: &str,
) -> Result<(u64, u64), KVAppError> {
    let tenant_dbname = name_ident.db_name_ident();

    let (seq_db_id, _db_meta) = get_db_or_err(
        kv_api,
        &tenant_dbname,
        format!("{}: {}", ctx, tenant_dbname.display()),
    )
    .await?;

    let db_id = *seq_db_id.data;

    let dbid_tbname = DBIdTableName {
        db_id,
        table_name: name_ident.table_name.clone(),
    };

    let (tb_id_seq, table_id) = get_u64_value(kv_api, &dbid_tbname).await?;
    if tb_id_seq > 0 {
        Ok((table_id, db_id))
    } else {
        Err(KVAppError::AppError(AppError::UnknownTable(
            UnknownTable::new(&name_ident.table_name, ctx),
        )))
    }
}

#[async_trait::async_trait]
pub trait RefApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    /// Create a table tag on the base table.
    ///
    /// A tag does not generate a new snapshot; it only records the snapshot location it points to.
    /// Tags can only be created on the base table.
    ///
    /// Writes: `__fd_table_tag/<table_id>/<tag_name> -> TableTag`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_tag(&self, req: CreateTableTagReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;
        let key_table_id = TableId { table_id };
        let key_tag = TableIdTagName::new(table_id, &req.tag_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Ensure the base table exists and has not changed.
            let seq_table_meta = self.get_pb(&key_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "create_table_tag"),
                )));
            };

            // Check seq matches caller's expectation.
            if req.seq.match_seq(&seq_table_meta).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        table_id,
                        req.seq,
                        seq_table_meta.seq,
                        "create_table_tag",
                    ),
                )));
            }

            // Check if tag already exists.
            let seq_tag = self.get_pb(&key_tag).await?;
            if seq_tag.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    TableAlreadyExists::new(
                        format!("table_id={table_id}/tag='{}'", req.tag_name),
                        "create_table_tag: tag already exists",
                    ),
                )));
            }

            let table_tag = TableTag {
                expire_at: req.expire_at,
                snapshot_loc: req.snapshot_loc.clone(),
            };

            let mut conditions = vec![
                // Table must not change.
                txn_cond_seq(&key_table_id, Eq, seq_table_meta.seq),
                // Tag must not already exist.
                txn_cond_seq(&key_tag, Eq, 0),
            ];

            if let Some(check) = req.lvt_check.as_ref() {
                let lvt_ident = LeastVisibleTimeIdent::new(&check.tenant, table_id);
                let res = self.get_pb(&lvt_ident).await?;
                let (lvt_seq, current_lvt) = match res {
                    Some(v) => (v.seq, Some(v.data)),
                    None => (0, None),
                };
                if let Some(current_lvt) = current_lvt {
                    if current_lvt.time > check.time {
                        return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                            TableSnapshotExpired::new(
                                table_id,
                                format!(
                                    "snapshot timestamp {:?} is older than the table's least visible time {:?}",
                                    check.time, current_lvt.time
                                ),
                            ),
                        )));
                    }
                }
                conditions.push(txn_cond_seq(&lvt_ident, Eq, lvt_seq));
            }

            let txn = TxnRequest::new(conditions, vec![txn_op_put(
                &key_tag,
                serialize_struct(&table_tag)?,
            )]);

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                table_id = table_id,
                tag_name :% =(&req.tag_name),
                succ = succ;
                "create_table_tag"
            );

            if succ {
                return Ok(());
            }
        }
    }

    /// Drop a table tag.
    ///
    /// Deletes: `__fd_table_tag/<table_id>/<tag_name>`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_tag(&self, req: DropTableTagReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;
        let tag_name = &req.tag_name;
        let key_tag = TableIdTagName::new(table_id, tag_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_tag = self.get_pb(&key_tag).await?;
            let tag_seq = seq_tag.as_ref().map_or(0, |s| s.seq);

            if tag_seq == 0 {
                // Tag does not exist, nothing to drop.
                return Ok(());
            }

            let txn = TxnRequest::new(vec![txn_cond_seq(&key_tag, Eq, tag_seq)], vec![txn_op_del(
                &key_tag,
            )]);

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                table_id = table_id,
                tag_name :% =(tag_name),
                succ = succ;
                "drop_table_tag"
            );

            if succ {
                return Ok(());
            }
        }
    }

    /// Get a table tag.
    ///
    /// Reads: `__fd_table_tag/<table_id>/<tag_name> -> TableTag`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_tag(
        &self,
        req: GetTableTagReq,
    ) -> Result<Option<SeqV<TableTag>>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;
        let tag_name = &req.tag_name;
        let key_tag = TableIdTagName::new(table_id, tag_name);
        let seq_tag = self.get_pb(&key_tag).await?;
        if let Some(tag) = &seq_tag {
            if let Some(expire_at) = tag.data.expire_at.as_ref() {
                if *expire_at <= Utc::now() {
                    return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                        TableSnapshotExpired::new(
                            table_id,
                            format!("table tag '{}' expired at {}", tag_name, expire_at),
                        ),
                    )));
                }
            }
        }
        Ok(seq_tag)
    }

    /// List table tags.
    ///
    /// Reads: `__fd_table_tag/<table_id>/<tag_name> -> TableTag`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_table_tags(
        &self,
        req: ListTableTagsReq,
    ) -> Result<Vec<(String, SeqV<TableTag>)>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_prefix = TableIdTagName::new(req.table_id, "");
        let dir = DirName::new(key_prefix);
        let entries = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;

        let now = Utc::now();
        let mut tags = Vec::with_capacity(entries.len());
        for (key, seq_tag) in entries {
            if !req.include_expired
                && seq_tag
                    .data
                    .expire_at
                    .as_ref()
                    .is_some_and(|expire_at| *expire_at <= now)
            {
                continue;
            }

            tags.push((key.tag_name, seq_tag));
        }

        Ok(tags)
    }

    /// Create a table branch.
    ///
    /// A branch is logically an independent table with a pointer back to the base table.
    /// The caller provides a pre-built TableMeta. This function will:
    /// - Filter out stale column_mask_policy and row_access_policy entries
    ///   whose column IDs no longer exist in the branch's schema.
    /// - Write policy reference KVs for the remaining valid policies.
    /// - Ownership and tag references are NOT inherited.
    ///
    /// Writes:
    /// - `__fd_table_branch/<table_id>/<branch_name> -> TableBranch`
    /// - `__fd_table_by_id/<branch_id> -> TableMeta` (branch's own TableMeta)
    /// - `__fd_branch_id_list/<table_id>/<branch_name> -> TableIdList`
    /// - Policy reference KVs for valid mask/row_access policies
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_branch(
        &self,
        req: CreateTableBranchReq,
    ) -> Result<Arc<TableInfo>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let branch_name = &req.branch_name;
        let table_id = req.table_id;
        let key_table_id = TableId { table_id };
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let key_branch_id_list = BranchIdHistoryIdent {
            table_id,
            branch_name: branch_name.clone(),
        };

        // Filter out stale policy entries based on the branch's schema.
        let mut branch_meta = req.table_meta.clone();
        let schema = &branch_meta.schema;

        // Remove column mask policies for columns not in the branch schema.
        branch_meta
            .column_mask_policy_columns_ids
            .retain(|col_id, _| !schema.is_column_deleted(*col_id));

        // Remove row access policy if any of its referenced columns are missing.
        if let Some(policy_map) = &branch_meta.row_access_policy_columns_ids {
            if policy_map
                .columns_ids
                .iter()
                .any(|col_id| schema.is_column_deleted(*col_id))
            {
                branch_meta.row_access_policy_columns_ids = None;
                branch_meta.row_access_policy = None;
            }
        }

        let mut maybe_branch_id: Option<u64> = None;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // 1. Ensure the base table exists and has not changed.
            let seq_table_meta = self.get_pb(&key_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "create_table_branch: base table"),
                )));
            };

            // Check seq matches caller's expectation.
            if req.seq.match_seq(&seq_table_meta).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        table_id,
                        req.seq,
                        seq_table_meta.seq,
                        "create_table_branch",
                    ),
                )));
            }

            // 2. Check if branch already exists.
            let seq_branch = self.get_pb(&key_branch).await?;
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    TableAlreadyExists::new(
                        format!("table_id={table_id}/branch='{}'", branch_name),
                        "create_table_branch: branch already exists",
                    ),
                )));
            }

            // 3. Get current branch id list.
            let seq_id_list = self.get_pb(&key_branch_id_list).await?;
            let id_list_seq = seq_id_list.as_ref().map_or(0, |s| s.seq);
            let mut id_list = seq_id_list.map(|s| s.data).unwrap_or_else(TableIdList::new);

            // 4. Generate branch_id (only once across retries).
            let branch_id = match maybe_branch_id {
                Some(id) => id,
                None => {
                    let id = fetch_id(self, IdGenerator::table_id()).await?;
                    maybe_branch_id = Some(id);
                    id
                }
            };

            let key_branch_table_id = TableId {
                table_id: branch_id,
            };

            // 5. Build TableBranch record.
            let table_branch = TableBranch {
                expire_at: req.expire_at,
                branch_id,
            };

            // 6. Append branch_id to id list.
            id_list.append(branch_id);

            let mut conditions = vec![
                // Base table must not change during branch creation.
                txn_cond_seq(&key_table_id, Eq, seq_table_meta.seq),
                // Branch must not already exist.
                txn_cond_seq(&key_branch, Eq, 0),
                // Branch id list must not change.
                txn_cond_seq(&key_branch_id_list, Eq, id_list_seq),
            ];

            if let Some(check) = req.lvt_check.as_ref() {
                let lvt_ident = LeastVisibleTimeIdent::new(&check.tenant, table_id);
                let res = self.get_pb(&lvt_ident).await?;
                let (lvt_seq, current_lvt) = match res {
                    Some(v) => (v.seq, Some(v.data)),
                    None => (0, None),
                };
                if let Some(current_lvt) = current_lvt {
                    if current_lvt.time > check.time {
                        return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                            TableSnapshotExpired::new(
                                table_id,
                                format!(
                                    "snapshot timestamp {:?} is older than the table's least visible time {:?}",
                                    check.time, current_lvt.time
                                ),
                            ),
                        )));
                    }
                }
                conditions.push(txn_cond_seq(&lvt_ident, Eq, lvt_seq));
            }

            let mut txn = TxnRequest::new(conditions, vec![
                // Write branch record: __fd_table_branch/<table_id>/<branch_name>
                txn_op_put(&key_branch, serialize_struct(&table_branch)?),
                // Write branch's TableMeta: __fd_table_by_id/<branch_id>
                txn_op_put(&key_branch_table_id, serialize_struct(&branch_meta)?),
                // Write branch id list: __fd_branch_id_list/<table_id>/<branch_name>
                txn_op_put(&key_branch_id_list, serialize_struct(&id_list)?),
            ]);

            // 7. Write column mask policy reference KVs.
            let policy_ids: HashSet<u64> = branch_meta
                .column_mask_policy_columns_ids
                .values()
                .map(|policy_map| policy_map.policy_id)
                .collect();

            for policy_id in policy_ids {
                let ident = MaskPolicyTableIdIdent::new_generic(
                    req.name_ident.tenant.clone(),
                    MaskPolicyIdTableId {
                        policy_id,
                        table_id: branch_id,
                    },
                );
                txn.if_then
                    .push(txn_op_put(&ident, serialize_struct(&MaskPolicyTableId)?));
            }

            // 8. Write row access policy reference KV.
            if let Some(policy_map) = &branch_meta.row_access_policy_columns_ids {
                let ident = RowAccessPolicyTableIdIdent::new_generic(
                    req.name_ident.tenant.clone(),
                    RowAccessPolicyIdTableId {
                        policy_id: policy_map.policy_id,
                        table_id: branch_id,
                    },
                );
                txn.if_then.push(txn_op_put(
                    &ident,
                    serialize_struct(&RowAccessPolicyTableId {})?,
                ));
            }

            let (succ, responses) = send_txn(self, txn).await?;

            debug!(
                table_id = table_id,
                branch_name :% =(branch_name),
                branch_id = branch_id,
                succ = succ;
                "create_table_branch"
            );

            if succ {
                let branch_seq = responses.last().and_then(|r| match &r.response {
                    Some(Response::Get(resp)) => resp.value.as_ref().map(|v| v.seq),
                    _ => None,
                });
                let Some(branch_seq) = branch_seq else {
                    return Err(KVAppError::AppError(AppError::UnknownTableId(
                        UnknownTableId::new(
                            branch_id,
                            "create_table_branch: missing table_id_seq in txn response",
                        ),
                    )));
                };

                return Ok(Arc::new(TableInfo {
                    ident: TableIdent {
                        table_id: branch_id,
                        seq: branch_seq,
                    },
                    desc: format!(
                        "'{}'.'{}'/'{}'",
                        req.name_ident.db_name, req.name_ident.table_name, branch_name,
                    ),
                    name: req.name_ident.table_name.clone(),
                    meta: branch_meta.clone(),
                    db_type: DatabaseType::NormalDB,
                    catalog_info: Default::default(),
                }));
            }
        }
    }

    /// Drop a table branch.
    ///
    /// Moves the branch's TableMeta from `__fd_table_by_id/<branch_id>`
    /// to `__fd_dropped_table_by_id/<branch_id>`, sets `drop_on`, and
    /// removes the branch record from `__fd_table_branch`.
    ///
    /// Also cleans up column_mask_policy, row_access_policy, ownership,
    /// and tag references associated with the branch.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_branch(&self, req: DropTableBranchReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let tenant = &req.tenant;
        let table_id = req.table_id;

        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // 1. Get the branch record to find branch_id.
            let seq_branch = self.get_pb(&key_branch).await?;
            let Some(seq_branch) = seq_branch else {
                // Branch does not exist, nothing to drop.
                return Ok(());
            };
            let branch_id = seq_branch.data.branch_id;

            // 2. Get the branch's TableMeta.
            let key_branch_table_id = TableId {
                table_id: branch_id,
            };
            let seq_table_meta = self.get_pb(&key_branch_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(branch_id, "drop_table_branch: branch table meta"),
                )));
            };

            // 3. Set drop_on and move to dropped space.
            let mut table_meta = seq_table_meta.data;
            table_meta.drop_on = Some(Utc::now());

            let key_dropped = DroppedTableId::new(branch_id);

            let mut txn = TxnRequest::new(
                vec![
                    txn_cond_seq(&key_branch, Eq, seq_branch.seq),
                    txn_cond_seq(&key_branch_table_id, Eq, seq_table_meta.seq),
                ],
                vec![
                    // Remove branch record.
                    txn_op_del(&key_branch),
                    // Remove from __fd_table_by_id.
                    txn_op_del(&key_branch_table_id),
                    // Write to __fd_dropped_table_by_id.
                    txn_op_put(&key_dropped, serialize_struct(&table_meta)?),
                ],
            );

            // 4. Clean up column mask policy references.
            let policy_ids: HashSet<u64> = table_meta
                .column_mask_policy_columns_ids
                .values()
                .map(|policy_map| policy_map.policy_id)
                .collect();

            txn.if_then.extend(policy_ids.into_iter().map(|policy_id| {
                txn_op_del(&MaskPolicyTableIdIdent::new_generic(
                    tenant.clone(),
                    MaskPolicyIdTableId {
                        policy_id,
                        table_id: branch_id,
                    },
                ))
            }));

            // 5. Clean up row access policy reference.
            if let Some(policy_map) = &table_meta.row_access_policy_columns_ids {
                txn.if_then
                    .push(txn_op_del(&RowAccessPolicyTableIdIdent::new_generic(
                        tenant.clone(),
                        RowAccessPolicyIdTableId {
                            policy_id: policy_map.policy_id,
                            table_id: branch_id,
                        },
                    )));
            }

            // 6. Todo: Clean up ownership.

            // 7. Clean up tag references.
            let taggable_object = TaggableObject::Table {
                table_id: branch_id,
            };
            let obj_tag_prefix = ObjectTagIdRefIdent::new_generic(
                tenant.clone(),
                ObjectTagIdRef::new(taggable_object.clone(), 0),
            );
            let obj_tag_dir = DirName::new(obj_tag_prefix);
            let strm = self.list_pb(ListOptions::unlimited(&obj_tag_dir)).await?;
            let tag_entries: Vec<_> = strm.try_collect().await?;
            for entry in tag_entries {
                let tag_id = entry.key.name().tag_id;
                let obj_ref_key = ObjectTagIdRefIdent::new_generic(
                    tenant.clone(),
                    ObjectTagIdRef::new(taggable_object.clone(), tag_id),
                );
                let tag_ref_key = TagIdObjectRefIdent::new_generic(
                    tenant.clone(),
                    TagIdObjectRef::new(tag_id, taggable_object.clone()),
                );
                txn.if_then.push(txn_op_del(&obj_ref_key));
                txn.if_then.push(txn_op_del(&tag_ref_key));
            }

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                table_id = table_id,
                branch_name :% =(branch_name),
                branch_id = branch_id,
                succ = succ;
                "drop_table_branch"
            );

            if succ {
                return Ok(());
            }
        }
    }

    /// Undrop a table branch.
    ///
    /// Moves the branch's TableMeta from `__fd_dropped_table_by_id/<branch_id>`
    /// back to `__fd_table_by_id/<branch_id>`, clears `drop_on`, and
    /// re-creates the branch record in `__fd_table_branch`.
    ///
    /// The branch_id is retrieved from the last entry in
    /// `__fd_branch_id_list/<table_id>/<branch_name>`.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch(&self, req: UndropTableBranchReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;
        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let key_branch_id_list = BranchIdHistoryIdent {
            table_id,
            branch_name: branch_name.clone(),
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // 1. Ensure branch is not currently active.
            let seq_branch = self.get_pb(&key_branch).await?;
            if seq_branch.is_some() {
                // Branch is already active, nothing to undrop.
                return Ok(());
            }

            // 2. Get the last branch_id from the id list.
            let seq_id_list = self.get_pb(&key_branch_id_list).await?;
            let Some(seq_id_list) = seq_id_list else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "undrop_table_branch: no branch id history"),
                )));
            };

            let Some(&branch_id) = seq_id_list.data.last() else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "undrop_table_branch: empty branch id list"),
                )));
            };

            // 3. Get the dropped TableMeta.
            let key_dropped = DroppedTableId::new(branch_id);
            let seq_dropped_meta = self.get_pb(&key_dropped).await?;
            let Some(seq_dropped_meta) = seq_dropped_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(branch_id, "undrop_table_branch: no dropped table meta"),
                )));
            };

            // 4. Clear drop_on.
            let mut table_meta = seq_dropped_meta.data;
            table_meta.drop_on = None;

            let key_branch_table_id = TableId {
                table_id: branch_id,
            };

            // 5. Re-create the branch record.
            let table_branch = TableBranch {
                expire_at: None,
                branch_id,
            };

            let txn = TxnRequest::new(
                vec![
                    // Branch must still not exist.
                    txn_cond_seq(&key_branch, Eq, 0),
                    // Dropped meta must not change.
                    txn_cond_seq(&key_dropped, Eq, seq_dropped_meta.seq),
                    // Branch id list must not change.
                    txn_cond_seq(&key_branch_id_list, Eq, seq_id_list.seq),
                ],
                vec![
                    // Remove from __fd_dropped_table_by_id.
                    txn_op_del(&key_dropped),
                    // Write back to __fd_table_by_id.
                    txn_op_put(&key_branch_table_id, serialize_struct(&table_meta)?),
                    // Re-create branch record.
                    txn_op_put(&key_branch, serialize_struct(&table_branch)?),
                ],
            );

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                table_id = table_id,
                branch_name :% =(branch_name),
                branch_id = branch_id,
                succ = succ;
                "undrop_table_branch"
            );

            if succ {
                return Ok(());
            }
        }
    }

    /// Get a table branch.
    ///
    /// 1. Reads `__fd_table_branch/<table_id>/<branch_name>` to get branch_id.
    /// 2. Reads `__fd_table_by_id/<branch_id>` to get the branch's TableMeta.
    /// 3. Returns `Arc<TableInfo>` like `get_table`.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_branch(&self, req: GetTableBranchReq) -> Result<Arc<TableInfo>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let tenant_dbname = req.name_ident.db_name_ident();

        let (seq_db_id, _db_meta) = get_db_or_err(
            self,
            &tenant_dbname,
            format!("{}: {}", "get_table_branch", tenant_dbname.display()),
        )
        .await?;

        let db_id = *seq_db_id.data;

        let dbid_tbname = DBIdTableName {
            db_id,
            table_name: req.name_ident.table_name.clone(),
        };

        let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
        if tb_id_seq == 0 {
            return Err(KVAppError::AppError(AppError::UnknownTable(
                UnknownTable::new(&req.name_ident.table_name, "get_table_branch"),
            )));
        }

        // 1. Get the branch record.
        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let seq_branch = self.get_pb(&key_branch).await?;
        let Some(seq_branch) = seq_branch else {
            return Err(KVAppError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(
                    table_id,
                    format!("get_table_branch: branch '{}' not found", branch_name),
                ),
            )));
        };
        if let Some(expire_at) = seq_branch.data.expire_at {
            if expire_at <= Utc::now() {
                return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                    TableSnapshotExpired::new(
                        table_id,
                        format!("table branch '{}' expired at {}", branch_name, expire_at),
                    ),
                )));
            }
        }
        let branch_id = seq_branch.data.branch_id;

        // 2. Get the branch's TableMeta.
        let key_branch_table_id = TableId {
            table_id: branch_id,
        };
        let seq_table_meta = self.get_pb(&key_branch_table_id).await?;
        let Some(seq_table_meta) = seq_table_meta else {
            return Err(KVAppError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(branch_id, "get_table_branch: branch table meta not found"),
            )));
        };

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id: branch_id,
                seq: seq_table_meta.seq,
            },
            desc: format!(
                "'{}'.'{}'/'{}'",
                req.name_ident.db_name, req.name_ident.table_name, branch_name,
            ),
            name: req.name_ident.table_name.clone(),
            meta: seq_table_meta.data,
            db_type: DatabaseType::NormalDB,
            catalog_info: Default::default(),
        };

        Ok(Arc::new(tb_info))
    }

    /// List table branches.
    ///
    /// Reads: `__fd_table_branch/<table_id>/<branch_name> -> TableBranch`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_table_branches(
        &self,
        req: ListTableBranchesReq,
    ) -> Result<Vec<ListTableBranchMetaItem>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_prefix = TableIdBranchName::new(req.table_id, "");
        let dir = DirName::new(key_prefix);
        let entries = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;

        let now = Utc::now();
        let mut branch_candidates = Vec::with_capacity(entries.len());
        for (key, seq_branch) in entries {
            if !req.include_expired
                && seq_branch
                    .data
                    .expire_at
                    .as_ref()
                    .is_some_and(|expire_at| *expire_at <= now)
            {
                continue;
            }

            branch_candidates.push((key.branch_name, seq_branch.data.branch_id));
        }

        if branch_candidates.is_empty() {
            return Ok(vec![]);
        }

        let branch_keys = branch_candidates
            .iter()
            .map(|(_, branch_id)| TableId {
                table_id: *branch_id,
            })
            .collect::<Vec<_>>();
        let branch_metas = self.get_pb_values_vec(branch_keys).await?;

        let mut branches = Vec::with_capacity(branch_metas.len());
        for ((branch_name, branch_id), branch_meta) in
            branch_candidates.into_iter().zip(branch_metas.into_iter())
        {
            let Some(branch_meta) = branch_meta else {
                continue;
            };
            branches.push((
                branch_name,
                TableId {
                    table_id: branch_id,
                },
                branch_meta,
            ));
        }

        Ok(branches)
    }

    /// List dropped table branches.
    ///
    /// Lists branch history under `__fd_branch_id_list/<table_id>/`, excludes active branches
    /// from `__fd_table_branch/<table_id>/`, and then loads dropped meta by
    /// `__fd_dropped_table_by_id/<branch_id>`.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_dropped_table_branches(
        &self,
        req: ListDroppedTableBranchesReq,
    ) -> Result<Vec<ListTableBranchMetaItem>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;

        let key_branch_history = BranchIdHistoryIdent {
            table_id,
            branch_name: "".to_string(),
        };
        let history_dir = DirName::new(key_branch_history);
        let histories = self
            .list_pb_vec(ListOptions::unlimited(&history_dir))
            .await?;

        let key_active_branch = TableIdBranchName::new(table_id, "");
        let active_dir = DirName::new(key_active_branch);
        let active_branches = self
            .list_pb_vec(ListOptions::unlimited(&active_dir))
            .await?;
        let active_names = active_branches
            .into_iter()
            .map(|(key, _)| key.branch_name)
            .collect::<HashSet<_>>();

        let mut dropped_candidates = Vec::with_capacity(histories.len());
        for (key, seq_id_list) in histories {
            if active_names.contains(&key.branch_name) {
                continue;
            }

            let Some(branch_id) = seq_id_list.data.last().copied() else {
                continue;
            };

            dropped_candidates.push((key.branch_name, branch_id));
        }

        if dropped_candidates.is_empty() {
            return Ok(vec![]);
        }

        let dropped_keys = dropped_candidates
            .iter()
            .map(|(_, branch_id)| DroppedTableId::new(*branch_id))
            .collect::<Vec<_>>();
        let dropped_metas = self.get_pb_values_vec(dropped_keys).await?;

        let mut dropped_branches = Vec::with_capacity(dropped_metas.len());
        for ((branch_name, branch_id), dropped_meta) in dropped_candidates
            .into_iter()
            .zip(dropped_metas.into_iter())
        {
            let Some(dropped_meta) = dropped_meta else {
                continue;
            };

            dropped_branches.push((
                branch_name,
                TableId {
                    table_id: branch_id,
                },
                dropped_meta,
            ));
        }

        Ok(dropped_branches)
    }
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
