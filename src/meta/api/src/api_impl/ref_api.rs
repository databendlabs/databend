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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::ReferenceAlreadyExists;
use databend_common_meta_app::app_error::ReferenceExpired;
use databend_common_meta_app::app_error::TableSnapshotExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UnknownReference;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::primitive::Id;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::schema::AutoIncrementStorageValue;
use databend_common_meta_app::schema::CommitTableBranchMetaReq;
use databend_common_meta_app::schema::CreateTableBranchReply;
use databend_common_meta_app::schema::CreateTableBranchReq;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::DroppedBranchIdent;
use databend_common_meta_app::schema::DroppedBranchMeta;
use databend_common_meta_app::schema::GetTableBranchReq;
use databend_common_meta_app::schema::GetTableTagReq;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_meta_app::schema::ObjectTagIdRef;
use databend_common_meta_app::schema::ObjectTagIdRefIdent;
use databend_common_meta_app::schema::STAGED_BRANCH_TIMEOUT;
use databend_common_meta_app::schema::StagedBranch;
use databend_common_meta_app::schema::StagedBranchIdent;
use databend_common_meta_app::schema::TableBranch;
use databend_common_meta_app::schema::TableBranchMeta;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdBranchName;
use databend_common_meta_app::schema::TableIdTagName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::TableTag;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UndropTableBranchByIdReq;
use databend_common_meta_app::schema::UndropTableBranchReq;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::ConditionResult::Eq;
use databend_meta_client::types::MatchSeqExt;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnCondition;
use databend_meta_client::types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;
use log::info;
use log::warn;

use crate::api_impl::schema_api::restore_policy_references_on_undrop;
use crate::database_util::get_db_or_err;
use crate::fetch_id;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_del;
use crate::txn_put_pb;
use crate::util::IdempotentKVTxnResponse;
use crate::util::IdempotentKVTxnSender;

async fn build_lvt_condition(
    kv_api: &(impl KVPbApi<Error = MetaError> + ?Sized),
    table_id: u64,
    lvt_check: &TableLvtCheck,
) -> Result<TxnCondition, KVAppError> {
    let lvt_ident = LeastVisibleTimeIdent::new(&lvt_check.tenant, table_id);
    let res = kv_api.get_pb(&lvt_ident).await?;
    let (lvt_seq, current_lvt) = match res {
        Some(v) => (v.seq, Some(v.data)),
        None => (0, None),
    };

    if let Some(current_lvt) = current_lvt {
        if current_lvt.time > lvt_check.time {
            return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                TableSnapshotExpired::new(
                    table_id,
                    format!(
                        "snapshot timestamp {:?} is older than the table's least visible time {:?}",
                        lvt_check.time, current_lvt.time
                    ),
                ),
            )));
        }
    }

    Ok(txn_cond_seq(&lvt_ident, Eq, lvt_seq))
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
        let table_tag = TableTag {
            expire_at: req.expire_at,
            snapshot_loc: req.snapshot_loc.clone(),
        };

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
            // Reject tags on soft-deleted tables so dropped-table cleanup cannot race with
            // a late tag creation that still sees the old table seq.
            if seq_table_meta.data.drop_on.is_some() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "create_table_tag"),
                )));
            }
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
                    ReferenceAlreadyExists::new(format!("Tag '{}' already exists", req.tag_name)),
                )));
            }

            let conditions = vec![
                // Table must not change.
                txn_cond_seq(&key_table_id, Eq, seq_table_meta.seq),
                // Tag must not already exist.
                txn_cond_seq(&key_tag, Eq, 0),
                // Check table lvt.
                build_lvt_condition(self, table_id, &req.lvt_check).await?,
            ];

            let txn = TxnRequest::new(conditions, vec![txn_put_pb(&key_tag, &table_tag)?]);
            let (succ, _responses) = send_txn(self, txn).await?;
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

        let key_tag = TableIdTagName::new(req.table_id, &req.tag_name);
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_tag = self.get_pb(&key_tag).await?;
            let Some(seq_tag) = seq_tag else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("Unknown tag '{}'", req.tag_name),
                ))));
            };
            if req.seq.match_seq(&seq_tag).is_err() {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Tag '{}' seq mismatched: expect {}, current {}",
                        req.tag_name, req.seq, seq_tag.seq
                    ),
                ))));
            }

            let txn = TxnRequest::new(vec![txn_cond_seq(&key_tag, Eq, seq_tag.seq)], vec![
                txn_del(&key_tag),
            ]);
            let (succ, _responses) = send_txn(self, txn).await?;
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

        let key_tag = TableIdTagName::new(req.table_id, &req.tag_name);
        let seq_tag = self.get_pb(&key_tag).await?;
        if !req.include_expired {
            if let Some(tag) = &seq_tag {
                if let Some(expire_at) = tag.data.expire_at.as_ref() {
                    if *expire_at <= Utc::now() {
                        return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                            format!("Tag '{}' expired at '{}'", req.tag_name, expire_at),
                        ))));
                    }
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

    /// Reserve a staged branch id under the base table.
    ///
    /// Phase 1 writes only `__fd_staged_branch/<base_table_id>/<branch_id> -> StagedBranch`.
    /// The caller then writes the branch snapshot object, and phase 2 publishes the visible
    /// `__fd_table_branch/<base_table_id>/<branch_name>` entry together with the branch table meta.
    ///
    /// This keeps the branch name hidden until the snapshot object is durable.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_branch(
        &self,
        req: CreateTableBranchReq,
    ) -> Result<CreateTableBranchReply, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let branch_name = &req.branch_name;
        let key_source_table_id = TableId {
            table_id: req.source_table_id,
        };
        let key_branch = TableIdBranchName::new(req.table_id, branch_name);

        let mut maybe_branch_id = None;
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let Some(seq_source_table_meta) = self.get_pb(&key_source_table_id).await? else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.source_table_id, "create_table_branch: source table"),
                )));
            };
            if req.seq.match_seq(&seq_source_table_meta).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.source_table_id,
                        req.seq,
                        seq_source_table_meta.seq,
                        "create_table_branch",
                    ),
                )));
            }

            let seq_branch = self.get_pb(&key_branch).await?;
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!("Branch '{}' already exists", branch_name)),
                )));
            }

            // Reuse the generated branch id across retries so a failed txn does not keep
            // allocating new table ids.
            let branch_id = match maybe_branch_id {
                Some(id) => id,
                None => {
                    let id = fetch_id(self, IdGenerator::table_id()).await?;
                    maybe_branch_id = Some(id);
                    id
                }
            };
            let key_staged_branch = StagedBranchIdent::new(req.table_id, branch_id);

            let mut conditions = vec![
                txn_cond_seq(&key_source_table_id, Eq, seq_source_table_meta.seq),
                txn_cond_seq(&key_branch, Eq, 0),
                txn_cond_seq(&key_staged_branch, Eq, 0),
            ];
            if let Some(lvt_check) = req.lvt_check.as_ref() {
                conditions.push(build_lvt_condition(self, req.source_table_id, lvt_check).await?);
            }

            let staged_branch = StagedBranch {
                create_on: Utc::now(),
                cleanup_marked: false,
            };
            let txn = TxnRequest::new(conditions, vec![txn_put_pb(
                &key_staged_branch,
                &staged_branch,
            )?]);

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                table_id = req.table_id,
                branch_name :% =(branch_name),
                branch_id = branch_id,
                succ = succ;
                "create_table_branch"
            );

            if succ {
                let mut auto_increment_start_vals = BTreeMap::new();
                for table_field in seq_source_table_meta.data.schema.fields() {
                    let Some(auto_increment_expr) = table_field.auto_increment_expr() else {
                        continue;
                    };

                    let source_ai_key =
                        AutoIncrementKey::new(req.source_table_id, table_field.column_id());
                    let source_ai_ident =
                        AutoIncrementStorageIdent::new_generic(&req.tenant, source_ai_key);
                    let start_value = match self.get_pb(&source_ai_ident).await? {
                        Some(seq_v) => seq_v.data.into_inner().0,
                        None => auto_increment_expr.start,
                    };
                    auto_increment_start_vals.insert(table_field.column_id(), start_value);
                }
                return Ok(CreateTableBranchReply {
                    branch_id,
                    auto_increment_start_vals,
                });
            }
        }
    }

    /// Publish a previously staged branch generation under its real branch name.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn commit_table_branch_meta(
        &self,
        req: CommitTableBranchMetaReq,
    ) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_table_id = TableId {
            table_id: req.table_id,
        };
        let key_branch = TableIdBranchName::new(req.table_id, &req.branch_name);
        let key_staged_branch = StagedBranchIdent::new(req.table_id, req.branch_id);
        let key_branch_table_id = TableId {
            table_id: req.branch_id,
        };

        let txn_sender = IdempotentKVTxnSender::new();
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let Some(seq_table_meta) = self.get_pb(&key_table_id).await? else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "commit_table_branch_meta: base table"),
                )));
            };
            if seq_table_meta.data.drop_on.is_some() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(
                        req.table_id,
                        "commit_table_branch_meta: base table is dropped",
                    ),
                )));
            }

            let seq_branch = self.get_pb(&key_branch).await?;
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!(
                        "Branch '{}' already committed",
                        req.branch_name
                    )),
                )));
            }

            let seq_staged_branch = self.get_pb(&key_staged_branch).await?;
            let Some(seq_staged_branch) = seq_staged_branch else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("Unknown branch '{}'", req.branch_name),
                ))));
            };
            if seq_staged_branch.data.cleanup_marked {
                return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                    format!(
                        "Staged branch '{}' is already marked for cleanup",
                        req.branch_name
                    ),
                ))));
            }
            if seq_staged_branch.data.create_on + STAGED_BRANCH_TIMEOUT <= Utc::now() {
                return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                    format!(
                        "Staged branch '{}' timed out and can no longer be published",
                        req.branch_name
                    ),
                ))));
            }

            let table_branch = TableBranch {
                expire_at: req.expire_at,
                branch_id: req.branch_id,
            };

            let mut txn_req = TxnRequest::new(
                vec![
                    txn_cond_seq(&key_table_id, Eq, seq_table_meta.seq),
                    txn_cond_seq(&key_branch, Eq, 0),
                    txn_cond_seq(&key_staged_branch, Eq, seq_staged_branch.seq),
                    txn_cond_seq(&key_branch_table_id, Eq, 0),
                ],
                vec![
                    txn_put_pb(&key_branch_table_id, &req.new_table_meta)?,
                    txn_put_pb(&key_branch, &table_branch)?,
                    txn_del(&key_staged_branch),
                ],
            );

            let policy_ids: HashSet<u64> = req
                .new_table_meta
                .column_mask_policy_columns_ids
                .values()
                .map(|policy_map| policy_map.policy_id)
                .collect();
            for policy_id in policy_ids {
                let ident =
                    MaskPolicyTableIdIdent::new_generic(req.tenant.clone(), MaskPolicyIdTableId {
                        policy_id,
                        table_id: req.branch_id,
                    });
                txn_req
                    .if_then
                    .push(txn_put_pb(&ident, &MaskPolicyTableId)?);
            }

            if let Some(policy_map) = &req.new_table_meta.row_access_policy_columns_ids {
                let ident = RowAccessPolicyTableIdIdent::new_generic(
                    req.tenant.clone(),
                    RowAccessPolicyIdTableId {
                        policy_id: policy_map.policy_id,
                        table_id: req.branch_id,
                    },
                );
                txn_req
                    .if_then
                    .push(txn_put_pb(&ident, &RowAccessPolicyTableId {})?);
            }

            for (column_id, start_value) in &req.auto_increment_start_vals {
                let auto_increment_key = AutoIncrementKey::new(req.branch_id, *column_id);
                let storage_ident =
                    AutoIncrementStorageIdent::new_generic(&req.tenant, auto_increment_key);
                let storage_value = Id::new_typed(AutoIncrementStorageValue(*start_value));

                txn_req
                    .if_then
                    .push(txn_put_pb(&storage_ident, &storage_value)?);
            }

            let txn_response = txn_sender.send_txn(self, txn_req).await?;
            let succ = match txn_response {
                IdempotentKVTxnResponse::Success(_) => true,
                IdempotentKVTxnResponse::AlreadyCommitted => {
                    info!(
                        "Transaction ID {} exists, the corresponding commit_table_branch_meta transaction has been executed successfully",
                        txn_sender.get_txn_id()
                    );
                    true
                }
                IdempotentKVTxnResponse::Failed(_) => false,
            };

            debug!(
                table_id = req.table_id,
                branch_name :% =(&req.branch_name),
                branch_id = req.branch_id,
                succ = succ;
                "commit_table_branch_meta"
            );

            if succ {
                return Ok(());
            }
        }
    }

    /// Mark staged branches under a base table so a later cleanup step can remove them.
    ///
    /// `cleanup_at` is the evaluation timestamp for staged cleanup eligibility.
    /// `Some(ts)` marks only staged branches whose `create_on + STAGED_BRANCH_TIMEOUT <= ts`;
    /// `None` marks every staged branch under the base table, which is used by dropped-table
    /// cleanup as a final fallback.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn mark_staged_branches_for_cleanup(
        &self,
        table_id: u64,
        cleanup_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<StagedBranchIdent>, KVAppError> {
        debug!(table_id = table_id, cleanup_at :? =(cleanup_at); "RefApi: {}", func_name!());

        let staged_prefix = StagedBranchIdent::new(table_id, 0);
        let staged_dir = DirName::new(staged_prefix);
        let staged_entries = self
            .list_pb_vec(ListOptions::unlimited(&staged_dir))
            .await?;

        let mut marked = Vec::new();
        for (key, seq_staged) in staged_entries {
            if seq_staged.data.cleanup_marked {
                marked.push(key);
                continue;
            }

            if cleanup_at.is_some_and(|cleanup_at| {
                seq_staged.data.create_on + STAGED_BRANCH_TIMEOUT > cleanup_at
            }) {
                continue;
            }

            let mut staged = seq_staged.data;
            staged.cleanup_marked = true;
            let txn = TxnRequest::new(vec![txn_cond_seq(&key, Eq, seq_staged.seq)], vec![
                txn_put_pb(&key, &staged)?,
            ]);
            let (succ, _) = send_txn(self, txn).await?;
            if succ {
                marked.push(key);
            }
        }

        Ok(marked)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_staged_table_branch(
        &self,
        table_id: u64,
        branch_id: u64,
    ) -> Result<(), KVAppError> {
        debug!(table_id = table_id, branch_id = branch_id; "RefApi: {}", func_name!());

        let key_staged_branch = StagedBranchIdent::new(table_id, branch_id);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_staged_branch = self.get_pb(&key_staged_branch).await?;
            let Some(seq_staged_branch) = seq_staged_branch else {
                return Ok(());
            };

            let mut txn = TxnRequest::default();
            txn_delete_exact(&mut txn, &key_staged_branch, seq_staged_branch.seq);

            let (succ, _) = send_txn(self, txn).await?;
            if succ {
                return Ok(());
            }
        }
    }

    /// Drop the visible branch entry and mark its underlying branch table as dropped.
    ///
    /// The branch table id is kept for history/vacuum handling, but the branch name becomes
    /// unavailable immediately after the visible `TableBranch` key is removed.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_branch(&self, req: DropTableBranchReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let tenant = &req.tenant;
        let table_id = req.table_id;
        let expected_branch_id = req.branch_id;
        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_branch = self.get_pb(&key_branch).await?;
            let Some(seq_branch) = seq_branch else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("Unknown branch '{}'", branch_name),
                ))));
            };
            let branch_id = seq_branch.data.branch_id;
            if branch_id != expected_branch_id {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Branch '{}' generation changed: expect branch_id {}, current {}",
                        branch_name, expected_branch_id, branch_id
                    ),
                ))));
            }

            let key_branch_table_id = TableId {
                table_id: expected_branch_id,
            };
            let seq_table_meta = self.get_pb(&key_branch_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(expected_branch_id, "drop_table_branch: branch table meta"),
                )));
            };
            let mut table_meta = seq_table_meta.data;
            // Keep the branch table meta for history/vacuum flows, but mark it dropped once the
            // visible branch name is removed.
            let drop_on = Utc::now();
            table_meta.drop_on = Some(drop_on);

            // Write dropped-branch entry so vacuum/undrop can discover this branch.
            let key_dropped_branch =
                DroppedBranchIdent::new(table_id, branch_name, expected_branch_id);
            let dropped_meta = DroppedBranchMeta {
                drop_on,
                expire_at: seq_branch.data.expire_at,
            };

            let mut txn = TxnRequest::new(
                vec![
                    txn_cond_seq(&key_branch, Eq, seq_branch.seq),
                    txn_cond_seq(&key_branch_table_id, Eq, seq_table_meta.seq),
                ],
                vec![
                    txn_del(&key_branch),
                    txn_put_pb(&key_branch_table_id, &table_meta)?,
                    txn_put_pb(&key_dropped_branch, &dropped_meta)?,
                ],
            );

            let policy_ids: HashSet<u64> = table_meta
                .column_mask_policy_columns_ids
                .values()
                .map(|policy_map| policy_map.policy_id)
                .collect();
            txn.if_then.extend(policy_ids.into_iter().map(|policy_id| {
                txn_del(&MaskPolicyTableIdIdent::new_generic(
                    tenant.clone(),
                    MaskPolicyIdTableId {
                        policy_id,
                        table_id: expected_branch_id,
                    },
                ))
            }));

            if let Some(policy_map) = &table_meta.row_access_policy_columns_ids {
                txn.if_then
                    .push(txn_del(&RowAccessPolicyTableIdIdent::new_generic(
                        tenant.clone(),
                        RowAccessPolicyIdTableId {
                            policy_id: policy_map.policy_id,
                            table_id: expected_branch_id,
                        },
                    )));
            }

            let taggable_object = TaggableObject::Table {
                table_id: expected_branch_id,
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
                txn.if_then.push(txn_del(&obj_ref_key));
                txn.if_then.push(txn_del(&tag_ref_key));
            }

            let (succ, _responses) = send_txn(self, txn).await?;
            if succ {
                return Ok(());
            }
        }
    }

    /// Resolve a visible branch name to its current branch table metadata.
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
        // Branch names are resolved under the *currently visible* base table id of `db.table`.
        // This is intentional: branches are treated as refs of the current base table namespace,
        // not as independently addressable objects. If the base table is dropped or replaced and
        // `db.table` points to a new table id, previously created branches under the old base
        // table id become unreachable by design.
        let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
        if tb_id_seq == 0 {
            return Err(KVAppError::AppError(AppError::UnknownTable(
                UnknownTable::new(&req.name_ident.table_name, "get_table_branch"),
            )));
        }

        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let seq_branch = self.get_pb(&key_branch).await?;
        let Some(seq_branch) = seq_branch else {
            return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                format!("Unknown branch '{}'", branch_name),
            ))));
        };
        // Expiration is enforced at branch name resolution time; the underlying branch table
        // may still exist in metadata until vacuum/gc handles it.
        if !req.include_expired {
            if let Some(expire_at) = seq_branch.data.expire_at {
                if expire_at <= Utc::now() {
                    return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                        format!("Branch '{}' expired at {}", branch_name, expire_at),
                    ))));
                }
            }
        }

        let branch_id = seq_branch.data.branch_id;
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
            // This meta API is currently reached only through the default catalog's branch lookup
            // path, so the returned TableInfo stays aligned with that default-catalog-only contract.
            catalog_info: Default::default(),
        };

        Ok(Arc::new(tb_info))
    }

    /// List table branches.
    ///
    /// Reads: `__fd_table_branch/<table_id>/<branch_name> -> TableBranch`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_table_branches(&self, table_id: u64) -> Result<Vec<TableBranchMeta>, KVAppError> {
        debug!(table_id = table_id; "RefApi: {}", func_name!());

        let key_prefix = TableIdBranchName::new(table_id, "");
        let dir = DirName::new(key_prefix);
        let entries = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;

        let mut branch_candidates = Vec::with_capacity(entries.len());
        for (key, seq_branch) in entries {
            branch_candidates.push((
                key.branch_name,
                seq_branch.data.branch_id,
                seq_branch.data.expire_at,
            ));
        }

        if branch_candidates.is_empty() {
            return Ok(vec![]);
        }

        let branch_keys = branch_candidates
            .iter()
            .map(|(_, branch_id, _)| TableId {
                table_id: *branch_id,
            })
            .collect::<Vec<_>>();
        let branch_metas = self.get_pb_values_vec(branch_keys).await?;

        let mut branches = Vec::with_capacity(branch_metas.len());
        for ((branch_name, branch_id, expire_at), branch_meta) in
            branch_candidates.into_iter().zip(branch_metas.into_iter())
        {
            let Some(branch_meta) = branch_meta else {
                continue;
            };
            if branch_meta.data.drop_on.is_some() {
                continue;
            }
            branches.push(TableBranchMeta {
                branch_name,
                branch_id: TableId {
                    table_id: branch_id,
                },
                branch_meta,
                expire_at,
            });
        }

        Ok(branches)
    }

    /// List all branch generations (active and dropped) for a base table.
    ///
    /// Reads active branches from `__fd_table_branch/<table_id>/` and dropped branches from
    /// `__fd_dropped_branch/<table_id>/`. Then batch-reads branch table metas from
    /// `__fd_table_by_id/<branch_id>`.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_history_table_branches(
        &self,
        req: ListHistoryTableBranchesReq,
    ) -> Result<Vec<TableBranchMeta>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;

        // 1. List active branches: __fd_table_branch/<table_id>/
        let key_active_branch = TableIdBranchName::new(table_id, "");
        let active_dir = DirName::new(key_active_branch);
        let active_branches = self
            .list_pb_vec(ListOptions::unlimited(&active_dir))
            .await?;
        let active_map: HashMap<u64, (String, Option<DateTime<Utc>>)> = active_branches
            .iter()
            .map(|(key, branch)| {
                (
                    branch.data.branch_id,
                    (key.branch_name.clone(), branch.data.expire_at),
                )
            })
            .collect();

        // 2. List dropped branches: __fd_dropped_branch/<table_id>/
        let dropped_prefix = DroppedBranchIdent::new(table_id, "dummy", 0);
        let dropped_dir = DirName::new_with_level(dropped_prefix, 2);
        let dropped_branches = self
            .list_pb_vec(ListOptions::unlimited(&dropped_dir))
            .await?;

        // 3. Collect all unique branch_ids
        let mut branch_id_set: HashSet<u64> = active_map.keys().copied().collect();
        let mut dropped_map: HashMap<u64, (String, DateTime<Utc>)> = HashMap::new();

        for (key, seq_dropped) in &dropped_branches {
            branch_id_set.insert(key.branch_id);
            dropped_map.insert(
                key.branch_id,
                (key.branch_name.clone(), seq_dropped.data.drop_on),
            );
        }

        if branch_id_set.is_empty() {
            return Ok(vec![]);
        }

        // 4. Batch read branch table metas
        let branch_keys: Vec<TableId> = branch_id_set
            .iter()
            .map(|&id| TableId { table_id: id })
            .collect();
        let branch_metas = self.get_pb_values_vec(branch_keys.clone()).await?;

        // 5. Build results, filtering by retention_boundary
        let mut branches = Vec::with_capacity(branch_metas.len());
        for (branch_id, branch_meta) in branch_keys.into_iter().zip(branch_metas.into_iter()) {
            let Some(branch_meta) = branch_meta else {
                continue;
            };

            // Filter branches whose effective delete time is already beyond the retention window.
            if let Some(retention_boundary) = req.retention_boundary {
                if let Some((_, drop_on)) = dropped_map.get(&branch_id.table_id) {
                    if *drop_on < retention_boundary {
                        continue;
                    }
                } else if let Some((_, expire_at)) = active_map.get(&branch_id.table_id) {
                    if expire_at.is_some_and(|expire_at| expire_at < retention_boundary) {
                        continue;
                    }
                }
            }

            let (branch_name, expire_at) =
                if let Some((name, expire_at)) = active_map.get(&branch_id.table_id) {
                    (name.clone(), *expire_at)
                } else if let Some((name, _)) = dropped_map.get(&branch_id.table_id) {
                    (name.clone(), None)
                } else {
                    // unreachable.
                    warn!(
                        "branch_id {} not found in active_map or dropped_map, skipping",
                        branch_id.table_id
                    );
                    continue;
                };

            branches.push(TableBranchMeta {
                branch_name,
                branch_id,
                branch_meta,
                expire_at,
            });
        }

        Ok(branches)
    }

    /// Undrop a table branch by name.
    ///
    /// If multiple dropped branches share the same name, returns an error
    /// asking the user to specify a branch_id via UNDROP BRANCH ... IDENTIFIER(<id>).
    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch(&self, req: UndropTableBranchReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let tenant = &req.tenant;
        let table_id = req.table_id;
        let branch_name = &req.branch_name;
        let retention_boundary = req.retention_boundary;

        // List dropped branches with this name: __fd_dropped_branch/<table_id>/<branch_name>/
        let dropped_prefix = DroppedBranchIdent::new(table_id, branch_name, 0);
        let dropped_dir = DirName::new(dropped_prefix);
        let dropped_entries = self
            .list_pb_vec(ListOptions::unlimited(&dropped_dir))
            .await?;

        if dropped_entries.is_empty() {
            return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                format!("No dropped branch '{}' found", branch_name),
            ))));
        }

        let retained_entries = dropped_entries
            .into_iter()
            .filter(|(_, meta)| meta.data.drop_on >= retention_boundary)
            .collect::<Vec<_>>();

        if retained_entries.is_empty() {
            return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                format!(
                    "Dropped branch '{}' expired before {} and can no longer be undropped",
                    branch_name, retention_boundary
                ),
            ))));
        }

        if retained_entries.len() > 1 {
            let ids: Vec<String> = retained_entries
                .iter()
                .map(|(k, _)| k.branch_id.to_string())
                .collect();
            return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                format!(
                    "Multiple dropped branches with name '{}' found (ids: {}), use UNDROP BRANCH ... IDENTIFIER(<branch_id>) to specify",
                    branch_name,
                    ids.join(", ")
                ),
            ))));
        }

        let (dropped_key, _dropped_meta) = retained_entries.into_iter().next().unwrap();
        let branch_id = dropped_key.branch_id;

        self.do_undrop_table_branch(
            tenant,
            table_id,
            branch_name,
            branch_id,
            retention_boundary,
            req.new_expire_at,
        )
        .await
    }

    /// Undrop a table branch by explicit branch_id.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch_by_id(
        &self,
        req: UndropTableBranchByIdReq,
    ) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        self.do_undrop_table_branch(
            &req.tenant,
            req.table_id,
            &req.branch_name,
            req.branch_id,
            req.retention_boundary,
            req.new_expire_at,
        )
        .await
    }

    /// Shared implementation for undrop branch (by name or by id).
    async fn do_undrop_table_branch(
        &self,
        tenant: &Tenant,
        table_id: u64,
        branch_name: &str,
        branch_id: u64,
        retention_boundary: DateTime<Utc>,
        new_expire_at: Option<DateTime<Utc>>,
    ) -> Result<(), KVAppError> {
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let key_dropped = DroppedBranchIdent::new(table_id, branch_name, branch_id);
        let key_branch_table_id = TableId {
            table_id: branch_id,
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // 1. Visible entry must not exist (branch name must be free).
            let seq_branch = self.get_pb(&key_branch).await?;
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!(
                        "Branch '{}' already exists, cannot undrop",
                        branch_name
                    )),
                )));
            }

            // 2. Dropped entry must exist.
            let seq_dropped = self.get_pb(&key_dropped).await?;
            let Some(seq_dropped) = seq_dropped else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Dropped branch '{}' (id={}) not found",
                        branch_name, branch_id
                    ),
                ))));
            };
            if seq_dropped.data.drop_on < retention_boundary {
                return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                    format!(
                        "Dropped branch '{}' expired at {} and can no longer be undropped",
                        branch_name, seq_dropped.data.drop_on
                    ),
                ))));
            }

            let now = Utc::now();
            let restored_expire_at = match (new_expire_at, seq_dropped.data.expire_at) {
                (Some(expire_at), _) => {
                    if expire_at <= now {
                        return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                            format!(
                                "Can not undrop branch '{}' with expire_at {} in the past",
                                branch_name, expire_at
                            ),
                        ))));
                    }
                    Some(expire_at)
                }
                (None, Some(expire_at)) if expire_at <= now => {
                    return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                        format!(
                            "Dropped branch '{}' expired at {} and must be undropped with a new expiration",
                            branch_name, expire_at
                        ),
                    ))));
                }
                (None, expire_at) => expire_at,
            };

            // 3. Branch table meta must exist.
            let seq_table_meta = self.get_pb(&key_branch_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(branch_id, "undrop_table_branch: branch table meta"),
                )));
            };
            let mut table_meta = seq_table_meta.data;

            // 4. Restore policy references.
            let (policy_restore_ops, policy_restore_conditions) =
                restore_policy_references_on_undrop(self, tenant, branch_id, &mut table_meta)
                    .await
                    .map_err(KVAppError::from)?;

            // 5. Clear drop_on, build txn.
            table_meta.drop_on = None;

            let visible_branch = TableBranch {
                expire_at: restored_expire_at,
                branch_id,
            };

            let txn = TxnRequest::new(
                [
                    vec![
                        // Visible entry must still be absent.
                        txn_cond_seq(&key_branch, Eq, 0),
                        // Dropped entry must not change.
                        txn_cond_seq(&key_dropped, Eq, seq_dropped.seq),
                        // Branch table meta must not change.
                        txn_cond_seq(&key_branch_table_id, Eq, seq_table_meta.seq),
                    ],
                    policy_restore_conditions,
                ]
                .concat(),
                [
                    vec![
                        // Restore visible entry.
                        txn_put_pb(&key_branch, &visible_branch)?,
                        // Clear drop_on in branch table meta.
                        txn_put_pb(&key_branch_table_id, &table_meta)?,
                        // Delete dropped entry.
                        txn_del(&key_dropped),
                    ],
                    policy_restore_ops,
                ]
                .concat(),
            );

            let (succ, _responses) = send_txn(self, txn).await?;
            if succ {
                return Ok(());
            }
        }
    }
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
