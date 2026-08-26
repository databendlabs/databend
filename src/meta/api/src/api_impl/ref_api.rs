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

use std::collections::HashMap;
use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::ReferenceAlreadyExists;
use databend_common_meta_app::app_error::ReferenceExpired;
use databend_common_meta_app::app_error::TableSnapshotExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UndropTableRetentionGuard;
use databend_common_meta_app::app_error::UnknownReference;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::schema::AutoIncrementStorageValue;
use databend_common_meta_app::schema::BranchIdToName;
use databend_common_meta_app::schema::CreateTableBranchReq;
use databend_common_meta_app::schema::CreateTableTagReq;
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
use databend_common_meta_app::schema::vacuum_watermark_ident::VacuumWatermarkIdent;
use databend_common_meta_app::value_id::ValueId;
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
use log::warn;

use crate::api_impl::schema_api::restore_policy_references_on_undrop;
use crate::fetch_id;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::txn::meta_txn;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_put_pb;

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

            let txn = TxnRequest::new(conditions, vec![txn_put_pb(&key_tag, &table_tag)]);
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
        let ctx = format!(
            "{}: table_id={}, tag_name='{}', seq={}",
            func_name!(),
            req.table_id,
            req.tag_name,
            req.seq
                .map_or_else(|| "any".to_string(), |seq| seq.to_string())
        );

        let mgr = meta_txn::MetaTxnManager::new(self, ctx);
        mgr.remove_key(key_tag, req.seq)
            .await
            .map_err(|err| match err {
                meta_txn::RunError::KvApi(err) => KVAppError::MetaError(err),
                meta_txn::RunError::TxnRetryMaxTimes(err) => {
                    KVAppError::AppError(AppError::from(err))
                }
                meta_txn::RunError::User(err) => KVAppError::AppError(AppError::from(err)),
            })
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

    /// Create and publish a branch in one transaction.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_branch(&self, req: CreateTableBranchReq) -> Result<u64, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let branch_name = &req.branch_name;
        // `source_table_id` is the object being forked from at create time, while
        // `base_table_id` is the base table namespace where the new branch name lives.
        //
        // For example, `ALTER TABLE t/b1 CREATE BRANCH b2` uses `b1`'s table id as
        // `source_table_id`, but still uses `t`'s table id as `base_table_id`,
        // because the new branch is resolved as `t/b2`.
        let source_table_id = req.source_branch_id.unwrap_or(req.base_table_id);
        let key_source_table_id = TableId {
            table_id: source_table_id,
        };
        let key_branch = TableIdBranchName::new(req.base_table_id, branch_name);

        let mut maybe_branch_id = None;
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let Some(seq_source_table_meta) = self.get_pb(&key_source_table_id).await? else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(source_table_id, "create_table_branch: source table"),
                )));
            };
            if req.seq.match_seq(&seq_source_table_meta).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        source_table_id,
                        req.seq,
                        seq_source_table_meta.seq,
                        "create_table_branch",
                    ),
                )));
            }

            if self.get_pb(&key_branch).await?.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!("Branch '{}' already exists", branch_name)),
                )));
            }

            let mut conditions = vec![
                txn_cond_seq(&key_source_table_id, Eq, seq_source_table_meta.seq),
                txn_cond_seq(&key_branch, Eq, 0),
            ];

            if req.source_branch_id.is_some() {
                // The source may be a branch table, but the new branch is still created under the
                // base table namespace, so the base table must still exist and be active.
                let key_table_id = TableId {
                    table_id: req.base_table_id,
                };
                let Some(seq_table_meta) = self.get_pb(&key_table_id).await? else {
                    return Err(KVAppError::AppError(AppError::UnknownTableId(
                        UnknownTableId::new(req.base_table_id, "create_table_branch: base table"),
                    )));
                };
                if seq_table_meta.data.drop_on.is_some() {
                    return Err(KVAppError::AppError(AppError::UnknownTableId(
                        UnknownTableId::new(
                            req.base_table_id,
                            "create_table_branch: base table is dropped",
                        ),
                    )));
                }
                conditions.push(txn_cond_seq(&key_table_id, Eq, seq_table_meta.seq));
            }

            if let Some(lvt_check) = req.lvt_check.as_ref() {
                conditions.push(build_lvt_condition(self, source_table_id, lvt_check).await?);
            }

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
            let key_branch_id_to_name = BranchIdToName { branch_id };
            let table_branch = TableBranch {
                expire_at: req.expire_at,
                branch_id,
            };

            let mut if_then = vec![
                txn_put_pb(&key_branch_table_id, &req.new_table_meta),
                txn_put_pb(&key_branch, &table_branch),
                txn_put_pb(&key_branch_id_to_name, &key_branch),
            ];
            // Branch creation does not clone catalog-managed indexes. Index definitions are
            // maintained independently per table/branch and can be created on the branch later.

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
                        table_id: branch_id,
                    });
                if_then.push(txn_put_pb(&ident, &MaskPolicyTableId));
            }

            if let Some(policy_map) = &req.new_table_meta.row_access_policy_columns_ids {
                let ident = RowAccessPolicyTableIdIdent::new_generic(
                    req.tenant.clone(),
                    RowAccessPolicyIdTableId {
                        policy_id: policy_map.policy_id,
                        table_id: branch_id,
                    },
                );
                if_then.push(txn_put_pb(&ident, &RowAccessPolicyTableId {}));
            }

            for table_field in req.new_table_meta.schema.fields() {
                let Some(auto_increment_expr) = table_field.auto_increment_expr() else {
                    continue;
                };

                // Auto-increment is seeded from the source's current storage value instead of the
                // historical fork point so new allocations stay monotonic and do not reuse ids.
                let source_ai_key = AutoIncrementKey::new(source_table_id, table_field.column_id());
                let source_ai_ident =
                    AutoIncrementStorageIdent::new_generic(&req.tenant, source_ai_key);
                let start_value = match self.get_pb(&source_ai_ident).await? {
                    Some(seq_v) => *seq_v.data,
                    None => auto_increment_expr.start,
                };

                let auto_increment_key = AutoIncrementKey::new(branch_id, table_field.column_id());
                let storage_ident =
                    AutoIncrementStorageIdent::new_generic(&req.tenant, auto_increment_key);
                let storage_value = ValueId::<AutoIncrementStorageValue>::new(start_value);
                if_then.push(txn_put_pb(&storage_ident, &storage_value));
            }

            let txn = TxnRequest::new(conditions, if_then);
            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                table_id = req.base_table_id,
                branch_name :% =(branch_name),
                branch_id = branch_id,
                succ = succ;
                "create_table_branch"
            );

            if succ {
                return Ok(branch_id);
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
            // Expiration is the branch's automatic deletion time. Dropping an already expired
            // branch must not restart its retention window from now.
            let now = Utc::now();
            let drop_on = seq_branch
                .data
                .expire_at
                .filter(|expire_at| *expire_at <= now)
                .unwrap_or(now);
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
                    txn_put_pb(&key_branch_table_id, &table_meta),
                    txn_put_pb(&key_dropped_branch, &dropped_meta),
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
    async fn get_table_branch(&self, req: GetTableBranchReq) -> Result<TableInfo, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(req.table_id, branch_name);
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
            // Meta lookup only resolves the branch object by `table_id + branch_name`.
            // The caller rewrites this into the user-facing form `db.table/branch`.
            desc: branch_name.clone(),
            // `name` should be the base table name in the final TableInfo used by query code.
            // It is filled by the caller after resolving the branch under a specific table name.
            name: String::new(),
            meta: seq_table_meta.data,
            db_type: DatabaseType::NormalDB,
            // This meta API is currently reached only through the default catalog's branch lookup
            // path, so the returned TableInfo stays aligned with that default-catalog-only contract.
            catalog_info: Default::default(),
        };

        Ok(tb_info)
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
        let mut dropped_map: HashMap<u64, (String, DroppedBranchMeta)> = HashMap::new();

        for (key, seq_dropped) in &dropped_branches {
            branch_id_set.insert(key.branch_id);
            dropped_map.insert(
                key.branch_id,
                (key.branch_name.clone(), seq_dropped.data.clone()),
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
                if let Some((_, drop_meta)) = dropped_map.get(&branch_id.table_id) {
                    if drop_meta.drop_on < retention_boundary {
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
                } else if let Some((name, drop_meta)) = dropped_map.get(&branch_id.table_id) {
                    (name.clone(), drop_meta.expire_at)
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
    /// If multiple dropped branches share the same name, get the latest dropped.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch(&self, req: UndropTableBranchReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;
        let branch_name = &req.branch_name;
        let vacuum_ident = VacuumWatermarkIdent::new_global(&req.tenant);
        let dropped_dir =
            DirName::new_with_level(DroppedBranchIdent::new(table_id, branch_name, 0), 1);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_vacuum = self.get_pb(&vacuum_ident).await?;
            let vacuum_seq = seq_vacuum.as_ref().map(|s| s.seq).unwrap_or(0);
            let retention_bound = seq_vacuum.as_ref().map(|sv| sv.data.time);

            // List dropped branches with this name: __fd_dropped_branch/<table_id>/<branch_name>/
            let dropped_entries = self
                .list_pb_vec(ListOptions::unlimited(&dropped_dir))
                .await?;

            let retained_entries = if let Some(boundary) = retention_bound {
                dropped_entries
                    .into_iter()
                    .filter(|(_, meta)| meta.data.drop_on > boundary)
                    .collect::<Vec<_>>()
            } else {
                dropped_entries
            };
            if retained_entries.is_empty() {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("No recoverable dropped branch '{}' found", branch_name),
                ))));
            }

            let mut txn = TxnRequest::default();
            txn.condition
                .push(txn_cond_seq(&vacuum_ident, Eq, vacuum_seq));

            let (dropped_key, seq_dropped) = retained_entries
                .into_iter()
                .max_by_key(|(_, meta)| meta.data.drop_on)
                .unwrap();
            let by_id_req = UndropTableBranchByIdReq {
                tenant: req.tenant.clone(),
                table_id,
                branch_id: dropped_key.branch_id,
                new_expire_at: req.new_expire_at,
            };
            if append_undrop_table_branch_txn(self, &by_id_req, branch_name, seq_dropped, &mut txn)
                .await?
            {
                return Ok(());
            }
        }
    }

    /// Undrop a table branch by explicit branch_id.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch_by_id(
        &self,
        req: UndropTableBranchByIdReq,
    ) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        // Dropped branch entries are keyed by base table id, branch name and branch id.
        // Resolve the original name first so callers only need the stable branch id.
        let key_branch_id_to_name = BranchIdToName {
            branch_id: req.branch_id,
        };
        let Some(seq_branch_name) = self.get_pb(&key_branch_id_to_name).await? else {
            return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                format!("Branch id {} not found", req.branch_id),
            ))));
        };
        let branch_ref = seq_branch_name.data;
        if branch_ref.table_id != req.table_id {
            return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                format!(
                    "Branch id {} does not belong to table id {}",
                    req.branch_id, req.table_id
                ),
            ))));
        }
        let branch_name = branch_ref.branch_name;
        let key_dropped = DroppedBranchIdent::new(req.table_id, &branch_name, req.branch_id);
        let vacuum_ident = VacuumWatermarkIdent::new_global(req.tenant.clone());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_dropped = self.get_pb(&key_dropped).await?;
            let Some(seq_dropped) = seq_dropped else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Dropped branch '{}' (id={}) not found",
                        branch_name, req.branch_id
                    ),
                ))));
            };

            let seq_vacuum = self.get_pb(&vacuum_ident).await?;
            if let Some(ref sv) = seq_vacuum {
                if seq_dropped.data.drop_on <= sv.data.time {
                    return Err(KVAppError::AppError(AppError::UndropTableRetentionGuard(
                        UndropTableRetentionGuard::new(
                            format!("branch '{}'", branch_name),
                            seq_dropped.data.drop_on,
                            sv.data.time,
                        ),
                    )));
                }
            }
            let vacuum_seq = seq_vacuum.as_ref().map(|s| s.seq).unwrap_or(0);

            let mut txn = TxnRequest::default();
            txn.condition
                .push(txn_cond_seq(&vacuum_ident, Eq, vacuum_seq));
            if append_undrop_table_branch_txn(self, &req, &branch_name, seq_dropped, &mut txn)
                .await?
            {
                return Ok(());
            }
        }
    }

    async fn get_branch_name_by_id(&self, branch_id: u64) -> Result<Option<String>, MetaError> {
        debug!(req :? =(&branch_id); "RefApi: {}", func_name!());

        let id_to_name_key = BranchIdToName { branch_id };

        let seq_branch_name = self.get_pb(&id_to_name_key).await?;

        debug!(ident :% =(&id_to_name_key); "get_branch_name_by_id");

        let branch_name = seq_branch_name.map(|s| s.data.branch_name);

        Ok(branch_name)
    }

    async fn mget_branch_names_by_ids(
        &self,
        branch_ids: &[u64],
    ) -> Result<Vec<Option<String>>, MetaError> {
        debug!(req :? =(&branch_ids); "RefApi: {}", func_name!());

        let id_to_name_keys = branch_ids
            .iter()
            .map(|branch_id| BranchIdToName {
                branch_id: *branch_id,
            })
            .collect::<Vec<_>>();
        let seq_branch_names = self.get_pb_values_vec(id_to_name_keys).await?;

        Ok(seq_branch_names
            .into_iter()
            .map(|seq_branch_name| seq_branch_name.map(|s| s.data.branch_name))
            .collect())
    }
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}

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

async fn append_undrop_table_branch_txn<KV>(
    kv_api: &KV,
    req: &UndropTableBranchByIdReq,
    branch_name: &str,
    seq_dropped: SeqV<DroppedBranchMeta>,
    txn: &mut TxnRequest,
) -> Result<bool, KVAppError>
where
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
    let key_branch = TableIdBranchName::new(req.table_id, branch_name);
    let key_dropped = DroppedBranchIdent::new(req.table_id, branch_name, req.branch_id);
    let key_branch_table_id = TableId {
        table_id: req.branch_id,
    };

    let now = Utc::now();
    let restored_expire_at = match (req.new_expire_at, seq_dropped.data.expire_at) {
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
    let visible_branch = TableBranch {
        expire_at: restored_expire_at,
        branch_id: req.branch_id,
    };

    let seq_table_meta = kv_api.get_pb(&key_branch_table_id).await?;
    let Some(seq_table_meta) = seq_table_meta else {
        return Err(KVAppError::AppError(AppError::UnknownTableId(
            UnknownTableId::new(req.branch_id, "undrop_table_branch: branch table meta"),
        )));
    };

    let mut table_meta = seq_table_meta.data;
    let (policy_restore_ops, policy_restore_conditions) =
        restore_policy_references_on_undrop(kv_api, &req.tenant, req.branch_id, &mut table_meta)
            .await
            .map_err(KVAppError::from)?;
    table_meta.drop_on = None;

    txn.condition.extend([
        txn_cond_seq(&key_branch, Eq, 0),
        txn_cond_seq(&key_dropped, Eq, seq_dropped.seq),
        txn_cond_seq(&key_branch_table_id, Eq, seq_table_meta.seq),
    ]);
    txn.condition.extend(policy_restore_conditions);
    txn.if_then.extend([
        txn_put_pb(&key_branch, &visible_branch),
        txn_put_pb(&key_branch_table_id, &table_meta),
        txn_del(&key_dropped),
    ]);
    txn.if_then.extend(policy_restore_ops);
    let (succ, _responses) = send_txn(kv_api, txn.clone()).await?;
    if succ {
        return Ok(true);
    }

    let seq_branch = kv_api.get_pb(&key_branch).await?;
    if seq_branch.is_some() {
        return Err(KVAppError::AppError(AppError::from(
            ReferenceAlreadyExists::new(format!(
                "Branch '{}' already exists, cannot undrop",
                branch_name
            )),
        )));
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use databend_common_meta_app::app_error::AppError;
    use databend_common_meta_app::schema::LeastVisibleTime;
    use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
    use databend_meta_client::types::MatchSeq;

    use super::CreateTableTagReq;
    use super::DropTableTagReq;
    use super::GetTableTagReq;
    use super::ListTableTagsReq;
    use super::RefApi;
    use super::TableId;
    use super::TableIdTagName;
    use super::TableLvtCheck;
    use super::TableTag;
    use crate::kv_app_error::KVAppError;
    use crate::testing;
    use crate::testing::KVPbApiTestExt;

    const TABLE_ID: u64 = 10;
    const TENANT: &str = "tenant1";

    fn table_tag(snapshot_loc: &str, expire_at: Option<DateTime<Utc>>) -> TableTag {
        TableTag {
            expire_at,
            snapshot_loc: snapshot_loc.to_string(),
        }
    }

    fn table_key(table_id: u64) -> TableId {
        TableId::new(table_id)
    }

    fn tag_key(tag_name: &str) -> TableIdTagName {
        tag_key_for(TABLE_ID, tag_name)
    }

    fn tag_key_for(table_id: u64, tag_name: &str) -> TableIdTagName {
        TableIdTagName::new(table_id, tag_name)
    }

    fn create_req(
        tag_name: &str,
        snapshot_loc: &str,
        table_seq: u64,
        expire_at: Option<DateTime<Utc>>,
        lvt_time: DateTime<Utc>,
    ) -> CreateTableTagReq {
        create_req_with_match_seq(
            tag_name,
            snapshot_loc,
            MatchSeq::Exact(table_seq),
            expire_at,
            lvt_time,
        )
    }

    fn create_req_with_match_seq(
        tag_name: &str,
        snapshot_loc: &str,
        seq: MatchSeq,
        expire_at: Option<DateTime<Utc>>,
        lvt_time: DateTime<Utc>,
    ) -> CreateTableTagReq {
        CreateTableTagReq {
            table_id: TABLE_ID,
            seq,
            tag_name: tag_name.to_string(),
            snapshot_loc: snapshot_loc.to_string(),
            expire_at,
            lvt_check: TableLvtCheck {
                tenant: testing::tenant(TENANT),
                time: lvt_time,
            },
        }
    }

    fn drop_tag_req(tag_name: &str, seq: Option<u64>) -> DropTableTagReq {
        DropTableTagReq {
            table_id: TABLE_ID,
            tag_name: tag_name.to_string(),
            seq,
        }
    }

    fn get_tag_req(tag_name: &str, include_expired: bool) -> GetTableTagReq {
        GetTableTagReq {
            table_id: TABLE_ID,
            tag_name: tag_name.to_string(),
            include_expired,
        }
    }

    fn list_tags_req(include_expired: bool) -> ListTableTagsReq {
        ListTableTagsReq {
            table_id: TABLE_ID,
            include_expired,
        }
    }

    async fn seed_table(
        store: &(impl KVPbApiTestExt + ?Sized),
        table_id: u64,
        seq: u64,
    ) -> anyhow::Result<()> {
        store
            .insert_pb_assert_seq(&table_key(table_id), testing::table_meta(), seq)
            .await
    }

    async fn seed_dropped_table(
        store: &(impl KVPbApiTestExt + ?Sized),
        table_id: u64,
        seq: u64,
    ) -> anyhow::Result<()> {
        store
            .insert_pb_assert_seq(&table_key(table_id), testing::dropped_table_meta(), seq)
            .await
    }

    async fn create_tag(
        store: &(impl RefApi + ?Sized),
        tag_name: &str,
        snapshot_loc: &str,
        table_seq: u64,
        expire_at: Option<DateTime<Utc>>,
    ) -> Result<(), KVAppError> {
        let req = create_req(
            tag_name,
            snapshot_loc,
            table_seq,
            expire_at,
            testing::fixed_time(3_000),
        );
        store.create_table_tag(req).await
    }

    #[tokio::test]
    async fn test_create_table_tag_writes_value_and_seq() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        create_tag(&store, "tag1", "snapshot-location", 1, None).await?;

        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location", None))
            .await?;
        store
            .assert_pb(&table_key(TABLE_ID), 1, testing::table_meta())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_accepts_match_seq_variants() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        let req = create_req_with_match_seq(
            "ge-fail",
            "snapshot-location-ge-fail",
            MatchSeq::GE(2),
            None,
            testing::fixed_time(3_000),
        );
        let err = store.create_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::TableVersionMismatched(_))
        ));
        store.assert_no_pb(&tag_key("ge-fail")).await?;

        let req = create_req_with_match_seq(
            "any",
            "snapshot-location-any",
            MatchSeq::Any,
            None,
            testing::fixed_time(3_000),
        );
        store.create_table_tag(req).await?;
        store
            .assert_pb(&tag_key("any"), 2, table_tag("snapshot-location-any", None))
            .await?;

        let req = create_req_with_match_seq(
            "ge",
            "snapshot-location-ge",
            MatchSeq::GE(1),
            None,
            testing::fixed_time(3_000),
        );
        store.create_table_tag(req).await?;
        store
            .assert_pb(&tag_key("ge"), 3, table_tag("snapshot-location-ge", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_missing_table_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let err = create_tag(&store, "tag1", "snapshot-location", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownTableId(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location", 1, None).await?;
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_dropped_table_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_dropped_table(&store, TABLE_ID, 1).await?;

        let err = create_tag(&store, "tag1", "snapshot-location", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownTableId(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        let tenant = testing::tenant(TENANT);
        let lvt_ident = LeastVisibleTimeIdent::new(&tenant, TABLE_ID);
        store
            .insert_pb_assert_seq(
                &lvt_ident,
                LeastVisibleTime::new(testing::fixed_time(3_000)),
                2,
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_table_seq_mismatch_without_write() -> anyhow::Result<()>
    {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        let err = create_tag(&store, "tag1", "snapshot-location", 2, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::TableVersionMismatched(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        create_tag(&store, "tag1", "snapshot-location", 1, None).await?;
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_duplicate_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, None).await?;

        let err = create_tag(&store, "tag1", "snapshot-location-2", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::ReferenceAlreadyExists(_))
        ));
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location-1", None))
            .await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_expired_duplicate_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let expire_at = Some(testing::fixed_time(1));
        let expired_tag = table_tag("snapshot-location-1", expire_at);

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, expire_at).await?;

        let err = create_tag(&store, "tag1", "snapshot-location-2", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::ReferenceAlreadyExists(_))
        ));
        store.assert_pb(&tag_key("tag1"), 2, expired_tag).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_checks_lvt_without_failed_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        let tenant = testing::tenant(TENANT);
        let lvt_ident = LeastVisibleTimeIdent::new(&tenant, TABLE_ID);
        store
            .insert_pb_assert_seq(
                &lvt_ident,
                LeastVisibleTime::new(testing::fixed_time(4_000)),
                2,
            )
            .await?;

        let req = create_req(
            "tag1",
            "snapshot-location",
            1,
            None,
            testing::fixed_time(3_000),
        );
        let err = store.create_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::TableSnapshotExpired(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        let req = create_req(
            "tag1",
            "snapshot-location",
            1,
            None,
            testing::fixed_time(4_000),
        );
        store.create_table_tag(req).await?;
        store
            .assert_pb(&tag_key("tag1"), 3, table_tag("snapshot-location", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_table_tag_checks_seq_and_deletes() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, None).await?;

        let req = drop_tag_req("tag1", Some(3));
        let err = store.drop_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownReference(_))
        ));
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location-1", None))
            .await?;

        let req = drop_tag_req("tag1", Some(2));
        store.drop_table_tag(req).await?;
        store.assert_no_pb(&tag_key("tag1")).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_table_tag_without_seq() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let req = drop_tag_req("tag1", None);
        let err = store.drop_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownReference(_))
        ));

        let req = drop_tag_req("tag1", Some(7));
        let err = store.drop_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownReference(_))
        ));

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, None).await?;

        let req = drop_tag_req("tag1", None);
        store.drop_table_tag(req).await?;
        store.assert_no_pb(&tag_key("tag1")).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_table_tag_returns_none_and_keeps_future_tag_active() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let req = get_tag_req("missing", false);
        let got = store.get_table_tag(req).await?;
        assert_eq!(got, None);

        seed_table(&store, TABLE_ID, 1).await?;
        let expire_at = Some(testing::fixed_time(4_000_000_000));
        let future_tag = table_tag("snapshot-location-future", expire_at);
        create_tag(&store, "future", "snapshot-location-future", 1, expire_at).await?;

        let req = get_tag_req("future", false);
        let got = store.get_table_tag(req).await?.unwrap();
        assert_eq!((got.seq, got.data), (2, future_tag));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_table_tag_checks_expiration_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let expire_at = Some(testing::fixed_time(1));
        let expired_tag = table_tag("snapshot-location-1", expire_at);

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, expire_at).await?;

        let req = get_tag_req("tag1", true);
        let got = store.get_table_tag(req).await?;
        let got = got.unwrap();
        assert_eq!((got.seq, got.data), (2, expired_tag.clone()));

        let req = get_tag_req("tag1", false);
        let err = store.get_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::ReferenceExpired(_))
        ));
        store.assert_pb(&tag_key("tag1"), 2, expired_tag).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_list_table_tags_empty_and_table_id_isolated() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let req = list_tags_req(true);
        let got = store.list_table_tags(req).await?;
        assert_eq!(got, vec![]);

        store
            .insert_pb_assert_seq(
                &tag_key_for(1, "one"),
                table_tag("snapshot-location-one", None),
                1,
            )
            .await?;
        store
            .insert_pb_assert_seq(&tag_key("ten"), table_tag("snapshot-location-ten", None), 2)
            .await?;
        store
            .insert_pb_assert_seq(
                &tag_key_for(100, "hundred"),
                table_tag("snapshot-location-hundred", None),
                3,
            )
            .await?;

        let req = list_tags_req(true);
        let got = store.list_table_tags(req).await?;
        let got = got
            .into_iter()
            .map(|(name, seqv)| (name, seqv.seq, seqv.data))
            .collect::<Vec<_>>();
        assert_eq!(got, vec![(
            "ten".to_string(),
            2,
            table_tag("snapshot-location-ten", None)
        )]);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_table_tags_filters_expired_tags() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let expired_at = Some(testing::fixed_time(1));
        let future_at = Some(testing::fixed_time(4_000_000_000));
        let active_tag = table_tag("snapshot-location-1", None);
        let expired_tag = table_tag("snapshot-location-2", expired_at);
        let future_tag = table_tag("snapshot-location-3", future_at);

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "active", "snapshot-location-1", 1, None).await?;
        create_tag(&store, "expired", "snapshot-location-2", 1, expired_at).await?;
        create_tag(&store, "future", "snapshot-location-3", 1, future_at).await?;

        let req = list_tags_req(false);
        let got = store.list_table_tags(req).await?;
        let got = got
            .into_iter()
            .map(|(name, seqv)| (name, seqv.seq, seqv.data))
            .collect::<Vec<_>>();
        assert_eq!(got, vec![
            ("active".to_string(), 2, active_tag.clone()),
            ("future".to_string(), 4, future_tag.clone()),
        ]);

        let req = list_tags_req(true);
        let got = store.list_table_tags(req).await?;
        let got = got
            .into_iter()
            .map(|(name, seqv)| (name, seqv.seq, seqv.data))
            .collect::<Vec<_>>();
        assert_eq!(got, vec![
            ("active".to_string(), 2, active_tag),
            ("expired".to_string(), 3, expired_tag),
            ("future".to_string(), 4, future_tag),
        ]);

        Ok(())
    }
}
