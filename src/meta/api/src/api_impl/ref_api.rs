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
use std::sync::Arc;

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::CreateAsDropTableWithoutDropTime;
use databend_common_meta_app::app_error::CreateTableWithDropTime;
use databend_common_meta_app::app_error::ReferenceAlreadyExists;
use databend_common_meta_app::app_error::ReferenceExpired;
use databend_common_meta_app::app_error::TableSnapshotExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UndropTableAlreadyExists;
use databend_common_meta_app::app_error::UndropTableHasNoHistory;
use databend_common_meta_app::app_error::UndropTableRetentionGuard;
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
use databend_common_meta_app::schema::BranchIdHistoryIdent;
use databend_common_meta_app::schema::CommitTableBranchMetaReq;
use databend_common_meta_app::schema::CreateTableBranchReply;
use databend_common_meta_app::schema::CreateTableBranchReq;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::GetTableBranchReq;
use databend_common_meta_app::schema::GetTableTagReq;
use databend_common_meta_app::schema::HistoryTableBranchMetaItem;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
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
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::TableTag;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UndropTableBranchReq;
use databend_common_meta_app::schema::VacuumWatermarkIdent;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnGetResponse;
use databend_meta_types::TxnRequest;
use databend_meta_types::protobuf as pb;
use databend_meta_types::txn_op_response::Response;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;
use log::info;

use crate::database_util::get_db_or_err;
use crate::deserialize_struct_get_response;
use crate::fetch_id;
use crate::garbage_collection_api::ORPHAN_POSTFIX;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::schema_api::restore_policy_references_on_undrop;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_get;
use crate::txn_put_pb;
use crate::util::IdempotentKVTxnResponse;
use crate::util::IdempotentKVTxnSender;

async fn build_lvt_condition(
    kv_api: &(impl KVPbApi<Error = MetaError> + ?Sized),
    table_id: u64,
    lvt_check: Option<&TableLvtCheck>,
) -> Result<Option<TxnCondition>, KVAppError> {
    let Some(check) = lvt_check else {
        return Ok(None);
    };

    let lvt_ident = LeastVisibleTimeIdent::new(&check.tenant, table_id);
    let res = kv_api.get_pb(&lvt_ident).await?;
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

    Ok(Some(txn_cond_seq(&lvt_ident, Eq, lvt_seq)))
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
        let keys = vec![key_table_id.to_string_key(), key_tag.to_string_key()];
        let mut data = {
            let values = self.mget_kv(&keys).await?;
            keys.iter()
                .zip(values.into_iter())
                .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                .collect::<Vec<_>>()
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

            // Ensure the base table exists and has not changed.
            let seq_table_meta = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableId>(d)?;
                assert_eq!(key_table_id, k);
                v
            };
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
            let seq_tag = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdTagName>(d)?;
                assert_eq!(key_tag, k);
                v
            };
            if seq_tag.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!("Tag '{}' already exists", req.tag_name)),
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

            if let Some(cond) = build_lvt_condition(self, table_id, req.lvt_check.as_ref()).await? {
                conditions.push(cond);
            }

            let txn = TxnRequest::new(conditions, vec![txn_put_pb(&key_tag, &table_tag)?]);

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
            data = vec![];
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
        let expected_seq = req.seq;
        let key_tag = TableIdTagName::new(table_id, tag_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_tag = self.get_pb(&key_tag).await?;
            let Some(seq_tag) = seq_tag else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("Unknown tag '{}'", tag_name),
                ))));
            };
            if expected_seq.match_seq(&seq_tag).is_err() {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Tag '{}' seq mismatched: expect {}, current {}",
                        tag_name, expected_seq, seq_tag.seq
                    ),
                ))));
            }

            let txn = TxnRequest::new(vec![txn_cond_seq(&key_tag, Eq, seq_tag.seq)], vec![
                txn_del(&key_tag),
            ]);

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
        if !req.include_expired {
            if let Some(tag) = &seq_tag {
                if let Some(expire_at) = tag.data.expire_at.as_ref() {
                    if *expire_at <= Utc::now() {
                        return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                            format!("Tag '{}' expired at '{}'", tag_name, expire_at),
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
    /// - `__fd_table_branch/<table_id>/<branch_name> -> TableBranch` (if `as_dropped=false`)
    /// - `__fd_table_by_id/<branch_id> -> TableMeta` (branch's own TableMeta)
    /// - `__fd_branch_id_list/<table_id>/<branch_name> -> TableIdList`
    /// - Policy reference KVs for valid mask/row_access policies
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_branch(
        &self,
        req: CreateTableBranchReq,
    ) -> Result<CreateTableBranchReply, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        if !req.as_dropped && req.table_meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateTableWithDropTime(
                CreateTableWithDropTime::new(&req.branch_name),
            )));
        }
        if req.as_dropped && req.table_meta.drop_on.is_none() {
            return Err(KVAppError::AppError(
                AppError::CreateAsDropTableWithoutDropTime(CreateAsDropTableWithoutDropTime::new(
                    &req.branch_name,
                )),
            ));
        }

        let branch_name = &req.branch_name;
        let table_id = req.table_id;
        let source_table_id = req.source_table_id;
        let key_source_table_id = TableId {
            table_id: source_table_id,
        };
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let key_branch_id_list = BranchIdHistoryIdent {
            table_id,
            branch_name: branch_name.clone(),
        };

        let branch_meta = req.table_meta;

        let mut maybe_branch_id: Option<u64> = None;
        let mut orphan_branch_name: Option<String> = None;

        // The keys of values to re-fetch for every retry in this txn.
        let keys = vec![
            key_source_table_id.to_string_key(),
            key_branch.to_string_key(),
            key_branch_id_list.to_string_key(),
        ];
        let mut data = {
            let values = self.mget_kv(&keys).await?;
            keys.iter()
                .zip(values.into_iter())
                .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                .collect::<Vec<_>>()
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Re-fetch all required keys in one shot for retry.
            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

            // 1. Ensure the source table exists and has not changed.
            let seq_source_table_meta = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableId>(d)?;
                assert_eq!(key_source_table_id, k);
                v
            };
            let Some(seq_source_table_meta) = seq_source_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(source_table_id, "create_table_branch: source table"),
                )));
            };

            // Check seq matches caller's expectation.
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

            // 2. Check if branch already exists.
            let seq_branch = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdBranchName>(d)?;
                assert_eq!(key_branch, k);
                v
            };
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!("Branch '{}' already exists", branch_name)),
                )));
            }

            // 3. Get current real branch id list.
            let seq_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<BranchIdHistoryIdent>(d)?;
                assert_eq!(key_branch_id_list, k);
                v
            };
            let id_list_seq = seq_id_list.as_ref().map_or(0, |s| s.seq);
            let id_list = seq_id_list.map(|s| s.data).unwrap_or_else(TableIdList::new);

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

            if req.as_dropped && orphan_branch_name.is_none() {
                // Use allocated branch_id as orphan suffix to avoid timestamp collisions
                // under concurrent create requests.
                orphan_branch_name = Some(format!("{}@{}", ORPHAN_POSTFIX, branch_id));
            }
            let save_key_branch_id_list = if let Some(orphan_branch_name) = &orphan_branch_name {
                BranchIdHistoryIdent {
                    table_id,
                    branch_name: orphan_branch_name.clone(),
                }
            } else {
                key_branch_id_list.clone()
            };

            let (save_id_list_seq, mut save_id_list, prev_branch_id) = if req.as_dropped {
                (0, TableIdList::new(), id_list.last().copied())
            } else {
                (id_list_seq, id_list, None)
            };

            // 5. Append branch_id to branch id list (or orphan list for as_dropped mode).
            save_id_list.append(branch_id);

            let mut conditions = vec![
                // Source table must not change during branch creation.
                txn_cond_seq(&key_source_table_id, Eq, seq_source_table_meta.seq),
                // Branch must not already exist.
                txn_cond_seq(&key_branch, Eq, 0),
                // Target branch id list must not change.
                txn_cond_seq(&save_key_branch_id_list, Eq, save_id_list_seq),
            ];

            if let Some(cond) =
                build_lvt_condition(self, source_table_id, req.lvt_check.as_ref()).await?
            {
                conditions.push(cond);
            }

            let mut if_then = vec![
                // Write branch's TableMeta: __fd_table_by_id/<branch_id>
                txn_put_pb(&key_branch_table_id, &branch_meta)?,
                // Write branch id list: __fd_branch_id_list/<table_id>/<list_name>
                txn_put_pb(&save_key_branch_id_list, &save_id_list)?,
            ];
            if !req.as_dropped {
                let table_branch = TableBranch {
                    expire_at: req.expire_at,
                    branch_id,
                };
                // Write active branch record: __fd_table_branch/<table_id>/<branch_name>
                if_then.push(txn_put_pb(&key_branch, &table_branch)?);
            }
            let mut txn = TxnRequest::new(conditions, if_then);

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
                txn.if_then.push(txn_put_pb(&ident, &MaskPolicyTableId)?);
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
                txn.if_then
                    .push(txn_put_pb(&ident, &RowAccessPolicyTableId {})?);
            }

            // 9. Initialize auto-increment storages for the new branch table.
            for table_field in branch_meta.schema.fields() {
                let Some(auto_increment_expr) = table_field.auto_increment_expr() else {
                    continue;
                };

                let auto_increment_key = AutoIncrementKey::new(branch_id, table_field.column_id());
                let storage_ident = AutoIncrementStorageIdent::new_generic(
                    &req.name_ident.tenant,
                    auto_increment_key,
                );
                let storage_value =
                    Id::new_typed(AutoIncrementStorageValue(auto_increment_expr.start));

                txn.if_then
                    .push(txn_put_pb(&storage_ident, &storage_value)?);
            }

            // Keep this `txn_get` as the last op to fetch the committed seq of `key_branch_table_id`.
            txn.if_then.push(txn_get(&key_branch_table_id));

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

                return Ok(CreateTableBranchReply {
                    branch_id,
                    branch_id_seq: branch_seq,
                    branch_meta: branch_meta.clone(),
                    orphan_branch_name: orphan_branch_name.clone(),
                    prev_branch_id,
                });
            } else {
                // re-run txn with re-fetched data
                data = vec![];
            }
        }
    }

    /// Commit a hidden branch and make it visible.
    ///
    /// This operation:
    /// - updates branch table meta (e.g. snapshot location),
    /// - clears `drop_on`,
    /// - creates active branch name mapping.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn commit_table_branch_meta(
        &self,
        req: CommitTableBranchMetaReq,
    ) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_branch = TableIdBranchName::new(req.table_id, &req.branch_name);
        let key_branch_id_list = BranchIdHistoryIdent {
            table_id: req.table_id,
            branch_name: req.branch_name.clone(),
        };
        let key_orphan_branch_id_list = BranchIdHistoryIdent {
            table_id: req.table_id,
            branch_name: req.orphan_branch_name.clone(),
        };
        let key_branch_table_id = TableId {
            table_id: req.branch_id,
        };
        let keys = vec![
            key_branch.to_string_key(),
            key_orphan_branch_id_list.to_string_key(),
            key_branch_id_list.to_string_key(),
            key_branch_table_id.to_string_key(),
        ];
        let mut data = {
            let values = self.mget_kv(&keys).await?;
            keys.iter()
                .zip(values.into_iter())
                .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                .collect::<Vec<_>>()
        };

        let txn_sender = IdempotentKVTxnSender::new();

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

            // Branch must still be hidden.
            let seq_branch = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdBranchName>(d)?;
                assert_eq!(key_branch, k);
                v
            };
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!(
                        "Branch '{}' already committed",
                        req.branch_name
                    )),
                )));
            }

            // Orphan branch history must exist and still point to this branch generation.
            let seq_orphan_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<BranchIdHistoryIdent>(d)?;
                assert_eq!(key_orphan_branch_id_list, k);
                v
            };
            let Some(seq_orphan_id_list) = seq_orphan_id_list else {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("Unknown branch '{}'", req.branch_name),
                ))));
            };
            if seq_orphan_id_list.data.id_list.len() != 1
                || seq_orphan_id_list.data.last().copied() != Some(req.branch_id)
            {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Branch '{}' orphan generation changed, expect branch id {}",
                        req.branch_name, req.branch_id
                    ),
                ))));
            }

            // Real branch history must still point to the expected previous generation.
            let seq_real_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<BranchIdHistoryIdent>(d)?;
                assert_eq!(key_branch_id_list, k);
                v
            };
            let real_id_list_seq = seq_real_id_list.as_ref().map_or(0, |s| s.seq);
            let mut real_id_list = seq_real_id_list
                .map(|s| s.data)
                .unwrap_or_else(TableIdList::new);
            if real_id_list.last().copied() != req.prev_branch_id {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!(
                        "Branch '{}' previous generation changed, expect {:?}, current {:?}",
                        req.branch_name,
                        req.prev_branch_id,
                        real_id_list.last().copied()
                    ),
                ))));
            }

            // Branch table meta must exist and match caller expectation.
            let seq_branch_meta = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableId>(d)?;
                assert_eq!(key_branch_table_id, k);
                v
            };
            let Some(seq_branch_meta) = seq_branch_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.branch_id, "commit_table_branch_meta"),
                )));
            };
            if req.seq.match_seq(&seq_branch_meta).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.branch_id,
                        req.seq,
                        seq_branch_meta.seq,
                        "commit_table_branch_meta",
                    ),
                )));
            }
            if seq_branch_meta.data.drop_on.is_none() {
                return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                    format!("Branch '{}' has already been committed", req.branch_name),
                ))));
            }

            let mut committed_branch_meta = req.new_table_meta.clone();
            committed_branch_meta.drop_on = None;

            let table_branch = TableBranch {
                expire_at: req.expire_at,
                branch_id: req.branch_id,
            };
            real_id_list.append(req.branch_id);

            let txn_req = TxnRequest::new(
                vec![
                    txn_cond_seq(&key_branch, Eq, 0),
                    txn_cond_seq(&key_orphan_branch_id_list, Eq, seq_orphan_id_list.seq),
                    txn_cond_seq(&key_branch_id_list, Eq, real_id_list_seq),
                    txn_cond_seq(&key_branch_table_id, Eq, seq_branch_meta.seq),
                ],
                vec![
                    txn_put_pb(&key_branch_table_id, &committed_branch_meta)?,
                    txn_put_pb(&key_branch, &table_branch)?,
                    txn_del(&key_orphan_branch_id_list),
                    txn_put_pb(&key_branch_id_list, &real_id_list)?,
                ],
            );

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

            data = vec![];
        }
    }

    /// Drop a table branch.
    ///
    /// Sets branch `TableMeta.drop_on` on `__fd_table_by_id/<branch_id>` and
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
        let expected_branch_id = req.branch_id;

        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // 1. Get the branch record to find branch_id.
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

            // 2. Get the branch's TableMeta.
            let key_branch_table_id = TableId {
                table_id: expected_branch_id,
            };
            let seq_table_meta = self.get_pb(&key_branch_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(expected_branch_id, "drop_table_branch: branch table meta"),
                )));
            };

            // 3. Set drop_on and remove branch from active branch namespace.
            let mut table_meta = seq_table_meta.data;
            table_meta.drop_on = Some(Utc::now());

            let mut txn = TxnRequest::new(
                vec![
                    txn_cond_seq(&key_branch, Eq, seq_branch.seq),
                    txn_cond_seq(&key_branch_table_id, Eq, seq_table_meta.seq),
                ],
                vec![
                    // Remove branch record.
                    txn_del(&key_branch),
                    // Mark branch meta dropped in __fd_table_by_id.
                    txn_put_pb(&key_branch_table_id, &table_meta)?,
                ],
            );

            // 4. Clean up column mask policy references.
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

            // 5. Clean up row access policy reference.
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

            // 6. Todo: Clean up ownership.

            // 7. Clean up tag references.
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

            debug!(
                table_id = table_id,
                branch_name :% =(branch_name),
                branch_id = expected_branch_id,
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
    /// Clears branch `TableMeta.drop_on` in `__fd_table_by_id/<branch_id>` and
    /// re-creates the branch record in `__fd_table_branch`.
    ///
    /// The branch_id is retrieved from the last entry in
    /// `__fd_branch_id_list/<table_id>/<branch_name>`.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_branch(&self, req: UndropTableBranchReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let tenant = req.tenant.clone();
        let table_id = req.table_id;
        let branch_name = &req.branch_name;
        let key_branch = TableIdBranchName::new(table_id, branch_name);
        let key_branch_id_list = BranchIdHistoryIdent {
            table_id,
            branch_name: branch_name.clone(),
        };
        let vacuum_ident = VacuumWatermarkIdent::new_global(tenant.clone());
        let keys = vec![
            key_branch.to_string_key(),
            key_branch_id_list.to_string_key(),
            vacuum_ident.to_string_key(),
        ];
        let mut data = {
            let values = self.mget_kv(&keys).await?;
            keys.iter()
                .zip(values.into_iter())
                .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                .collect::<Vec<_>>()
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

            // 1. Ensure branch is not currently active.
            let seq_branch = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdBranchName>(d)?;
                assert_eq!(key_branch, k);
                v
            };
            if seq_branch.is_some() {
                return Err(KVAppError::AppError(AppError::UndropTableAlreadyExists(
                    UndropTableAlreadyExists::new(branch_name),
                )));
            }

            // 2. Get the last branch_id from the id list.
            let seq_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<BranchIdHistoryIdent>(d)?;
                assert_eq!(key_branch_id_list, k);
                v
            };
            let Some(seq_id_list) = seq_id_list else {
                return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(branch_name),
                )));
            };

            let Some(&branch_id) = seq_id_list.data.last() else {
                return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(branch_name),
                )));
            };

            // 3. Get the branch TableMeta and ensure it is in dropped state.
            let key_branch_table_id = TableId {
                table_id: branch_id,
            };
            let seq_branch_meta = self.get_pb(&key_branch_table_id).await?;
            let Some(seq_branch_meta) = seq_branch_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(branch_id, "undrop_table_branch: no branch table meta"),
                )));
            };

            let drop_marker = *seq_branch_meta
                .data
                .drop_on
                .as_ref()
                .unwrap_or(&seq_branch_meta.data.updated_on);

            let seq_vacuum_retention = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<VacuumWatermarkIdent>(d)?;
                assert_eq!(vacuum_ident, k);
                v
            };

            if let Some(ref sr) = seq_vacuum_retention {
                let retention_time = sr.data.time;
                if drop_marker <= retention_time {
                    return Err(KVAppError::AppError(AppError::UndropTableRetentionGuard(
                        UndropTableRetentionGuard::new(branch_name, drop_marker, retention_time),
                    )));
                }
            }

            // 4. Restore policy references or clean missing policies in branch meta.
            let mut table_meta = seq_branch_meta.data;
            let (policy_restore_ops, policy_restore_conditions) =
                restore_policy_references_on_undrop(self, &tenant, branch_id, &mut table_meta)
                    .await
                    .map_err(KVAppError::from)?;

            // 5. Clear drop_on.
            table_meta.drop_on = None;

            // 6. Re-create the branch record.
            let table_branch = TableBranch {
                expire_at: None,
                branch_id,
            };

            let vacuum_seq = seq_vacuum_retention.as_ref().map(|sr| sr.seq).unwrap_or(0);

            let txn = TxnRequest::new(
                [
                    vec![
                        // Branch must still not exist.
                        txn_cond_seq(&key_branch, Eq, 0),
                        // Branch meta must not change.
                        txn_cond_seq(&key_branch_table_id, Eq, seq_branch_meta.seq),
                        // Branch id list must not change.
                        txn_cond_seq(&key_branch_id_list, Eq, seq_id_list.seq),
                        // Vacuum watermark must not change during undrop.
                        txn_cond_seq(&vacuum_ident, Eq, vacuum_seq),
                    ],
                    policy_restore_conditions,
                ]
                .concat(),
                [
                    vec![
                        // Write back to __fd_table_by_id.
                        txn_put_pb(&key_branch_table_id, &table_meta)?,
                        // Re-create branch record.
                        txn_put_pb(&key_branch, &table_branch)?,
                    ],
                    policy_restore_ops,
                ]
                .concat(),
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

            data = vec![];
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
            return Err(KVAppError::AppError(AppError::from(UnknownReference::new(
                format!("Unknown branch '{}'", branch_name),
            ))));
        };
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
            if branch_meta.data.drop_on.is_some() {
                continue;
            }
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

    /// List all branch generations (active and dropped) for a base table.
    ///
    /// Reads branch history under `__fd_branch_id_list/<table_id>/`, then reads branch metas
    /// from `__fd_table_by_id/<branch_id>`. Active status and expiration are attached from
    /// `__fd_table_branch/<table_id>/<branch_name>`.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_history_table_branches(
        &self,
        req: ListHistoryTableBranchesReq,
    ) -> Result<Vec<HistoryTableBranchMetaItem>, KVAppError> {
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
        let active_map = active_branches
            .iter()
            .map(|(key, branch)| {
                (
                    key.branch_name.clone(),
                    (branch.data.branch_id, branch.data.expire_at),
                )
            })
            .collect::<HashMap<_, _>>();

        let mut branch_candidates = Vec::with_capacity(histories.len());
        let mut seen_branch_ids = HashSet::new();
        for (key, seq_id_list) in &histories {
            for branch_id in &seq_id_list.data.id_list {
                if seen_branch_ids.insert(*branch_id) {
                    branch_candidates.push((key.branch_name.clone(), *branch_id));
                }
            }
        }

        // Defensive: include active branch ids not found in branch history.
        for (branch_name, (branch_id, _)) in &active_map {
            if seen_branch_ids.insert(*branch_id) {
                branch_candidates.push((branch_name.clone(), *branch_id));
            }
        }

        if branch_candidates.is_empty() {
            return Ok(vec![]);
        }

        let branch_keys = branch_candidates
            .iter()
            .map(|(_, branch_id)| TableId::new(*branch_id))
            .collect::<Vec<_>>();
        let branch_metas = self.get_pb_values_vec(branch_keys).await?;

        let mut branches = Vec::with_capacity(branch_metas.len());
        for ((branch_name, branch_id), branch_meta) in
            branch_candidates.into_iter().zip(branch_metas.into_iter())
        {
            let Some(branch_meta) = branch_meta else {
                continue;
            };

            if req.retention_boundary.is_some_and(|retention_boundary| {
                branch_meta
                    .data
                    .drop_on
                    .is_some_and(|drop_on| drop_on < retention_boundary)
            }) {
                continue;
            }

            let expire_at = active_map
                .get(&branch_name)
                .and_then(|(active_id, expire_at)| {
                    if *active_id == branch_id {
                        Some(*expire_at)
                    } else {
                        None
                    }
                })
                .unwrap_or(None);

            branches.push(HistoryTableBranchMetaItem {
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
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
