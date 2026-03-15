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
use databend_common_meta_app::app_error::CreateAsDropTableWithoutDropTime;
use databend_common_meta_app::app_error::CreateTableWithDropTime;
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

    /// Create a table branch.
    ///
    /// A branch is logically an independent table with a pointer back to the base table.
    /// The caller provides a pre-built TableMeta. This function writes both the branch record
    /// and the branch table meta for the new branch generation.
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
        let mut maybe_branch_id = None;
        let mut orphan_branch_name = None;

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

            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

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

            let seq_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<BranchIdHistoryIdent>(d)?;
                assert_eq!(key_branch_id_list, k);
                v
            };
            let id_list_seq = seq_id_list.as_ref().map_or(0, |s| s.seq);
            let id_list = seq_id_list.map(|s| s.data).unwrap_or_else(TableIdList::new);

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

            save_id_list.append(branch_id);

            let mut conditions = vec![
                txn_cond_seq(&key_source_table_id, Eq, seq_source_table_meta.seq),
                txn_cond_seq(&key_branch, Eq, 0),
                txn_cond_seq(&save_key_branch_id_list, Eq, save_id_list_seq),
            ];
            if let Some(cond) =
                build_lvt_condition(self, source_table_id, req.lvt_check.as_ref()).await?
            {
                conditions.push(cond);
            }

            let mut if_then = vec![
                txn_put_pb(&key_branch_table_id, &branch_meta)?,
                txn_put_pb(&save_key_branch_id_list, &save_id_list)?,
            ];
            if !req.as_dropped {
                let table_branch = TableBranch {
                    expire_at: req.expire_at,
                    branch_id,
                };
                if_then.push(txn_put_pb(&key_branch, &table_branch)?);
            }
            let mut txn = TxnRequest::new(conditions, if_then);

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
                let Some(branch_id_seq) = branch_seq else {
                    return Err(KVAppError::AppError(AppError::UnknownTableId(
                        UnknownTableId::new(
                            branch_id,
                            "create_table_branch: missing table_id_seq in txn response",
                        ),
                    )));
                };

                return Ok(CreateTableBranchReply {
                    branch_id,
                    branch_id_seq,
                    branch_meta: branch_meta.clone(),
                    orphan_branch_name: orphan_branch_name.clone(),
                    prev_branch_id,
                });
            }

            data = vec![];
        }
    }

    /// Commit a hidden branch and make it visible.
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
            table_meta.drop_on = Some(Utc::now());

            let mut txn = TxnRequest::new(
                vec![
                    txn_cond_seq(&key_branch, Eq, seq_branch.seq),
                    txn_cond_seq(&key_branch_table_id, Eq, seq_table_meta.seq),
                ],
                vec![
                    txn_del(&key_branch),
                    txn_put_pb(&key_branch_table_id, &table_meta)?,
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

    /// Get a table branch.
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
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
