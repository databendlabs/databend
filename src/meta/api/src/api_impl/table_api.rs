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
use std::convert::Infallible;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::CommitTableMetaError;
use databend_common_meta_app::app_error::CreateAsDropTableWithoutDropTime;
use databend_common_meta_app::app_error::CreateTableWithDropTime;
use databend_common_meta_app::app_error::DuplicatedIndexColumnId;
use databend_common_meta_app::app_error::DuplicatedUpsertFiles;
use databend_common_meta_app::app_error::IndexColumnIdNotFound;
use databend_common_meta_app::app_error::MultiStmtTxnCommitFailed;
use databend_common_meta_app::app_error::StreamAlreadyExists;
use databend_common_meta_app::app_error::StreamVersionMismatched;
use databend_common_meta_app::app_error::TableAlreadyExists;
use databend_common_meta_app::app_error::TableSnapshotExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UndropTableHasNoHistory;
use databend_common_meta_app::app_error::UndropTableWithNoDropTime;
use databend_common_meta_app::app_error::UnknownDatabaseId;
use databend_common_meta_app::app_error::UnknownStreamId;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::app_error::ViewAlreadyExists;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::primitive::Id;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::schema::AutoIncrementStorageValue;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListDroppedTableResp;
use databend_common_meta_app::schema::ListTableCopiedFileReply;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SwapTableReply;
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpdateTableMetaReply;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_app::schema::table_niv::TableNIV;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::MetaError;
use databend_meta_types::MetaId;
use databend_meta_types::SeqV;
use databend_meta_types::TxnGetRequest;
use databend_meta_types::TxnGetResponse;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;
use databend_meta_types::protobuf as pb;
use databend_meta_types::txn_op::Request;
use databend_meta_types::txn_op_response::Response;
use fastrace::func_name;
use log::debug;
use log::error;
use log::info;
use seq_marked::SeqValue;

use super::database_api::DatabaseApi;
use super::database_util::get_db_or_err;
use super::garbage_collection_api::ORPHAN_POSTFIX;
use super::garbage_collection_api::get_history_tables_for_gc;
use super::schema_api::build_upsert_table_deduplicated_label;
use super::schema_api::construct_drop_table_txn_operations;
use super::schema_api::get_db_by_id_or_err;
use super::schema_api::get_history_table_metas;
use super::schema_api::handle_undrop_table;
use crate::DEFAULT_MGET_SIZE;
use crate::assert_table_exist;
use crate::deserialize_struct;
use crate::error_util::table_has_to_not_exist;
use crate::fetch_id;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_fetch_util::deserialize_id_get_response;
use crate::kv_fetch_util::deserialize_struct_get_response;
use crate::kv_fetch_util::mget_pb_values;
use crate::kv_fetch_util::mget_u64_values;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::list_u64_value;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_get;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;
use crate::txn_put_pb;
use crate::txn_put_u64;
use crate::util::IdempotentKVTxnResponse;
use crate::util::IdempotentKVTxnSender;

/// Assert that `table_id` is the latest entry in `tb_id_list`.
///
/// Returns an error if the last ID in the history list doesn't match the
/// expected current table ID, indicating a concurrent modification.
fn assert_table_id_is_latest(
    table_id: u64,
    tb_id_list: &TableIdList,
    table_name: &str,
    operation: &str,
) -> Result<(), KVAppError> {
    let last = tb_id_list.last().copied();
    if Some(table_id) == last {
        return Ok(());
    }
    let msg = format!(
        "{operation} {table_name}: table id conflict, id list last: {last:?}, current: {table_id}"
    );
    error!("{msg}");
    Err(KVAppError::AppError(AppError::UnknownTable(
        UnknownTable::new(table_name, msg),
    )))
}

fn validate_index_columns(meta: &TableMeta) -> Result<(), KVAppError> {
    let mut seen = HashSet::new();
    for (_, index) in meta.indexes.iter() {
        for &column_id in &index.column_ids {
            if meta.schema.is_column_deleted(column_id) {
                return Err(KVAppError::AppError(AppError::IndexColumnIdNotFound(
                    IndexColumnIdNotFound::new(column_id, &index.name),
                )));
            }
            if !seen.insert((column_id, index.index_type.clone())) {
                return Err(KVAppError::AppError(AppError::DuplicatedIndexColumnId(
                    DuplicatedIndexColumnId::new(column_id, &index.name),
                )));
            }
        }
    }
    Ok(())
}

fn validate_create_table_request(req: &CreateTableReq) -> Result<(), KVAppError> {
    let name = &req.name_ident.table_name;
    if !req.as_dropped && req.table_meta.drop_on.is_some() {
        return Err(KVAppError::AppError(AppError::CreateTableWithDropTime(
            CreateTableWithDropTime::new(name),
        )));
    }
    if req.as_dropped && req.table_meta.drop_on.is_none() {
        return Err(KVAppError::AppError(
            AppError::CreateAsDropTableWithoutDropTime(CreateAsDropTableWithoutDropTime::new(name)),
        ));
    }
    Ok(())
}

/// TableApi defines APIs for table lifecycle and metadata management.
///
/// This trait handles:
/// - Table creation, deletion, and restoration
/// - Table metadata queries and listing
/// - Table batch operations and file management
/// - Table retention and garbage collection
#[async_trait::async_trait]
pub trait TableApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, KVAppError> {
        // Make an error if table exists.
        fn make_exists_err(req: &CreateTableReq) -> AppError {
            let name = &req.name_ident.table_name;
            let name_ident = &req.name_ident;

            match req.table_meta.engine.as_str() {
                "STREAM" => {
                    let exist_err =
                        StreamAlreadyExists::new(name, format!("create_table: {}", name_ident));
                    AppError::from(exist_err)
                }
                "VIEW" => {
                    let exist_err =
                        ViewAlreadyExists::new(name, format!("create_table: {}", name_ident));
                    AppError::from(exist_err)
                }
                _ => {
                    let exist_err =
                        TableAlreadyExists::new(name, format!("create_table: {}", name_ident));
                    AppError::from(exist_err)
                }
            }
        }

        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();

        let mut maybe_key_table_id: Option<TableId> = None;

        validate_create_table_request(&req)?;

        // fixed: does not change in every loop.
        let seq_db_id = self
            .get_database_id_or_err(&tenant_dbname, "create_table")
            .await?
            .map_err(|e| KVAppError::AppError(AppError::UnknownDatabase(e)))?;

        // fixed
        let key_dbid = seq_db_id.data;

        // fixed
        let key_dbid_tbname = DBIdTableName {
            db_id: *seq_db_id.data,
            table_name: req.name_ident.table_name.clone(),
        };

        // fixed
        let key_table_id_list = TableIdHistoryIdent {
            database_id: *seq_db_id.data,
            table_name: req.name_ident.table_name.clone(),
        };
        // if req.as_dropped, append new table id to orphan table id list
        let (orphan_table_name, save_key_table_id_list) = if req.as_dropped {
            let now = Utc::now().timestamp_micros();
            let orphan_table_name = format!("{}@{}", ORPHAN_POSTFIX, now);
            (Some(orphan_table_name.clone()), TableIdHistoryIdent {
                database_id: *seq_db_id.data,
                table_name: orphan_table_name,
            })
        } else {
            (None, key_table_id_list.clone())
        };

        // The keys of values to re-fetch for every retry in this txn.
        let keys = vec![
            key_dbid.to_string_key(),
            key_dbid_tbname.to_string_key(),
            key_table_id_list.to_string_key(),
        ];

        // Initialize required key-values
        let mut data = {
            let values = self.mget_kv(&keys).await?;
            keys.iter()
                .zip(values.into_iter())
                .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                .collect::<Vec<_>>()
        };

        validate_index_columns(&req.table_meta)?;

        let mut trials = txn_backoff(None, func_name!());

        loop {
            trials.next().unwrap()?.await;

            // When retrying, data is cleared and re-fetched in one shot.
            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

            let db_meta = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<DatabaseId>(d)?;
                assert_eq!(key_dbid, k);

                v.ok_or_else(|| {
                    AppError::UnknownDatabaseId(UnknownDatabaseId::new(
                        *seq_db_id.data,
                        format!("{}: {}", func_name!(), seq_db_id.data),
                    ))
                })?
            };

            let mut txn = TxnRequest::default();

            let seq_table_id = {
                let d = data.remove(0);
                let (k, v) = deserialize_id_get_response::<DBIdTableName>(d)?;
                assert_eq!(key_dbid_tbname, k);

                if let Some(id) = v {
                    // TODO: move if_not_exists to upper caller. It is not duty of SchemaApi.
                    match req.create_option {
                        CreateOption::Create => {
                            let app_err = make_exists_err(&req);
                            return Err(KVAppError::AppError(app_err));
                        }
                        CreateOption::CreateIfNotExists => {
                            return Ok(CreateTableReply {
                                table_id: *id.data,
                                table_id_seq: None,
                                db_id: *seq_db_id.data,
                                new_table: false,
                                spec_vec: None,
                                prev_table_id: None,
                                orphan_table_name: None,
                            });
                        }
                        CreateOption::CreateOrReplace => {
                            if req.as_dropped {
                                // If the table is being created as a dropped table, we do not
                                // need to combine with drop_table_txn operations, just return
                                // the sequence number associated with the value part of
                                // the key-value pair (key_dbid_tbname, table_id).

                                SeqV::new(id.seq, *id.data)
                            } else {
                                let (seq, id) = construct_drop_table_txn_operations(
                                    self,
                                    req.name_ident.table_name.clone(),
                                    &req.name_ident.tenant,
                                    req.catalog_name.clone(),
                                    *id.data,
                                    *seq_db_id.data,
                                    true,
                                    false,
                                    &mut txn,
                                )
                                .await?;
                                SeqV::new(seq, id)
                            }
                        }
                    }
                } else {
                    SeqV::new(0, 0)
                }
            };

            let (mut tb_id_list, prev_table_id, tb_id_list_seq) = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdHistoryIdent>(d)?;
                assert_eq!(key_table_id_list, k);

                let tb_id_list = v.unwrap_or_default();

                // if `as_dropped` is true, append new table id into a temp new table
                // if create table return success, table id will be moved to table id list,
                // else, it will be vacuum when `vacuum drop table`
                if req.as_dropped {
                    (
                        // a new TableIdList
                        TableIdList::new(),
                        // save last table id and check when commit table meta
                        tb_id_list.data.id_list.last().copied(),
                        // seq MUST be 0
                        0,
                    )
                } else {
                    (tb_id_list.data, None, tb_id_list.seq)
                }
            };

            // Table id is unique and does not need to re-generate in every loop.
            let key_table_id = match &maybe_key_table_id {
                None => {
                    let id = fetch_id(self, IdGenerator::table_id()).await?;
                    maybe_key_table_id = Some(TableId { table_id: id });
                    maybe_key_table_id.as_ref().unwrap()
                }
                Some(id) => id,
            };

            let table_id = key_table_id.table_id;

            let key_table_id_to_name = TableIdToName { table_id };

            debug!(
                key_table_id :? =(key_table_id),
                name :? =(tenant_dbname_tbname);
                "new table id"
            );

            {
                // append new table_id into list
                tb_id_list.append(table_id);
                let dbid_tbname_seq = seq_table_id.seq;

                txn.condition.extend(vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_seq(&key_dbid, Eq, db_meta.seq),
                    // no other table with the same name is inserted.
                    txn_cond_seq(&key_dbid_tbname, Eq, dbid_tbname_seq),
                    // no other table id with the same name is append.
                    txn_cond_seq(&save_key_table_id_list, Eq, tb_id_list_seq),
                ]);

                txn.if_then.extend(vec![
                    // Changing a table in a db has to update the seq of db_meta,
                    // to block the batch-delete-tables when deleting a db.
                    txn_put_pb(&key_dbid, &db_meta.data)?, /* (db_id) -> db_meta */
                    txn_put_pb(key_table_id, &req.table_meta)?, /* (tenant, db_id, tb_id) -> tb_meta */
                    txn_put_pb(&save_key_table_id_list, &tb_id_list)?, /* _fd_table_id_list/db_id/table_name -> tb_id_list */
                    // This record does not need to assert `table_id_to_name_key == 0`,
                    // Because this is a reverse index for db_id/table_name -> table_id, and it is unique.
                    txn_put_pb(&key_table_id_to_name, &key_dbid_tbname)?, /* __fd_table_id_to_name/db_id/table_name -> DBIdTableName */
                ]);

                if req.as_dropped {
                    // To create the table in a "dropped" state,
                    // - we intentionally omit the tuple (key_dbid_name, table_id).
                    //   This ensures the table remains invisible, and available to be vacuumed.
                    // - also, the `table_id_seq` of newly create table should be obtained.
                    //   The caller need to know the `table_id_seq` to manipulate the table more efficiently
                    //   This TxnOp::Get is(should be) the last operation in the `if_then` list.
                    txn.if_then.push(txn_get(key_table_id));
                } else {
                    // Otherwise, make newly created table visible by putting the tuple:
                    // (tenant, db_id, tb_name) -> tb_id
                    txn.if_then.push(txn_put_u64(&key_dbid_tbname, table_id)?)
                }

                for table_field in req.table_meta.schema.fields() {
                    let Some(auto_increment_expr) = table_field.auto_increment_expr() else {
                        continue;
                    };

                    let auto_increment_key =
                        AutoIncrementKey::new(table_id, table_field.column_id());
                    let storage_ident =
                        AutoIncrementStorageIdent::new_generic(req.tenant(), auto_increment_key);
                    let storage_value =
                        Id::new_typed(AutoIncrementStorageValue(auto_increment_expr.start));
                    txn.if_then
                        .extend(vec![txn_put_pb(&storage_ident, &storage_value)?]);
                }

                let (succ, responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(tenant_dbname_tbname),
                    key_table_id :? =(key_table_id),
                    succ = succ,
                    responses :? =(responses);
                    "create_table"
                );

                // extract the table_id_seq (if any) from the kv txn responses
                let table_id_seq = if req.as_dropped {
                    responses.last().and_then(|r| match &r.response {
                        Some(Response::Get(resp)) => resp.value.as_ref().map(|v| v.seq),
                        _ => None,
                    })
                } else {
                    None
                };

                if succ {
                    return Ok(CreateTableReply {
                        table_id,
                        table_id_seq,
                        db_id: *seq_db_id.data,
                        new_table: dbid_tbname_seq == 0,
                        spec_vec: None,
                        prev_table_id,
                        orphan_table_name,
                    });
                } else {
                    // re-run txn with re-fetched data
                    data = vec![];
                }
            }
        }
    }

    #[logcall::logcall("debug")]
    #[fastrace::trace]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply, KVAppError> {
        let table_id = req.tb_id;
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let tenant = &req.tenant;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let opt = construct_drop_table_txn_operations(
                self,
                req.table_name.clone(),
                &req.tenant,
                None,
                table_id,
                req.db_id,
                req.if_exists,
                true,
                &mut txn,
            )
            .await?;
            // seq == 0 means that req.if_exists == true and cannot find table meta,
            // in this case just return directly
            if opt.1 == 0 {
                return Ok(DropTableReply {});
            }

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                tenant :% =(tenant.display()),
                id :? =(&table_id),
                succ = succ;
                "drop_table_by_id"
            );
            if succ {
                return Ok(DropTableReply {});
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        handle_undrop_table(self, req).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_by_id(&self, req: UndropTableByIdReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        handle_undrop_table(self, req).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();
        let tenant_newdbname_newtbname = TableNameIdent {
            tenant: tenant_dbname_tbname.tenant.clone(),
            db_name: req.new_db_name.clone(),
            table_name: req.new_table_name.clone(),
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get db by name to ensure presence

            let (seq_db_id, db_meta) = get_db_or_err(self, &tenant_dbname, "rename_table").await?;

            // Get table by db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id: *seq_db_id.data,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
            if req.if_exists {
                if tb_id_seq == 0 {
                    // TODO: table does not exist, can not return table id.
                    return Ok(RenameTableReply { table_id: 0 });
                }
            } else {
                assert_table_exist(
                    tb_id_seq,
                    tenant_dbname_tbname,
                    "rename_table: src (db,table)",
                )?;
            }

            // get table id list from _fd_table_id_list/db_id/table_name
            let dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: *seq_db_id.data,
                table_name: req.name_ident.table_name.clone(),
            };

            let seq_table_history = self.get_pb(&dbid_tbname_idlist).await?;

            let tb_id_list_seq = seq_table_history.seq();
            // may the table is created before add db_id_list,
            // so we just add the id into the list.
            let mut tb_id_list = seq_table_history
                .into_value()
                .unwrap_or_else(|| TableIdList::new_with_ids([table_id]));

            assert_table_id_is_latest(
                table_id,
                &tb_id_list,
                &req.name_ident.table_name,
                "rename_table",
            )?;

            // Get the renaming target db to ensure presence.

            let tenant_new_dbname =
                DatabaseNameIdent::new(tenant_dbname.tenant(), &req.new_db_name);
            let (new_seq_db_id, new_db_meta) =
                get_db_or_err(self, &tenant_new_dbname, "rename_table: new db").await?;

            // Get the renaming target table to ensure absence

            let newdbid_newtbname = DBIdTableName {
                db_id: *new_seq_db_id.data,
                table_name: req.new_table_name.clone(),
            };
            let (new_tb_id_seq, _new_tb_id) = get_u64_value(self, &newdbid_newtbname).await?;
            table_has_to_not_exist(new_tb_id_seq, &tenant_newdbname_newtbname, "rename_table")?;

            let new_dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: *new_seq_db_id.data,
                table_name: req.new_table_name.clone(),
            };

            let seq_list = self.get_pb(&new_dbid_tbname_idlist).await?;

            let new_tb_id_list_seq = seq_list.seq();

            let mut new_tb_id_list = if let Some(seq_list) = seq_list {
                seq_list.data
            } else {
                TableIdList::new()
            };

            // get table id name
            let table_id_to_name_key = TableIdToName { table_id };
            let table_id_to_name_seq = self.get_seq(&table_id_to_name_key).await?;

            let db_id_table_name = DBIdTableName {
                db_id: *new_seq_db_id.data,
                table_name: req.new_table_name.clone(),
            };

            {
                // move table id from old table id list to new table id list
                tb_id_list.pop();
                new_tb_id_list.append(table_id);

                let mut txn = TxnRequest::new(
                    vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&seq_db_id.data, Eq, db_meta.seq),
                        txn_cond_seq(&new_seq_db_id.data, Eq, new_db_meta.seq),
                        // table_name->table_id does not change.
                        // Updating the table meta is ok.
                        txn_cond_seq(&dbid_tbname, Eq, tb_id_seq),
                        txn_cond_seq(&newdbid_newtbname, Eq, 0),
                        // no other table id with the same name is append.
                        txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq),
                        txn_cond_seq(&new_dbid_tbname_idlist, Eq, new_tb_id_list_seq),
                        txn_cond_seq(&table_id_to_name_key, Eq, table_id_to_name_seq),
                    ],
                    vec![
                        txn_del(&dbid_tbname),                      // (db_id, tb_name) -> tb_id
                        txn_put_u64(&newdbid_newtbname, table_id)?, /* (db_id, new_tb_name) -> tb_id */
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_put_pb(&seq_db_id.data, &*db_meta)?, // (db_id) -> db_meta
                        txn_put_pb(&dbid_tbname_idlist, &tb_id_list)?, /* _fd_table_id_list/db_id/old_table_name -> tb_id_list */
                        txn_put_pb(&new_dbid_tbname_idlist, &new_tb_id_list)?, /* _fd_table_id_list/db_id/new_table_name -> tb_id_list */
                        txn_put_pb(&table_id_to_name_key, &db_id_table_name)?, /* __fd_table_id_to_name/db_id/table_name -> DBIdTableName */
                    ],
                );

                if *seq_db_id.data != *new_seq_db_id.data {
                    txn.if_then
                        .push(txn_put_pb(&new_seq_db_id.data, &*new_db_meta)?); // (db_id) -> db_meta
                }

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(tenant_dbname_tbname),
                    to :? =(&newdbid_newtbname),
                    table_id :? =(&table_id),
                    succ = succ;
                    "rename_table"
                );

                if succ {
                    return Ok(RenameTableReply { table_id });
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn swap_table(&self, req: SwapTableReq) -> Result<SwapTableReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get databases
            let tenant_dbname_left = req.origin_table.db_name_ident();

            let (seq_db_id_left, db_meta_left) =
                get_db_or_err(self, &tenant_dbname_left, "swap_table: tenant_dbname_left").await?;

            let dbid_tbname_left = DBIdTableName {
                db_id: *seq_db_id_left.data,
                table_name: req.origin_table.table_name.clone(),
            };

            let (tb_id_seq_left, table_id_left) = get_u64_value(self, &dbid_tbname_left).await?;
            if req.if_exists && tb_id_seq_left == 0 {
                return Ok(SwapTableReply {});
            }
            assert_table_exist(
                tb_id_seq_left,
                &req.origin_table,
                "swap_table: origin_table",
            )?;

            let dbid_tbname_right = DBIdTableName {
                db_id: *seq_db_id_left.data,
                table_name: req.target_table_name.clone(),
            };

            let (tb_id_seq_right, table_id_right) = get_u64_value(self, &dbid_tbname_right).await?;
            if req.if_exists && tb_id_seq_right == 0 {
                return Ok(SwapTableReply {});
            }
            assert_table_exist(
                tb_id_seq_right,
                &TableNameIdent {
                    tenant: req.origin_table.tenant.clone(),
                    db_name: req.origin_table.db_name.clone(),
                    table_name: req.target_table_name.clone(),
                },
                "swap_table: target_table",
            )?;

            // Get table id lists
            let dbid_tbname_idlist_left = TableIdHistoryIdent {
                database_id: *seq_db_id_left.data,
                table_name: req.origin_table.table_name.clone(),
            };
            let dbid_tbname_idlist_right = TableIdHistoryIdent {
                database_id: *seq_db_id_left.data,
                table_name: req.target_table_name.clone(),
            };

            let seq_table_history_left = self.get_pb(&dbid_tbname_idlist_left).await?;
            let seq_table_history_right = self.get_pb(&dbid_tbname_idlist_right).await?;

            let tb_id_list_seq_left = seq_table_history_left.seq();
            let tb_id_list_seq_right = seq_table_history_right.seq();

            let mut tb_id_list_left = seq_table_history_left
                .into_value()
                .unwrap_or_else(|| TableIdList::new_with_ids([table_id_left]));
            let mut tb_id_list_right = seq_table_history_right
                .into_value()
                .unwrap_or_else(|| TableIdList::new_with_ids([table_id_right]));

            assert_table_id_is_latest(
                table_id_left,
                &tb_id_list_left,
                &req.origin_table.table_name,
                "swap_table",
            )?;
            assert_table_id_is_latest(
                table_id_right,
                &tb_id_list_right,
                &req.target_table_name,
                "swap_table",
            )?;

            // Get table id to name mappings
            let table_id_to_name_key_left = TableIdToName {
                table_id: table_id_left,
            };
            let table_id_to_name_key_right = TableIdToName {
                table_id: table_id_right,
            };
            let table_id_to_name_seq_left = self.get_seq(&table_id_to_name_key_left).await?;
            let table_id_to_name_seq_right = self.get_seq(&table_id_to_name_key_right).await?;

            // Prepare new mappings after swap
            let db_id_table_name_left = DBIdTableName {
                db_id: *seq_db_id_left.data,
                table_name: req.origin_table.table_name.clone(),
            };
            let db_id_table_name_right = DBIdTableName {
                db_id: *seq_db_id_left.data,
                table_name: req.target_table_name.clone(),
            };

            {
                // Update history lists: remove current table IDs
                tb_id_list_left.pop();
                tb_id_list_right.pop();
                // Add swapped table IDs
                tb_id_list_left.append(table_id_right);
                tb_id_list_right.append(table_id_left);

                let txn = TxnRequest::new(
                    vec![
                        // Ensure databases haven't changed
                        txn_cond_seq(&seq_db_id_left.data, Eq, db_meta_left.seq),
                        // Ensure table name->table_id mappings haven't changed
                        txn_cond_seq(&dbid_tbname_left, Eq, tb_id_seq_left),
                        txn_cond_seq(&dbid_tbname_right, Eq, tb_id_seq_right),
                        // Ensure table history lists haven't changed
                        txn_cond_seq(&dbid_tbname_idlist_left, Eq, tb_id_list_seq_left),
                        txn_cond_seq(&dbid_tbname_idlist_right, Eq, tb_id_list_seq_right),
                        // Ensure table_id->name mappings haven't changed
                        txn_cond_seq(&table_id_to_name_key_left, Eq, table_id_to_name_seq_left),
                        txn_cond_seq(&table_id_to_name_key_right, Eq, table_id_to_name_seq_right),
                    ],
                    vec![
                        // Swap table name->table_id mappings
                        txn_put_u64(&dbid_tbname_left, table_id_right)?, /* origin_table_name -> target_table_id */
                        txn_put_u64(&dbid_tbname_right, table_id_left)?, /* target_table_name -> origin_table_id */
                        // Update database metadata sequences
                        txn_put_pb(&seq_db_id_left.data, &*db_meta_left)?,
                        // Update table history lists
                        txn_put_pb(&dbid_tbname_idlist_left, &tb_id_list_left)?,
                        txn_put_pb(&dbid_tbname_idlist_right, &tb_id_list_right)?,
                        // Update table_id->name mappings
                        txn_put_pb(&table_id_to_name_key_left, &db_id_table_name_right)?, /* origin_table_id -> target_table_name */
                        txn_put_pb(&table_id_to_name_key_right, &db_id_table_name_left)?, /* target_table_id -> origin_table_name */
                    ],
                );

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    origin_table :? =(&req.origin_table),
                    target_table_name :? =(&req.target_table_name),
                    table_id_left :? =(&table_id_left),
                    table_id_right :? =(&table_id_right),
                    succ = succ;
                    "swap_table"
                );

                if succ {
                    return Ok(SwapTableReply {});
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn truncate_table(
        &self,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply, KVAppError> {
        // NOTE: this method read and remove in multiple transactions.
        // It is not atomic, but it is safe because it deletes only the files that matches the seq.

        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let table_id = TableId {
            table_id: req.table_id,
        };

        let chunk_size = req.batch_size.unwrap_or(DEFAULT_MGET_SIZE as u64);

        // 1. Grab a snapshot view of the copied files of a table.
        //
        // If table seq is not changed before and after listing, we can be sure the list of copied
        // files is consistent to this version of the table.

        let mut seq_1 = self.get_seq(&table_id).await?;

        let mut trials = txn_backoff(None, func_name!());
        let copied_files = loop {
            trials.next().unwrap()?.await;

            let copied_file_ident = TableCopiedFileNameIdent {
                table_id: table_id.table_id,
                file: "dummy".to_string(),
            };
            let dir_name = DirName::new(copied_file_ident);
            let copied_files = self.list_pb_vec(ListOptions::unlimited(&dir_name)).await?;

            let seq_2 = self.get_seq(&table_id).await?;

            if seq_1 == seq_2 {
                debug!(
                    "list all copied file of table {}: {} files",
                    table_id.table_id,
                    copied_files.len()
                );
                break copied_files;
            } else {
                seq_1 = seq_2;
            }
        };

        // 2. Remove the copied files only when the seq of a copied file has not changed.
        //
        // - We do not assert the seq of each copied file in each transaction, since we only delete
        //   non-changed ones.

        for chunk in copied_files.chunks(chunk_size as usize) {
            let txn = TxnRequest::new(
                vec![],
                chunk
                    .iter()
                    .map(|(name, seq_file)| {
                        TxnOp::delete_exact(name.to_string_key(), Some(seq_file.seq()))
                    })
                    .collect(),
            );

            let (_succ, _responses) = send_txn(self, txn).await?;
        }

        Ok(TruncateTableReply {})
    }

    // make table meta visible by:
    // 1. move table id from orphan table id list to table id list
    // 2. set table meta.drop_on as None
    #[logcall::logcall]
    #[fastrace::trace]
    async fn commit_table_meta(
        &self,
        req: CommitTableMetaReq,
    ) -> Result<CommitTableMetaReply, KVAppError> {
        let table_id = req.table_id;
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;

        let txn_sender = IdempotentKVTxnSender::new();

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get db by name to ensure presence

            let (db_meta_seq, db_meta) =
                get_db_by_id_or_err(self, req.db_id, "commit_table_meta").await?;
            let db_id = req.db_id;

            // Get table by tenant,db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let (dbid_tbname_seq, _table_id) = get_u64_value(self, &dbid_tbname).await?;

            // get table id list from _fd_table_id_list/db_id/table_name

            let orphan_dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: db_id,
                table_name: req.orphan_table_name.clone().unwrap(),
            };
            let dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: db_id,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let keys = vec![
                orphan_dbid_tbname_idlist.to_string_key(),
                dbid_tbname_idlist.to_string_key(),
            ];

            let mut data = {
                let values = self.mget_kv(&keys).await?;
                keys.iter()
                    .zip(values.into_iter())
                    .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                    .collect::<Vec<_>>()
            };

            let orphan_tb_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdHistoryIdent>(d)?;
                assert_eq!(orphan_dbid_tbname_idlist, k);

                v.unwrap_or_default()
            };
            if orphan_tb_id_list.data.id_list.len() != 1 {
                error!("table {:?} orphan list is empty", tenant_dbname_tbname);
                let exist_err = CommitTableMetaError::new(
                    tenant_dbname_tbname.table_name.clone(),
                    "orphan list length != 1".to_string(),
                );
                return Err(KVAppError::AppError(AppError::from(exist_err)));
            }

            let mut tb_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdHistoryIdent>(d)?;
                assert_eq!(dbid_tbname_idlist, k);

                v.unwrap_or_default()
            };

            if tb_id_list.data.id_list.last() != req.prev_table_id.as_ref() {
                error!(
                    "table {:?} table id list has been changed",
                    tenant_dbname_tbname
                );
                let exist_err = CommitTableMetaError::new(
                    tenant_dbname_tbname.table_name.clone(),
                    "prev_table_id has been changed".to_string(),
                );
                return Err(KVAppError::AppError(AppError::from(exist_err)));
            }

            let table_id = match orphan_tb_id_list.data.id_list.last() {
                Some(table_id) => *table_id,
                None => {
                    return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                        UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                    )));
                }
            };
            tb_id_list.data.id_list.push(table_id);

            // get tb_meta of the last table id
            let tbid = TableId { table_id };
            let (tb_meta_seq, tb_meta) = self.get_pb_seq_and_value(&tbid).await?;

            debug!(
                ident :% =(&tbid),
                name :% =(tenant_dbname_tbname);
                "commit_table_meta"
            );

            {
                // reset drop on time
                let mut tb_meta = tb_meta.unwrap();
                // undrop a table with no drop_on time
                if tb_meta.drop_on.is_none() {
                    return Err(KVAppError::AppError(AppError::UndropTableWithNoDropTime(
                        UndropTableWithNoDropTime::new(&tenant_dbname_tbname.table_name),
                    )));
                }
                tb_meta.drop_on = None;

                let txn_req = TxnRequest::new(
                    vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
                        // still this table id
                        txn_cond_seq(&dbid_tbname, Eq, dbid_tbname_seq),
                        // table is not changed
                        txn_cond_seq(&tbid, Eq, tb_meta_seq),
                        txn_cond_seq(&orphan_dbid_tbname_idlist, Eq, orphan_tb_id_list.seq),
                        txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list.seq),
                    ],
                    vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_put_pb(&DatabaseId { db_id }, &db_meta)?, // (db_id) -> db_meta
                        txn_put_u64(&dbid_tbname, table_id)?, /* (tenant, db_id, tb_name) -> tb_id */
                        // txn_put_pb(&dbid_tbname_idlist, &tb_id_list)?, // _fd_table_id_list/db_id/table_name -> tb_id_list
                        txn_put_pb(&tbid, &tb_meta)?, // (tenant, db_id, tb_id) -> tb_meta
                        txn_del(&orphan_dbid_tbname_idlist), // del orphan table idlist
                        txn_put_pb(&dbid_tbname_idlist, &tb_id_list.data)?, /* _fd_table_id_list/db_id/table_name -> tb_id_list */
                    ],
                );

                let txn_response = txn_sender.send_txn(self, txn_req).await?;
                let succ = match txn_response {
                    IdempotentKVTxnResponse::Success(_) => true,
                    IdempotentKVTxnResponse::AlreadyCommitted => {
                        info!(
                            "Transaction ID {} exists, the corresponding commit_table_meta transaction has been executed successfully",
                            txn_sender.get_txn_id()
                        );
                        true
                    }
                    IdempotentKVTxnResponse::Failed(_) => false,
                };

                debug!(
                    name :? =(tenant_dbname_tbname),
                    id :? =(&tbid),
                    succ = succ;
                    "commit_table_meta"
                );

                if succ {
                    return Ok(CommitTableMetaReply {});
                }
            }
        }
    }

    async fn update_multi_table_meta(
        &self,
        req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult, KVAppError> {
        let kv_txn_sender = IdempotentKVTxnSender::new();
        self.update_multi_table_meta_with_sender(req, &kv_txn_sender)
            .await
    }

    async fn update_multi_table_meta_with_sender(
        &self,
        req: UpdateMultiTableMetaReq,
        txn_sender: &IdempotentKVTxnSender,
    ) -> Result<UpdateMultiTableMetaResult, KVAppError> {
        let UpdateMultiTableMetaReq {
            mut update_table_metas,
            copied_files,
            update_stream_metas,
            deduplicated_labels,
            update_temp_tables: _,
        } = req;

        let mut tbl_seqs = HashMap::new();
        let mut txn = TxnRequest::default();
        let mut mismatched_tbs = vec![];
        let tid_vec = update_table_metas
            .iter()
            .map(|req| {
                TableId {
                    table_id: req.0.table_id,
                }
                .to_string_key()
            })
            .collect::<Vec<_>>();
        let mut tb_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(self, &tid_vec).await?;
        for ((req, _), (tb_meta_seq, table_meta)) in
            update_table_metas.iter().zip(tb_meta_vec.iter_mut())
        {
            let req_seq = req.seq;

            if *tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "update_multi_table_meta"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                mismatched_tbs.push((
                    req.table_id,
                    *tb_meta_seq,
                    std::mem::take(table_meta).unwrap(),
                ));
            }
        }

        if !mismatched_tbs.is_empty() {
            return Ok(Err(mismatched_tbs));
        }

        let mut new_table_meta_map: BTreeMap<u64, TableMeta> = BTreeMap::new();
        for ((req, _), (tb_meta_seq, table_meta)) in
            update_table_metas.iter_mut().zip(tb_meta_vec.iter())
        {
            let tbid = TableId {
                table_id: req.table_id,
            };
            // `update_table_meta` MUST NOT modify `shared_by` field
            let table_meta = table_meta.as_ref().unwrap();

            let mut new_table_meta = req.new_table_meta.clone();
            new_table_meta.shared_by = table_meta.shared_by.clone();

            tbl_seqs.insert(req.table_id, *tb_meta_seq);
            txn.condition.push(txn_cond_seq(&tbid, Eq, *tb_meta_seq));

            // Add LVT check if provided
            if let Some(check) = req.lvt_check.as_ref() {
                let lvt_ident = LeastVisibleTimeIdent::new(&check.tenant, req.table_id);
                let res = self.get_pb(&lvt_ident).await?;
                let (seq, current_lvt) = match res {
                    Some(v) => (v.seq, Some(v.data)),
                    None => (0, None),
                };
                if let Some(current_lvt) = current_lvt {
                    if current_lvt.time > check.time {
                        return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                            TableSnapshotExpired::new(
                                req.table_id,
                                format!(
                                    "snapshot timestamp {:?} is older than the table's least visible time {:?}",
                                    check.time, current_lvt.time
                                ),
                            ),
                        )));
                    }
                }
                // no other one has updated LVT since we read it
                txn.condition.push(txn_cond_seq(&lvt_ident, Eq, seq));
            }

            txn.if_then.push(txn_put_pb(&tbid, &new_table_meta)?);
            txn.else_then.push(TxnOp {
                request: Some(Request::Get(TxnGetRequest::new(tbid.to_string_key()))),
            });

            new_table_meta_map.insert(req.table_id, new_table_meta);
        }

        // `remove_table_copied_files` and `upsert_table_copied_file_info`
        // all modify `TableCopiedFileInfo`,
        // so there used to has `TableCopiedFileLockKey` in these two functions
        // to protect TableCopiedFileInfo modification.
        // In issue: https://github.com/datafuselabs/databend/issues/8897,
        // there is chance that if copy files concurrently, `upsert_table_copied_file_info`
        // may return `TxnRetryMaxTimes`.
        // So now, in case that `TableCopiedFileInfo` has expire time, remove `TableCopiedFileLockKey`
        // in each function. In this case there is chance that some `TableCopiedFileInfo` may not be
        // removed in `remove_table_copied_files`, but these data can be purged in case of expire time.

        let insert_if_not_exists_table_ids = copied_files
            .iter()
            .filter(|(_, req)| req.insert_if_not_exists)
            .map(|(table_id, _)| *table_id)
            .collect::<Vec<_>>();

        for (table_id, req) in copied_files {
            let tbid = TableId { table_id };

            let table_meta_seq = tbl_seqs[&tbid.table_id];
            txn.condition.push(txn_cond_eq_seq(&tbid, table_meta_seq));

            for (file_name, file_info) in req.file_info {
                let key = TableCopiedFileNameIdent {
                    table_id: tbid.table_id,
                    file: file_name,
                };

                if req.insert_if_not_exists {
                    txn.condition.push(txn_cond_eq_seq(&key, 0));
                }
                txn.if_then
                    .push(txn_put_pb_with_ttl(&key, &file_info, req.ttl)?)
            }
        }

        let sid_vec = update_stream_metas
            .iter()
            .map(|req| {
                TableId {
                    table_id: req.stream_id,
                }
                .to_string_key()
            })
            .collect::<Vec<_>>();
        let stream_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(self, &sid_vec).await?;
        for (req, (stream_meta_seq, stream_meta)) in
            update_stream_metas.iter().zip(stream_meta_vec.into_iter())
        {
            let stream_id = TableId {
                table_id: req.stream_id,
            };

            if stream_meta_seq == 0 || stream_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownStreamId(
                    UnknownStreamId::new(req.stream_id, "update_multi_table_meta"),
                )));
            }

            if req.seq.match_seq(&stream_meta_seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    StreamVersionMismatched::new(
                        req.stream_id,
                        req.seq,
                        stream_meta_seq,
                        "update_multi_table_meta",
                    ),
                )));
            }

            let mut new_stream_meta = stream_meta.unwrap();
            new_stream_meta.options = req.options.clone();
            new_stream_meta.updated_on = Utc::now();

            txn.condition
                .push(txn_cond_seq(&stream_id, Eq, stream_meta_seq));
            txn.if_then.push(txn_put_pb(&stream_id, &new_stream_meta)?);
        }

        for deduplicated_label in deduplicated_labels {
            txn.if_then
                .push(build_upsert_table_deduplicated_label(deduplicated_label));
        }

        let txn_response = txn_sender.send_txn(self, txn).await?;

        let else_branch_op_responses = match txn_response {
            IdempotentKVTxnResponse::Success(_) => {
                return Ok(Ok(UpdateTableMetaReply {}));
            }
            IdempotentKVTxnResponse::AlreadyCommitted => {
                info!(
                    "Transaction ID {} exists, the corresponding update_multi_table_meta transaction has been executed successfully",
                    txn_sender.get_txn_id()
                );
                return Ok(Ok(UpdateTableMetaReply {}));
            }
            IdempotentKVTxnResponse::Failed(op_responses) => op_responses,
        };

        let mut mismatched_tbs = vec![];
        for (resp, req) in else_branch_op_responses
            .iter()
            .zip(update_table_metas.iter())
        {
            let Some(Response::Get(get_resp)) = &resp.response else {
                unreachable!(
                    "internal error: expect some TxnGetResponseGet, but got {:?}",
                    resp.response
                )
            };
            // deserialize table version info
            let (tb_meta_seq, table_meta): (_, TableMeta) = if let Some(seq_v) = &get_resp.value {
                (seq_v.seq, deserialize_struct(&seq_v.data)?)
            } else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.0.table_id, "update_multi_table_meta"),
                )));
            };

            // check table version
            if req.0.seq.match_seq(&tb_meta_seq).is_err() {
                mismatched_tbs.push((req.0.table_id, tb_meta_seq, table_meta));
            }
        }

        if mismatched_tbs.is_empty() {
            if !insert_if_not_exists_table_ids.is_empty() {
                // If insert_if_not_exists is true and transaction failed, it's likely due to duplicated files
                Err(KVAppError::AppError(AppError::from(
                    DuplicatedUpsertFiles::new(
                        insert_if_not_exists_table_ids,
                        "update_multi_table_meta",
                    ),
                )))
            } else {
                // if all table version does match, but tx failed, we don't know why, just return error
                Err(KVAppError::AppError(AppError::from(
                    MultiStmtTxnCommitFailed::new("update_multi_table_meta"),
                )))
            }
        } else {
            // up layer will retry
            Ok(Err(mismatched_tbs))
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };

        self.crud_upsert_with(&tbid, |seq_meta: Option<SeqV<TableMeta>>| {
            let Some(seq_meta) = seq_meta else {
                return Err(AppError::UnknownTableId(UnknownTableId::new(
                    req.table_id,
                    "upsert_table_option",
                )));
            };

            if req.seq.match_seq(&seq_meta).is_err() {
                return Err(AppError::from(TableVersionMismatched::new(
                    req.table_id,
                    req.seq,
                    seq_meta.seq(),
                    "upsert_table_option",
                )));
            }

            let mut table_meta = seq_meta.data;

            for (k, opt_v) in &req.options {
                match opt_v {
                    None => {
                        table_meta.options.remove(k);
                    }
                    Some(v) => {
                        table_meta.options.insert(k.to_string(), v.to_string());
                    }
                }
            }

            Ok(Some(table_meta))
        })
        .await??;

        Ok(UpsertTableOptionReply {})
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.inner;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();

        // Get db by name to ensure presence

        let res = get_db_or_err(
            self,
            &tenant_dbname,
            format!("get_table: {}", tenant_dbname.display()),
        )
        .await;

        let (seq_db_id, _db_meta) = res?;

        let dbid_tbname = DBIdTableName {
            db_id: *seq_db_id.data,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };

        let table_niv = self.get_table_in_db(&dbid_tbname).await?;

        let Some(table_niv) = table_niv else {
            return Err(AppError::from(UnknownTable::new(
                &tenant_dbname_tbname.table_name,
                format!("get_table: {}", tenant_dbname_tbname),
            ))
            .into());
        };

        let (_name, id, seq_meta) = table_niv.unpack();

        let db_type = DatabaseType::NormalDB;

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id: id.table_id,
                seq: seq_meta.seq,
            },
            desc: format!(
                "'{}'.'{}'",
                tenant_dbname.database_name(),
                tenant_dbname_tbname.table_name
            ),
            name: tenant_dbname_tbname.table_name.clone(),
            meta: seq_meta.data,
            db_type,
            catalog_info: Default::default(),
        };

        return Ok(Arc::new(tb_info));
    }

    /// Get multiple tables by db_id and table names in batch.
    /// Returns TableInfo for tables that exist, in the same order as input.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_tables(
        &self,
        db_id: u64,
        db_name: &str,
        table_names: &[String],
    ) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
        debug!(db_id = db_id, table_names :? = table_names; "TableApi: {}", func_name!());

        // Build DBIdTableName keys for all table names
        let dbid_tbnames: Vec<DBIdTableName> = table_names
            .iter()
            .map(|name| DBIdTableName::new(db_id, name))
            .collect();

        // Batch get table ids
        let table_ids = mget_u64_values(self, &dbid_tbnames).await?;

        // Collect valid table ids with their names
        let mut valid_tables: Vec<(String, u64)> = Vec::with_capacity(table_names.len());
        for (dbid_tbname, table_id_opt) in dbid_tbnames.into_iter().zip(table_ids.into_iter()) {
            if let Some(table_id) = table_id_opt {
                valid_tables.push((dbid_tbname.table_name, table_id));
            }
        }

        if valid_tables.is_empty() {
            return Ok(vec![]);
        }

        // Batch get table metas
        let table_id_keys: Vec<TableId> = valid_tables
            .iter()
            .map(|(_, id)| TableId { table_id: *id })
            .collect();
        let seq_metas = self.get_pb_values_vec(table_id_keys).await?;

        // Build TableInfo for valid tables
        let db_type = DatabaseType::NormalDB;
        let mut results = Vec::with_capacity(valid_tables.len());
        for ((table_name, table_id), seq_meta_opt) in
            valid_tables.into_iter().zip(seq_metas.into_iter())
        {
            if let Some(seq_meta) = seq_meta_opt {
                let tb_info = TableInfo {
                    ident: TableIdent {
                        table_id,
                        seq: seq_meta.seq,
                    },
                    desc: format!("'{}'.'{}'", db_name, table_name),
                    name: table_name,
                    meta: seq_meta.data,
                    db_type: db_type.clone(),
                    catalog_info: Default::default(),
                };
                results.push(Arc::new(tb_info));
            }
        }

        Ok(results)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<Option<SeqV<TableMeta>>, MetaError> {
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let id = TableId { table_id };

        let seq_table_meta = self.get_pb(&id).await?;
        Ok(seq_table_meta)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_in_db(
        &self,
        name_ident: &DBIdTableName,
    ) -> Result<Option<TableNIV>, MetaError> {
        debug!(req :? =(name_ident); "SchemaApi: {}", func_name!());

        let table_id = {
            // Get table by tenant, db_id, table_name to assert presence.

            let (tb_id_seq, table_id) = get_u64_value(self, name_ident).await?;
            if tb_id_seq == 0 {
                return Ok(None);
            }

            table_id
        };

        let tbid = TableId { table_id };

        let seq_meta = self.get_pb(&tbid).await?;

        let Some(seq_meta) = seq_meta else {
            return Ok(None);
        };

        Ok(Some(TableNIV::new(name_ident.clone(), tbid, seq_meta)))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tables(
        &self,
        req: ListTableReq,
    ) -> Result<Vec<(String, TableId, SeqV<TableMeta>)>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let dbid_tbname = DBIdTableName {
            db_id: req.database_id.db_id,
            // Use empty name to scan all tables
            table_name: "".to_string(),
        };

        let (names, ids) = list_u64_value(self, &dbid_tbname).await?;

        let ids = ids
            .into_iter()
            .map(|id| TableId { table_id: id })
            .collect::<Vec<_>>();

        let seq_metas = self.get_pb_values_vec(ids.clone()).await?;

        let res = names
            .into_iter()
            .zip(ids)
            .zip(seq_metas)
            .filter_map(|((n, id), seq_meta)| seq_meta.map(|x| (n.table_name, id, x)))
            .collect::<Vec<_>>();
        Ok(res)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_history_tables(
        &self,
        include_non_retainable: bool,
        req: ListTableReq,
    ) -> Result<Vec<TableNIV>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // List tables by tenant, db_id.
        let table_id_history_ident = TableIdHistoryIdent {
            database_id: req.database_id.db_id,
            table_name: "dummy".to_string(),
        };

        let dir_name = DirName::new(table_id_history_ident);

        let ident_histories = self.list_pb_vec(ListOptions::unlimited(&dir_name)).await?;

        let mut res = vec![];
        let now = Utc::now();

        for (ident, history) in ident_histories {
            debug!(name :% =(&ident); "get_tables_history");

            let id_metas =
                get_history_table_metas(self, include_non_retainable, &now, history.data).await?;

            let table_nivs = id_metas.into_iter().map(|(table_id, seq_meta)| {
                let name = DBIdTableName::new(ident.database_id, ident.table_name.clone());
                TableNIV::new(name, table_id, seq_meta)
            });

            res.extend(table_nivs);
        }

        return Ok(res);
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_table_names_by_ids(
        &self,
        table_ids: &[MetaId],
        get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>, KVAppError> {
        debug!(req :? =(&table_ids); "SchemaApi: {}", func_name!());

        let id_to_name_idents = table_ids.iter().map(|id| TableIdToName { table_id: *id });

        let seq_names = self.get_pb_values_vec(id_to_name_idents).await?;
        let mut table_names = seq_names
            .into_iter()
            .map(|seq_name| seq_name.map(|s| s.data.table_name))
            .collect::<Vec<_>>();

        let id_idents = table_ids.iter().map(|id| TableId { table_id: *id });
        let seq_metas = self.get_pb_values_vec(id_idents).await?;
        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                if seq_meta.data.drop_on.is_some() && !get_dropped_table {
                    table_names[i] = None;
                }
            } else {
                table_names[i] = None;
            }
        }

        Ok(table_names)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>, MetaError> {
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let table_id_to_name_key = TableIdToName { table_id };

        let seq_table_name = self.get_pb(&table_id_to_name_key).await?;

        debug!(ident :% =(&table_id_to_name_key); "get_table_name_by_id");

        let table_name = seq_table_name.map(|s| s.data.table_name);

        Ok(table_name)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;
        let tbid = TableId { table_id };

        let seq_meta = self.get_pb(&tbid).await?;

        let Some(seq_meta) = seq_meta else {
            return Err(KVAppError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(table_id, ""),
            )));
        };

        debug!(
            ident :% =(&tbid),
            table_meta :? =(&seq_meta);
            "get_table_copied_file_info"
        );

        let mut file_infos = BTreeMap::new();

        let mut keys = Vec::with_capacity(req.files.len());

        for file in req.files.iter() {
            let ident = TableCopiedFileNameIdent {
                table_id,
                file: file.clone(),
            };
            keys.push(ident.to_string_key());
        }

        let mut file_names = req.files.into_iter();

        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let seq_infos = mget_pb_values(self, c).await?;

            for (_seq, file_info) in seq_infos {
                let f_name = file_names.next().unwrap();

                if let Some(f_info) = file_info {
                    file_infos.insert(f_name, f_info);
                }
            }
        }

        Ok(GetTableCopiedFileReply {
            file_info: file_infos,
        })
    }

    async fn list_table_copied_file_info(
        &self,
        table_id: u64,
    ) -> Result<ListTableCopiedFileReply, MetaError> {
        let key = TableCopiedFileNameIdent {
            table_id,
            file: "".to_string(),
        };

        let res = self
            .list_pb_vec(ListOptions::unlimited(&DirName::new(key)))
            .await?;
        let mut file_info = BTreeMap::new();
        for (name_key, seqv) in res {
            file_info.insert(name_key.file, seqv.data);
        }

        Ok(ListTableCopiedFileReply { file_info })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_retainable_tables(
        &self,
        history_ident: &TableIdHistoryIdent,
    ) -> Result<Vec<(TableId, SeqV<TableMeta>)>, MetaError> {
        let Some(seq_table_id_list) = self.get_pb(history_ident).await? else {
            return Ok(vec![]);
        };

        get_history_table_metas(self, false, &Utc::now(), seq_table_id_list.data).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<ListDroppedTableResp, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let the_limit = req.limit.unwrap_or(usize::MAX);

        let drop_time_range = req.drop_time_range;

        if req.database_name.is_none() {
            let db_infos = self
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: req.tenant.clone(),
                    },
                    true,
                )
                .await?;

            let mut vacuum_tables = vec![];
            let mut vacuum_ids = vec![];

            for db_info in db_infos {
                if vacuum_tables.len() >= the_limit {
                    return Ok(ListDroppedTableResp {
                        vacuum_tables,
                        drop_ids: vacuum_ids,
                    });
                }

                let vacuum_db = drop_time_range.contains(&db_info.meta.drop_on);

                // If to vacuum a db, just vacuum all tables.
                // Otherwise, choose only dropped tables(before retention time).
                let table_drop_time_range = if vacuum_db {
                    None..Some(DateTime::<Utc>::MAX_UTC)
                } else {
                    drop_time_range.clone()
                };

                let capacity = the_limit - vacuum_tables.len();
                let table_nivs = get_history_tables_for_gc(
                    self,
                    table_drop_time_range,
                    db_info.database_id.db_id,
                    capacity,
                    vacuum_db,
                )
                .await?;

                for table_niv in table_nivs.iter() {
                    vacuum_ids.push(DroppedId::from(table_niv.clone()));
                }

                let db_name = db_info.name_ident.database_name().to_string();
                let db_name_ident = db_info.name_ident.clone();

                // A DB can be removed only when all its tables are removed.
                if vacuum_db && capacity > table_nivs.len() {
                    vacuum_ids.push(DroppedId::Db {
                        db_id: db_info.database_id.db_id,
                        db_name: db_name.clone(),
                    });
                }

                vacuum_tables.extend(std::iter::repeat(db_name_ident).zip(table_nivs));
            }

            return Ok(ListDroppedTableResp {
                vacuum_tables,
                drop_ids: vacuum_ids,
            });
        }

        let database_name = req.database_name.clone().unwrap();
        let tenant_dbname = DatabaseNameIdent::new(&req.tenant, database_name);

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            &tenant_dbname,
            format!("get_table_history: {}", tenant_dbname.display()),
        )
        .await;

        let (seq_db_id, _db_meta) = res?;

        let database_id = seq_db_id.data;
        let table_nivs = get_history_tables_for_gc(
            self,
            drop_time_range.clone(),
            database_id.db_id,
            the_limit,
            false,
        )
        .await?;

        let mut drop_ids = vec![];
        let mut vacuum_tables = vec![];

        for niv in table_nivs {
            drop_ids.push(DroppedId::from(niv.clone()));
            vacuum_tables.push((tenant_dbname.clone(), niv));
        }

        Ok(ListDroppedTableResp {
            vacuum_tables,
            drop_ids,
        })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_table_lvt(
        &self,
        name_ident: &LeastVisibleTimeIdent,
        value: &LeastVisibleTime,
    ) -> Result<LeastVisibleTime, KVAppError> {
        debug!(req :? =(&name_ident, &value); "TableApi: {}", func_name!());

        let transition = self
            .crud_upsert_with::<Infallible>(name_ident, |t: Option<SeqV<LeastVisibleTime>>| {
                let curr = t.into_value().unwrap_or_default();
                if curr.time >= value.time {
                    Ok(None)
                } else {
                    Ok(Some(value.clone()))
                }
            })
            .await?;

        return Ok(transition.unwrap().result.into_value().unwrap_or_default());
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_lvt(
        &self,
        name_ident: &LeastVisibleTimeIdent,
    ) -> Result<Option<LeastVisibleTime>, KVAppError> {
        debug!(req :? =(&name_ident); "TableApi: {}", func_name!());
        let res = self.get_pb(name_ident).await?;
        Ok(res.map(|v| v.data))
    }
}

#[async_trait::async_trait]
impl<KV> TableApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
