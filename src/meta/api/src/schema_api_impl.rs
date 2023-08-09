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
use std::fmt::Display;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_meta_app::app_error::AppError;
use common_meta_app::app_error::CatalogAlreadyExists;
use common_meta_app::app_error::CreateDatabaseWithDropTime;
use common_meta_app::app_error::CreateIndexWithDropTime;
use common_meta_app::app_error::CreateTableWithDropTime;
use common_meta_app::app_error::DatabaseAlreadyExists;
use common_meta_app::app_error::DropDbWithDropTime;
use common_meta_app::app_error::DropIndexWithDropTime;
use common_meta_app::app_error::DropTableWithDropTime;
use common_meta_app::app_error::DuplicatedUpsertFiles;
use common_meta_app::app_error::GetIndexWithDropTime;
use common_meta_app::app_error::IndexAlreadyExists;
use common_meta_app::app_error::ShareHasNoGrantedPrivilege;
use common_meta_app::app_error::TableAlreadyExists;
use common_meta_app::app_error::TableVersionMismatched;
use common_meta_app::app_error::TxnRetryMaxTimes;
use common_meta_app::app_error::UndropDbHasNoHistory;
use common_meta_app::app_error::UndropDbWithNoDropTime;
use common_meta_app::app_error::UndropTableAlreadyExists;
use common_meta_app::app_error::UndropTableHasNoHistory;
use common_meta_app::app_error::UndropTableWithNoDropTime;
use common_meta_app::app_error::UnknownCatalog;
use common_meta_app::app_error::UnknownDatabaseId;
use common_meta_app::app_error::UnknownIndex;
use common_meta_app::app_error::UnknownTable;
use common_meta_app::app_error::UnknownTableId;
use common_meta_app::app_error::VirtualColumnAlreadyExists;
use common_meta_app::app_error::WrongShare;
use common_meta_app::app_error::WrongShareObject;
use common_meta_app::data_mask::MaskpolicyTableIdList;
use common_meta_app::data_mask::MaskpolicyTableIdListKey;
use common_meta_app::schema::CatalogId;
use common_meta_app::schema::CatalogIdToName;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::CatalogMeta;
use common_meta_app::schema::CatalogNameIdent;
use common_meta_app::schema::CountTablesKey;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateCatalogReply;
use common_meta_app::schema::CreateCatalogReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateIndexReply;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::CreateTableLockRevReply;
use common_meta_app::schema::CreateTableLockRevReq;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::CreateVirtualColumnReply;
use common_meta_app::schema::CreateVirtualColumnReq;
use common_meta_app::schema::DBIdTableName;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseIdToName;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseInfoFilter;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::DbIdList;
use common_meta_app::schema::DbIdListKey;
use common_meta_app::schema::DeleteTableLockRevReq;
use common_meta_app::schema::DropCatalogReply;
use common_meta_app::schema::DropCatalogReq;
use common_meta_app::schema::DropDatabaseReply;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropIndexReply;
use common_meta_app::schema::DropIndexReq;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropVirtualColumnReply;
use common_meta_app::schema::DropVirtualColumnReq;
use common_meta_app::schema::DroppedId;
use common_meta_app::schema::EmptyProto;
use common_meta_app::schema::ExtendTableLockRevReq;
use common_meta_app::schema::GcDroppedTableReq;
use common_meta_app::schema::GcDroppedTableResp;
use common_meta_app::schema::GetCatalogReq;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetIndexReply;
use common_meta_app::schema::GetIndexReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::IndexId;
use common_meta_app::schema::IndexIdToName;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::IndexNameIdent;
use common_meta_app::schema::ListCatalogReq;
use common_meta_app::schema::ListDatabaseReq;
use common_meta_app::schema::ListDroppedTableReq;
use common_meta_app::schema::ListDroppedTableResp;
use common_meta_app::schema::ListIndexesByIdReq;
use common_meta_app::schema::ListIndexesReq;
use common_meta_app::schema::ListTableLockRevReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::ListVirtualColumnsReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::SetTableColumnMaskPolicyAction;
use common_meta_app::schema::SetTableColumnMaskPolicyReply;
use common_meta_app::schema::SetTableColumnMaskPolicyReq;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::TableCopiedFileNameIdent;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdList;
use common_meta_app::schema::TableIdListKey;
use common_meta_app::schema::TableIdToName;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableInfoFilter;
use common_meta_app::schema::TableLockKey;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateIndexReply;
use common_meta_app::schema::UpdateIndexReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpdateVirtualColumnReply;
use common_meta_app::schema::UpdateVirtualColumnReq;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_app::schema::VirtualColumnMeta;
use common_meta_app::schema::VirtualColumnNameIdent;
use common_meta_app::share::ShareGrantObject;
use common_meta_app::share::ShareNameIdent;
use common_meta_app::share::ShareTableInfoMap;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::Key;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::txn_op::Request;
use common_meta_types::txn_op_response::Response;
use common_meta_types::ConditionResult;
use common_meta_types::InvalidReply;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaError;
use common_meta_types::MetaId;
use common_meta_types::MetaNetworkError;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::TxnCondition;
use common_meta_types::TxnGetRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use log::as_debug;
use log::as_display;
use log::debug;
use log::error;
use ConditionResult::Eq;

use crate::assert_table_exist;
use crate::convert_share_meta_to_spec;
use crate::db_has_to_exist;
use crate::deserialize_struct;
use crate::fetch_id;
use crate::get_pb_value;
use crate::get_share_id_to_name_or_err;
use crate::get_share_meta_by_id_or_err;
use crate::get_share_or_err;
use crate::get_share_table_info;
use crate::get_u64_value;
use crate::is_db_need_to_be_remove;
use crate::kv_app_error::KVAppError;
use crate::list_keys;
use crate::list_u64_value;
use crate::remove_db_from_share;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::txn_op_put_with_expire;
use crate::util::deserialize_u64;
use crate::util::get_index_metas_by_ids;
use crate::util::get_table_by_id_or_err;
use crate::util::get_table_names_by_ids;
use crate::util::get_virtual_column_by_id_or_err;
use crate::util::list_tables_from_share_db;
use crate::util::list_tables_from_unshare_db;
use crate::util::mget_pb_values;
use crate::util::remove_table_from_share;
use crate::util::txn_trials;
use crate::IdGenerator;
use crate::SchemaApi;
use crate::DEFAULT_MGET_SIZE;
use crate::TXN_MAX_RETRY_TIMES;

const DEFAULT_DATA_RETENTION_SECONDS: i64 = 24 * 60 * 60;

/// SchemaApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls SchemaApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> SchemaApi for KV {
    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        if req.meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateDatabaseWithDropTime(
                CreateDatabaseWithDropTime::new(&name_key.db_name),
            )));
        }

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            // Get db by name to ensure absence
            let (db_id_seq, db_id) = get_u64_value(self, name_key).await?;
            debug!(db_id_seq = db_id_seq, db_id = db_id, name_key = as_debug!(name_key); "get_database");

            if db_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateDatabaseReply { db_id })
                } else {
                    Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            &name_key.db_name,
                            format!("create db: tenant: {}", name_key.tenant),
                        ),
                    )))
                };
            }

            // get db id list from _fd_db_id_list/db_id
            let dbid_idlist = DbIdListKey {
                tenant: name_key.tenant.clone(),
                db_name: name_key.db_name.clone(),
            };
            let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
                get_pb_value(self, &dbid_idlist).await?;

            let mut db_id_list = if db_id_list_seq == 0 {
                DbIdList::new()
            } else {
                db_id_list_opt.unwrap_or(DbIdList::new())
            };

            // Create db by inserting these record:
            // (tenant, db_name) -> db_id
            // (db_id) -> db_meta
            // append db_id into _fd_db_id_list/<tenant>/<db_name>
            // (db_id) -> (tenant,db_name)

            // if create database from a share then also need to update these record:
            // share_id -> share_meta

            let db_id = fetch_id(self, IdGenerator::database_id()).await?;
            let id_key = DatabaseId { db_id };
            let id_to_name_key = DatabaseIdToName { db_id };

            debug!(db_id = db_id, name_key = as_debug!(name_key); "new database id");

            {
                // append db_id into db_id_list
                db_id_list.append(db_id);

                let condition = vec![
                    txn_cond_seq(name_key, Eq, 0),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                    txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                ];
                let if_then = vec![
                    txn_op_put(name_key, serialize_u64(db_id)?), // (tenant, db_name) -> db_id
                    txn_op_put(&id_key, serialize_struct(&req.meta)?), // (db_id) -> db_meta
                    txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?), /* _fd_db_id_list/<tenant>/<db_name> -> db_id_list */
                    txn_op_put(&id_to_name_key, serialize_struct(name_key)?), /* __fd_database_id_to_name/<db_id> -> (tenant,db_name) */
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_database"
                );

                if succ {
                    return Ok(CreateDatabaseReply { db_id });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("create_database", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.name_ident;
        let mut retry = 0;

        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let res = get_db_or_err(
                self,
                tenant_dbname,
                format!("drop_database: {}", &tenant_dbname),
            )
            .await;

            let (db_id_seq, db_id, db_meta_seq, mut db_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownDatabase(_)) = e {
                        if req.if_exists {
                            return Ok(DropDatabaseReply { spec_vec: None });
                        }
                    }

                    return Err(e);
                }
            };

            let mut condition = vec![];
            let mut if_then = vec![];

            // remove db_name -> db id
            condition.push(txn_cond_seq(tenant_dbname, Eq, db_id_seq));
            if_then.push(txn_op_del(tenant_dbname)); // (tenant, db_name) -> db_id

            // remove db from share
            let mut spec_vec = Vec::with_capacity(db_meta.shared_by.len());
            for share_id in &db_meta.shared_by {
                let res = remove_db_from_share(
                    self,
                    *share_id,
                    db_id,
                    tenant_dbname,
                    &mut condition,
                    &mut if_then,
                )
                .await;

                match res {
                    Ok((share_name, share_meta)) => {
                        spec_vec.push(
                            convert_share_meta_to_spec(self, &share_name, *share_id, share_meta)
                                .await?,
                        );
                    }
                    Err(e) => match e {
                        // ignore UnknownShareId error
                        KVAppError::AppError(AppError::UnknownShareId(_)) => {
                            error!(
                                "UnknownShareId {} when drop_database {} shared by",
                                share_id, tenant_dbname
                            );
                        }
                        _ => return Err(e),
                    },
                }
            }
            if !spec_vec.is_empty() {
                db_meta.shared_by.clear();
            }

            let (removed, _from_share) = is_db_need_to_be_remove(
                self,
                db_id,
                // remove db directly if created from share
                |db_meta| db_meta.from_share.is_some(),
                &mut condition,
                &mut if_then,
            )
            .await?;

            if removed {
                // if db create from share then remove it directly and remove db id from share
                debug!(
                    name = as_debug!(tenant_dbname),
                    id = as_debug!(&DatabaseId { db_id });
                    "drop_database from share"
                );

                // if remove db, MUST also removed db id from db id list
                let dbid_idlist = DbIdListKey {
                    tenant: tenant_dbname.tenant.clone(),
                    db_name: tenant_dbname.db_name.clone(),
                };
                let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
                    get_pb_value(self, &dbid_idlist).await?;

                let mut db_id_list = if db_id_list_seq == 0 {
                    DbIdList::new()
                } else {
                    db_id_list_opt.unwrap_or(DbIdList::new())
                };
                if let Some(last_db_id) = db_id_list.last() {
                    if *last_db_id == db_id {
                        db_id_list.pop();
                        condition.push(txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq));
                        if_then.push(txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?));
                    }
                }
            } else {
                // Delete db by these operations:
                // del (tenant, db_name) -> db_id
                // set db_meta.drop_on = now and update (db_id) -> db_meta

                let db_id_key = DatabaseId { db_id };

                debug!(
                    db_id = db_id,
                    name_key = as_debug!(tenant_dbname);
                    "drop_database"
                );

                {
                    // drop a table with drop time
                    if db_meta.drop_on.is_some() {
                        return Err(KVAppError::AppError(AppError::DropDbWithDropTime(
                            DropDbWithDropTime::new(&tenant_dbname.db_name),
                        )));
                    }
                    // update drop on time
                    db_meta.drop_on = Some(Utc::now());

                    condition.push(txn_cond_seq(&db_id_key, Eq, db_meta_seq));

                    if_then.push(txn_op_put(&db_id_key, serialize_struct(&db_meta)?)); // (db_id) -> db_meta
                }
            }

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name = as_debug!(tenant_dbname),
                id = as_debug!(&DatabaseId { db_id }),
                succ = succ;
                "drop_database"
            );

            if succ {
                return Ok(DropDatabaseReply {
                    spec_vec: if spec_vec.is_empty() {
                        None
                    } else {
                        Some(spec_vec)
                    },
                });
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_database", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> Result<UndropDatabaseReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let res =
                get_db_or_err(self, name_key, format!("undrop_database: {}", &name_key)).await;

            if res.is_ok() {
                return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                    DatabaseAlreadyExists::new(
                        &name_key.db_name,
                        format!("undrop_database: {} has already existed", name_key.db_name),
                    ),
                )));
            }

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist = DbIdListKey {
                tenant: name_key.tenant.clone(),
                db_name: name_key.db_name.clone(),
            };
            let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
                get_pb_value(self, &dbid_idlist).await?;

            let mut db_id_list = if db_id_list_seq == 0 {
                return Err(KVAppError::AppError(AppError::UndropDbHasNoHistory(
                    UndropDbHasNoHistory::new(&name_key.db_name),
                )));
            } else {
                db_id_list_opt.ok_or(KVAppError::AppError(AppError::UndropDbHasNoHistory(
                    UndropDbHasNoHistory::new(&name_key.db_name),
                )))?
            };

            // Return error if there is no db id history.
            let db_id =
                *db_id_list
                    .last()
                    .ok_or(KVAppError::AppError(AppError::UndropDbHasNoHistory(
                        UndropDbHasNoHistory::new(&name_key.db_name),
                    )))?;

            // get db_meta of the last db id
            let dbid = DatabaseId { db_id };
            let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) =
                get_pb_value(self, &dbid).await?;

            debug!(db_id = db_id, name_key = as_debug!(name_key); "undrop_database");

            {
                // reset drop on time
                let mut db_meta = db_meta.unwrap();
                // undrop a table with no drop time
                if db_meta.drop_on.is_none() {
                    return Err(KVAppError::AppError(AppError::UndropDbWithNoDropTime(
                        UndropDbWithNoDropTime::new(&name_key.db_name),
                    )));
                }
                db_meta.drop_on = None;

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0),
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                        txn_cond_seq(&dbid, Eq, db_meta_seq),
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_u64(db_id)?), // (tenant, db_name) -> db_id
                        txn_op_put(&dbid, serialize_struct(&db_meta)?), // (db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name_key = as_debug!(name_key),
                    succ = succ;
                    "undrop_database"
                );

                if succ {
                    return Ok(UndropDatabaseReply {});
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("undrop_database", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.name_ident;
        let tenant_newdbname = DatabaseNameIdent {
            tenant: tenant_dbname.tenant.clone(),
            db_name: req.new_db_name.clone(),
        };

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            // get old db, not exists return err
            let (old_db_id_seq, old_db_id) = get_u64_value(self, tenant_dbname).await?;
            if req.if_exists {
                if old_db_id_seq == 0 {
                    return Ok(RenameDatabaseReply {});
                }
            } else {
                db_has_to_exist(old_db_id_seq, tenant_dbname, "rename_database: src (db)")?;
            }

            debug!(
                old_db_id = old_db_id,
                tenant_dbname = as_debug!(tenant_dbname);
                "rename_database"
            );

            // get new db, exists return err
            let (db_id_seq, _db_id) = get_u64_value(self, &tenant_newdbname).await?;
            db_has_to_not_exist(db_id_seq, &tenant_newdbname, "rename_database")?;

            // get db id -> name
            let db_id_key = DatabaseIdToName { db_id: old_db_id };
            let (db_name_seq, _): (_, Option<DatabaseNameIdent>) =
                get_pb_value(self, &db_id_key).await?;

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist = DbIdListKey {
                tenant: tenant_dbname.tenant.clone(),
                db_name: tenant_dbname.db_name.clone(),
            };
            let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
                get_pb_value(self, &dbid_idlist).await?;
            let mut db_id_list: DbIdList;
            if db_id_list_seq == 0 {
                // may the database is created before add db_id_list, so we just add the id into the list.
                db_id_list = DbIdList::new();
                db_id_list.append(old_db_id);
            } else {
                match db_id_list_opt {
                    Some(list) => db_id_list = list,
                    None => {
                        // may the database is created before add db_id_list, so we just add the id into the list.
                        db_id_list = DbIdList::new();
                        db_id_list.append(old_db_id);
                    }
                }
            };

            if let Some(last_db_id) = db_id_list.last() {
                if *last_db_id != old_db_id {
                    return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            &tenant_dbname.db_name,
                            format!("rename_database: {} with a wrong db id", tenant_dbname),
                        ),
                    )));
                }
            } else {
                return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                    DatabaseAlreadyExists::new(
                        &tenant_dbname.db_name,
                        format!("rename_database: {} with none db id history", tenant_dbname),
                    ),
                )));
            }

            let new_dbid_idlist = DbIdListKey {
                tenant: tenant_dbname.tenant.clone(),
                db_name: req.new_db_name.clone(),
            };
            let (new_db_id_list_seq, new_db_id_list_opt): (_, Option<DbIdList>) =
                get_pb_value(self, &new_dbid_idlist).await?;
            let mut new_db_id_list: DbIdList;
            if new_db_id_list_seq == 0 {
                new_db_id_list = DbIdList::new();
            } else {
                new_db_id_list = new_db_id_list_opt.unwrap_or(DbIdList::new());
            };

            // rename database
            {
                // move db id from old db id list to new db id list
                db_id_list.pop();
                new_db_id_list.append(old_db_id);

                let txn_req = TxnRequest {
                    condition: vec![
                        // Prevent renaming or deleting in other threads.
                        txn_cond_seq(tenant_dbname, Eq, old_db_id_seq),
                        txn_cond_seq(&db_id_key, Eq, db_name_seq),
                        txn_cond_seq(&tenant_newdbname, Eq, 0),
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                        txn_cond_seq(&new_dbid_idlist, Eq, new_db_id_list_seq),
                    ],
                    if_then: vec![
                        txn_op_del(tenant_dbname), // del old_db_name
                        // Renaming db should not affect the seq of db_meta. Just modify db name.
                        txn_op_put(&tenant_newdbname, serialize_u64(old_db_id)?), /* (tenant, new_db_name) -> old_db_id */
                        txn_op_put(&new_dbid_idlist, serialize_struct(&new_db_id_list)?), /* _fd_db_id_list/tenant/new_db_name -> new_db_id_list */
                        txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?), /* _fd_db_id_list/tenant/db_name -> db_id_list */
                        txn_op_put(&db_id_key, serialize_struct(&tenant_newdbname)?), /* __fd_database_id_to_name/<db_id> -> (tenant,db_name) */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(tenant_dbname),
                    to = as_debug!(&tenant_newdbname),
                    database_id = as_debug!(&old_db_id),
                    succ = succ;
                    "rename_database"
                );

                if succ {
                    return Ok(RenameDatabaseReply {});
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("rename_database", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.inner;

        let (_, db_id, db_meta_seq, db_meta) =
            get_db_or_err(self, name_key, "get_database").await?;

        let db = DatabaseInfo {
            ident: DatabaseIdent {
                db_id,
                seq: db_meta_seq,
            },
            name_ident: name_key.clone(),
            meta: db_meta,
        };

        Ok(Arc::new(db))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_database_history(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        // List tables by tenant, db_id, table_name.
        let dbid_tbname_idlist = DbIdListKey {
            tenant: req.tenant,
            // Using a empty db to to list all
            db_name: "".to_string(),
        };
        let db_id_list_keys = list_keys(self, &dbid_tbname_idlist).await?;

        let mut db_info_list = vec![];
        let now = Utc::now();
        let keys: Vec<String> = db_id_list_keys
            .iter()
            .map(|db_id_list_key| db_id_list_key.to_string_key())
            .collect();
        let mut db_id_list_keys_iter = db_id_list_keys.into_iter();
        let include_drop_db = matches!(&req.filter, Some(DatabaseInfoFilter::IncludeDropped));
        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let db_id_list_seq_and_list: Vec<(u64, Option<DbIdList>)> =
                mget_pb_values(self, c).await?;

            for (db_id_list_seq, db_id_list_opt) in db_id_list_seq_and_list {
                let db_id_list_key = db_id_list_keys_iter.next().unwrap();
                let db_id_list = if db_id_list_seq == 0 {
                    continue;
                } else {
                    match db_id_list_opt {
                        Some(list) => list,
                        None => {
                            continue;
                        }
                    }
                };

                let inner_keys: Vec<String> = db_id_list
                    .id_list
                    .iter()
                    .map(|db_id| DatabaseId { db_id: *db_id }.to_string_key())
                    .collect();
                let mut db_id_list_iter = db_id_list.id_list.into_iter();
                for c in inner_keys.chunks(DEFAULT_MGET_SIZE) {
                    let db_meta_seq_meta_vec: Vec<(u64, Option<DatabaseMeta>)> =
                        mget_pb_values(self, c).await?;

                    for (db_meta_seq, db_meta) in db_meta_seq_meta_vec {
                        let db_id = db_id_list_iter.next().unwrap();
                        if db_meta_seq == 0 || db_meta.is_none() {
                            error!("get_database_history cannot find {:?} db_meta", db_id);
                            continue;
                        }
                        let db_meta = db_meta.unwrap();
                        // if include drop db, then no need to fill out of retention time db
                        if !include_drop_db
                            && is_drop_time_out_of_retention_time(&db_meta.drop_on, &now)
                        {
                            continue;
                        }

                        let db = DatabaseInfo {
                            ident: DatabaseIdent {
                                db_id,
                                seq: db_meta_seq,
                            },
                            name_ident: DatabaseNameIdent {
                                tenant: db_id_list_key.tenant.clone(),
                                db_name: db_id_list_key.db_name.clone(),
                            },
                            meta: db_meta,
                        };

                        db_info_list.push(Arc::new(db));
                    }
                }
            }
        }

        return Ok(db_info_list);
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = DatabaseNameIdent {
            tenant: req.tenant,
            // Using a empty db to to list all
            db_name: "".to_string(),
        };

        // Pairs of db-name and db_id with seq
        let (tenant_dbnames, db_ids) = list_u64_value(self, &name_key).await?;

        // Keys for fetching serialized DatabaseMeta from kvapi::KVApi
        let mut kv_keys = Vec::with_capacity(db_ids.len());

        for db_id in db_ids.iter() {
            let k = DatabaseId { db_id: *db_id }.to_string_key();
            kv_keys.push(k);
        }

        // Batch get all db-metas.
        // - A db-meta may be already deleted. It is Ok. Just ignore it.

        let seq_metas = self.mget_kv(&kv_keys).await?;
        let mut db_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let db_meta: DatabaseMeta = deserialize_struct(&seq_meta.data)?;

                let db_info = DatabaseInfo {
                    ident: DatabaseIdent {
                        db_id: db_ids[i],
                        seq: seq_meta.seq,
                    },
                    name_ident: DatabaseNameIdent {
                        tenant: name_key.tenant.clone(),
                        db_name: tenant_dbnames[i].db_name.clone(),
                    },
                    meta: db_meta,
                };
                db_infos.push(Arc::new(db_info));
            } else {
                debug!(
                    k = &kv_keys[i];
                    "db_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(db_infos)
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_index = &req.name_ident;

        if req.meta.dropped_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateIndexWithDropTime(
                CreateIndexWithDropTime::new(&tenant_index.index_name),
            )));
        }

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            // Get index by name to ensure absence
            let (index_id_seq, index_id) = get_u64_value(self, tenant_index).await?;
            debug!(
                index_id_seq = index_id_seq,
                index_id = index_id,
                tenant_index = as_debug!(tenant_index);
                "get_index_seq_id"
            );

            if index_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateIndexReply { index_id })
                } else {
                    Err(KVAppError::AppError(AppError::IndexAlreadyExists(
                        IndexAlreadyExists::new(
                            &tenant_index.index_name,
                            format!("create index with tenant: {}", tenant_index.tenant),
                        ),
                    )))
                };
            }

            // Create index by inserting these record:
            // (tenant, index_name) -> index_id
            // (index_id) -> index_meta
            // (index_id) -> (tenant,index_name)

            let index_id = fetch_id(self, IdGenerator::index_id()).await?;
            let id_key = IndexId { index_id };
            let id_to_name_key = IndexIdToName { index_id };

            debug!(
                index_id = index_id,
                index_key = as_debug!(tenant_index);
                "new index id"
            );

            {
                let condition = vec![
                    txn_cond_seq(tenant_index, Eq, 0),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                ];
                let if_then = vec![
                    txn_op_put(tenant_index, serialize_u64(index_id)?), /* (tenant, index_name) -> index_id */
                    txn_op_put(&id_key, serialize_struct(&req.meta)?),  // (index_id) -> index_meta
                    txn_op_put(&id_to_name_key, serialize_struct(tenant_index)?), /* __fd_index_id_to_name/<index_id> -> (tenant,index_name) */
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    index_name = as_debug!(tenant_index),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_index"
                );

                if succ {
                    return Ok(CreateIndexReply { index_id });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("create_index", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_index = &req.name_ident;
        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);

        loop {
            trials.next().unwrap()?;

            let res = get_index_or_err(self, tenant_index).await?;

            let (index_id_seq, index_id, index_meta_seq, index_meta) = res;

            if index_id_seq == 0 {
                return if req.if_exists {
                    Ok(DropIndexReply {})
                } else {
                    return Err(KVAppError::AppError(AppError::UnknownIndex(
                        UnknownIndex::new(&tenant_index.index_name, "drop_index"),
                    )));
                };
            }

            let index_id_key = IndexId { index_id };
            // Safe unwrap(): index_meta_seq > 0 implies index_meta is not None.
            let mut index_meta = index_meta.unwrap();

            debug!(index_id = index_id, name_key = as_debug!(tenant_index); "drop_index");

            // drop an index with drop time
            if index_meta.dropped_on.is_some() {
                return Err(KVAppError::AppError(AppError::DropIndexWithDropTime(
                    DropIndexWithDropTime::new(&tenant_index.index_name),
                )));
            }
            // update drop on time
            index_meta.dropped_on = Some(Utc::now());

            // Delete index by these operations:
            // del (tenant, index_name) -> index_id
            // set index_meta.drop_on = now and update (index_id) -> index_meta
            let condition = vec![
                txn_cond_seq(tenant_index, Eq, index_id_seq),
                txn_cond_seq(&index_id_key, Eq, index_meta_seq),
            ];

            let if_then = vec![
                txn_op_del(tenant_index), // (tenant, index_name) -> index_id
                txn_op_put(&index_id_key, serialize_struct(&index_meta)?), /* (index_id) -> index_meta */
            ];

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name = as_debug!(tenant_index),
                id = as_debug!(&IndexId { index_id }),
                succ = succ;
                "drop_index"
            );

            if succ {
                break;
            }
        }
        Ok(DropIndexReply {})
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_index = &req.name_ident;

        let res = get_index_or_err(self, tenant_index).await?;

        let (index_id_seq, index_id, _, index_meta) = res;

        if index_id_seq == 0 {
            return Err(KVAppError::AppError(AppError::UnknownIndex(
                UnknownIndex::new(&tenant_index.index_name, "get_index"),
            )));
        }

        // Safe unwrap(): index_meta_seq > 0 implies index_meta is not None.
        let index_meta = index_meta.unwrap();

        debug!(
            index_id = index_id,
            name_key = as_debug!(tenant_index);
            "drop_index"
        );

        // get an index with drop time
        if index_meta.dropped_on.is_some() {
            return Err(KVAppError::AppError(AppError::GetIndexWithDropTIme(
                GetIndexWithDropTime::new(&tenant_index.index_name),
            )));
        }

        Ok(GetIndexReply {
            index_id,
            index_meta,
        })
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let index_id_key = IndexId {
            index_id: req.index_id,
        };

        let reply = self
            .upsert_kv(UpsertKVReq::new(
                &index_id_key.to_string_key(),
                MatchSeq::GE(1),
                Operation::Update(serialize_struct(&req.index_meta)?),
                None,
            ))
            .await?;

        if !reply.is_changed() {
            Err(KVAppError::AppError(AppError::UnknownIndex(
                UnknownIndex::new(&req.index_name, "update_index"),
            )))
        } else {
            Ok(UpdateIndexReply {})
        }
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn list_indexes(
        &self,
        req: ListIndexesReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        // Get index id list by `prefix_list` "<prefix>/<tenant>"
        let prefix_key = kvapi::KeyBuilder::new_prefixed(IndexNameIdent::PREFIX)
            .push_str(&req.tenant)
            .done();

        let id_list = self.prefix_list_kv(&prefix_key).await?;
        let mut id_name_list = Vec::with_capacity(id_list.len());
        for (key, seq) in id_list.iter() {
            let name_ident = IndexNameIdent::from_str_key(key).map_err(|e| {
                KVAppError::MetaError(MetaError::from(InvalidReply::new("list_indexes", &e)))
            })?;
            let index_id = deserialize_u64(&seq.data)?;
            id_name_list.push((index_id.0, name_ident.index_name));
        }

        debug!(ident = prefix_key; "list_indexes");

        if id_name_list.is_empty() {
            return Ok(vec![]);
        }

        // filter the dropped indexes.
        let index_metas = {
            let index_metas = get_index_metas_by_ids(self, id_name_list).await?;
            index_metas
                .into_iter()
                .filter(|(_, _, meta)| {
                    // 1. index is not dropped.
                    // 2. table_id is not specified
                    //    or table_id is specified and equals to the given table_id.
                    meta.dropped_on.is_none()
                        && req.table_id.filter(|id| *id != meta.table_id).is_none()
                })
                .collect::<Vec<_>>()
        };

        Ok(index_metas)
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<u64>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        // Get index id list by `prefix_list` "<prefix>/<tenant>"
        let prefix_key = kvapi::KeyBuilder::new_prefixed(IndexNameIdent::PREFIX)
            .push_str(&req.tenant)
            .done();

        let id_list = self.prefix_list_kv(&prefix_key).await?;
        let mut id_name_list = Vec::with_capacity(id_list.len());
        for (key, seq) in id_list.iter() {
            let name_ident = IndexNameIdent::from_str_key(key).map_err(|e| {
                KVAppError::MetaError(MetaError::from(InvalidReply::new("list_indexes", &e)))
            })?;
            let index_id = deserialize_u64(&seq.data)?;
            id_name_list.push((index_id.0, name_ident.index_name));
        }

        debug!(ident = as_display!(&prefix_key); "list_indexes");

        if id_name_list.is_empty() {
            return Ok(vec![]);
        }

        let index_ids = {
            let index_metas = get_index_metas_by_ids(self, id_name_list).await?;
            index_metas
                .into_iter()
                .filter(|(_, _, meta)| req.table_id == meta.table_id)
                .map(|(id, _, _)| id)
                .collect::<Vec<_>>()
        };

        Ok(index_ids)
    }

    // virtual column

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        loop {
            trials.next().unwrap()?;

            let (_, old_virtual_column_opt): (_, Option<VirtualColumnMeta>) =
                get_pb_value(self, &req.name_ident).await?;

            if old_virtual_column_opt.is_some() {
                return Err(KVAppError::AppError(AppError::VirtualColumnAlreadyExists(
                    VirtualColumnAlreadyExists::new(
                        req.name_ident.table_id,
                        format!(
                            "create virtual column with tenant: {} table_id: {}",
                            req.name_ident.tenant, req.name_ident.table_id
                        ),
                    ),
                )));
            }
            let virtual_column_meta = VirtualColumnMeta {
                table_id: req.name_ident.table_id,
                virtual_columns: req.virtual_columns.clone(),
                created_on: Utc::now(),
                updated_on: None,
            };

            // Create virtual column by inserting this record:
            // (tenant, table_id) -> virtual_column_meta
            {
                let condition = vec![txn_cond_seq(&req.name_ident, Eq, 0)];
                let if_then = vec![txn_op_put(
                    &req.name_ident,
                    serialize_struct(&virtual_column_meta)?,
                )];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    "req.name_ident" = as_debug!(&virtual_column_meta),
                    succ = succ;
                    "create_virtual_column"
                );

                if succ {
                    break;
                }
            }
        }

        Ok(CreateVirtualColumnReply {})
    }

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        loop {
            trials.next().unwrap()?;

            let (seq, old_virtual_column_meta) =
                get_virtual_column_by_id_or_err(self, &req.name_ident, ctx).await?;

            let virtual_column_meta = VirtualColumnMeta {
                table_id: req.name_ident.table_id,
                virtual_columns: req.virtual_columns.clone(),
                created_on: old_virtual_column_meta.created_on,
                updated_on: Some(Utc::now()),
            };

            // Update virtual column by inserting this record:
            // (tenant, table_id) -> virtual_column_meta
            {
                let condition = vec![txn_cond_seq(&req.name_ident, Eq, seq)];
                let if_then = vec![txn_op_put(
                    &req.name_ident,
                    serialize_struct(&virtual_column_meta)?,
                )];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    "req.name_ident" = as_debug!(&virtual_column_meta),
                    succ = succ;
                    "update_virtual_column"
                );

                if succ {
                    break;
                }
            }
        }

        Ok(UpdateVirtualColumnReply {})
    }

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        loop {
            trials.next().unwrap()?;

            let (_, _) = get_virtual_column_by_id_or_err(self, &req.name_ident, ctx).await?;

            // Drop virtual column by deleting this record:
            // (tenant, table_id) -> virtual_column_meta
            {
                let if_then = vec![txn_op_del(&req.name_ident)];
                let txn_req = TxnRequest {
                    condition: vec![],
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    "req.name_ident" = as_debug!(&req.name_ident),
                    succ = succ;
                    "drop_virtual_column"
                );

                if succ {
                    break;
                }
            }
        }

        Ok(DropVirtualColumnReply {})
    }

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        if let Some(table_id) = req.table_id {
            let name_ident = VirtualColumnNameIdent {
                tenant: req.tenant.clone(),
                table_id,
            };
            let (_, virtual_column_opt): (_, Option<VirtualColumnMeta>) =
                get_pb_value(self, &name_ident).await?;

            if let Some(virtual_column) = virtual_column_opt {
                return Ok(vec![virtual_column]);
            } else {
                return Ok(vec![]);
            }
        }

        // Get virtual columns list by `prefix_list` "<prefix>/<tenant>"
        let prefix_key = kvapi::KeyBuilder::new_prefixed(VirtualColumnNameIdent::PREFIX)
            .push_str(&req.tenant)
            .done();

        let list = self.prefix_list_kv(&prefix_key).await?;
        let mut virtual_column_list = Vec::with_capacity(list.len());
        for (_, seq) in list.iter() {
            let virtual_column_meta: VirtualColumnMeta = deserialize_struct(&seq.data)?;
            virtual_column_list.push(virtual_column_meta);
        }

        Ok(virtual_column_list)
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();
        let mut tbcount_found = false;
        let mut tb_count = 0;
        let mut tb_count_seq;

        if req.table_meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateTableWithDropTime(
                CreateTableWithDropTime::new(&tenant_dbname_tbname.table_name),
            )));
        }

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "create_table").await?;

            // cannot operate on shared database
            if let Some(from_share) = db_meta.from_share {
                return Err(KVAppError::AppError(AppError::ShareHasNoGrantedPrivilege(
                    ShareHasNoGrantedPrivilege::new(&from_share.tenant, &from_share.share_name),
                )));
            }

            // Get table by tenant,db_id, table_name to assert absence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            let (tb_id_seq, tb_id) = get_u64_value(self, &dbid_tbname).await?;
            if tb_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateTableReply {
                        table_id: tb_id,
                        new_table: false,
                    })
                } else {
                    Err(KVAppError::AppError(AppError::TableAlreadyExists(
                        TableAlreadyExists::new(
                            &tenant_dbname_tbname.table_name,
                            format!("create_table: {}", tenant_dbname_tbname),
                        ),
                    )))
                };
            }

            // get table id list from _fd_table_id_list/db_id/table_name
            let dbid_tbname_idlist = TableIdListKey {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };
            let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
                get_pb_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list = if tb_id_list_seq == 0 {
                TableIdList::new()
            } else {
                tb_id_list_opt.unwrap_or(TableIdList::new())
            };

            // get current table count from _fd_table_count/tenant
            let tb_count_key = CountTablesKey {
                tenant: tenant_dbname.tenant.clone(),
            };
            (tb_count_seq, tb_count) = {
                let (seq, count) = get_u64_value(self, &tb_count_key).await?;
                if seq > 0 {
                    (seq, count)
                } else if !tbcount_found {
                    // only count_tables for the first time.
                    tbcount_found = true;
                    (0, count_tables(self, &tb_count_key).await?)
                } else {
                    (0, tb_count)
                }
            };
            // Create table by inserting these record:
            // (db_id, table_name) -> table_id
            // (table_id) -> table_meta
            // append table_id into _fd_table_id_list/db_id/table_name
            // (table_id) -> table_name

            let table_id = fetch_id(self, IdGenerator::table_id()).await?;

            let tbid = TableId { table_id };

            // get table id name
            let table_id_to_name_key = TableIdToName { table_id };
            let db_id_table_name = DBIdTableName {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            debug!(
                table_id = table_id,
                name = as_debug!(tenant_dbname_tbname);
                "new table id"
            );

            {
                // append new table_id into list
                tb_id_list.append(table_id);

                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
                        // no other table with the same name is inserted.
                        txn_cond_seq(&dbid_tbname, Eq, 0),
                        // no other table id with the same name is append.
                        txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq),
                        // update table count atomically
                        txn_cond_seq(&tb_count_key, Eq, tb_count_seq),
                        txn_cond_seq(&table_id_to_name_key, Eq, 0),
                    ],
                    if_then: vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), /* (db_id) -> db_meta */
                        txn_op_put(&dbid_tbname, serialize_u64(table_id)?), /* (tenant, db_id, tb_name) -> tb_id */
                        txn_op_put(&tbid, serialize_struct(&req.table_meta)?), /* (tenant, db_id, tb_id) -> tb_meta */
                        txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?), /* _fd_table_id_list/db_id/table_name -> tb_id_list */
                        txn_op_put(&tb_count_key, serialize_u64(tb_count + 1)?), /* _fd_table_count/tenant -> tb_count */
                        txn_op_put(&table_id_to_name_key, serialize_struct(&db_id_table_name)?), /* __fd_table_id_to_name/db_id/table_name -> DBIdTableName */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(tenant_dbname_tbname),
                    id = as_debug!(&tbid),
                    succ = succ;
                    "create_table"
                );

                if succ {
                    return Ok(CreateTableReply {
                        table_id,
                        new_table: true,
                    });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("create_table", TXN_MAX_RETRY_TIMES),
        )))
    }

    /// List all tables belonging to every db and every tenant.
    ///
    /// It returns a list of (table-id, table-meta-seq, table-meta).
    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn list_all_tables(&self) -> Result<Vec<(TableId, u64, TableMeta)>, KVAppError> {
        debug!("SchemaApi: {}", func_name!());

        let reply = self
            .prefix_list_kv(&vec![TableId::PREFIX, ""].join("/"))
            .await?;

        let mut res = vec![];

        for (kk, vv) in reply.into_iter() {
            let table_id = TableId::from_str_key(&kk).map_err(|e| {
                let inv = InvalidReply::new("list_all_tables", &e);
                let meta_net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(meta_net_err)
            })?;

            let table_meta: TableMeta = deserialize_struct(&vv.data)?;

            res.push((table_id, vv.seq, table_meta));
        }
        Ok(res)
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();
        let mut tbcount_found = false;
        let mut tb_count = 0;
        let mut tb_count_seq;

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "undrop_table").await?;

            // cannot operate on shared database
            if let Some(from_share) = db_meta.from_share {
                return Err(KVAppError::AppError(AppError::ShareHasNoGrantedPrivilege(
                    ShareHasNoGrantedPrivilege::new(&from_share.tenant, &from_share.share_name),
                )));
            }

            // Get table by tenant,db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            // If table id already exists, return error.
            let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
            if tb_id_seq > 0 || table_id > 0 {
                return Err(KVAppError::AppError(AppError::UndropTableAlreadyExists(
                    UndropTableAlreadyExists::new(&tenant_dbname_tbname.table_name),
                )));
            }

            // get table id list from _fd_table_id_list/db_id/table_name
            let dbid_tbname_idlist = TableIdListKey {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };
            let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
                get_pb_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list = if tb_id_list_seq == 0 {
                return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                )));
            } else {
                tb_id_list_opt.ok_or(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                )))?
            };

            // Return error if there is no table id history.
            let table_id = match tb_id_list.last() {
                Some(table_id) => *table_id,
                None => {
                    return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                        UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                    )));
                }
            };

            // get tb_meta of the last table id
            let tbid = TableId { table_id };
            let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(self, &tbid).await?;

            // get current table count from _fd_table_count/tenant
            let tb_count_key = CountTablesKey {
                tenant: tenant_dbname.tenant.clone(),
            };
            (tb_count_seq, tb_count) = {
                let (seq, count) = get_u64_value(self, &tb_count_key).await?;
                if seq > 0 {
                    (seq, count)
                } else if !tbcount_found {
                    // only count_tables for the first time.
                    tbcount_found = true;
                    (0, count_tables(self, &tb_count_key).await?)
                } else {
                    (0, tb_count)
                }
            };
            // add drop_on time on table meta
            // (db_id, table_name) -> table_id

            debug!(
                ident = as_display!(&tbid),
                name = as_display!(tenant_dbname_tbname);
                "undrop table"
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

                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
                        // still this table id
                        txn_cond_seq(&dbid_tbname, Eq, tb_id_seq),
                        // table is not changed
                        txn_cond_seq(&tbid, Eq, tb_meta_seq),
                        // update table count atomically
                        txn_cond_seq(&tb_count_key, Eq, tb_count_seq),
                    ],
                    if_then: vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), /* (db_id) -> db_meta */
                        txn_op_put(&dbid_tbname, serialize_u64(table_id)?), /* (tenant, db_id, tb_name) -> tb_id */
                        // txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?)?, // _fd_table_id_list/db_id/table_name -> tb_id_list
                        txn_op_put(&tbid, serialize_struct(&tb_meta)?), /* (tenant, db_id, tb_id) -> tb_meta */
                        txn_op_put(&tb_count_key, serialize_u64(tb_count + 1)?), /* _fd_table_count/tenant -> tb_count */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(tenant_dbname_tbname),
                    id = as_debug!(&tbid),
                    succ = succ;
                    "undrop_table"
                );

                if succ {
                    return Ok(UndropTableReply {});
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("undrop_table", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();
        let tenant_newdbname_newtbname = TableNameIdent {
            tenant: tenant_dbname_tbname.tenant.clone(),
            db_name: req.new_db_name.clone(),
            table_name: req.new_table_name.clone(),
        };

        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "rename_table").await?;

            // cannot operate on shared database
            if let Some(from_share) = db_meta.from_share {
                return Err(KVAppError::AppError(AppError::ShareHasNoGrantedPrivilege(
                    ShareHasNoGrantedPrivilege::new(&from_share.tenant, &from_share.share_name),
                )));
            }

            // Get table by db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
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
            let dbid_tbname_idlist = TableIdListKey {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };
            let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
                get_pb_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list: TableIdList;
            if tb_id_list_seq == 0 {
                // may the table is created before add db_id_list, so we just add the id into the list.
                tb_id_list = TableIdList::new();
                tb_id_list.append(table_id);
            } else {
                match tb_id_list_opt {
                    Some(list) => tb_id_list = list,
                    None => {
                        // may the table is created before add db_id_list, so we just add the id into the list.
                        tb_id_list = TableIdList::new();
                        tb_id_list.append(table_id);
                    }
                }
            };

            if let Some(last_table_id) = tb_id_list.last() {
                if *last_table_id != table_id {
                    return Err(KVAppError::AppError(AppError::UnknownTable(
                        UnknownTable::new(
                            &req.name_ident.table_name,
                            format!("{}: {}", "rename table", tenant_dbname_tbname),
                        ),
                    )));
                }
            } else {
                return Err(KVAppError::AppError(AppError::UnknownTable(
                    UnknownTable::new(
                        &req.name_ident.table_name,
                        format!("{}: {}", "rename table", tenant_dbname_tbname),
                    ),
                )));
            }

            // Get the renaming target db to ensure presence.

            let tenant_newdbname = DatabaseNameIdent {
                tenant: tenant_dbname.tenant.clone(),
                db_name: req.new_db_name.clone(),
            };
            let (_, new_db_id, new_db_meta_seq, new_db_meta) =
                get_db_or_err(self, &tenant_newdbname, "rename_table: new db").await?;

            // Get the renaming target table to ensure absence

            let newdbid_newtbname = DBIdTableName {
                db_id: new_db_id,
                table_name: req.new_table_name.clone(),
            };
            let (new_tb_id_seq, _new_tb_id) = get_u64_value(self, &newdbid_newtbname).await?;
            table_has_to_not_exist(new_tb_id_seq, &tenant_newdbname_newtbname, "rename_table")?;

            let new_dbid_tbname_idlist = TableIdListKey {
                db_id: new_db_id,
                table_name: req.new_table_name.clone(),
            };
            let (new_tb_id_list_seq, new_tb_id_list_opt): (_, Option<TableIdList>) =
                get_pb_value(self, &new_dbid_tbname_idlist).await?;

            let mut new_tb_id_list: TableIdList;
            if new_tb_id_list_seq == 0 {
                new_tb_id_list = TableIdList::new();
            } else {
                new_tb_id_list = new_tb_id_list_opt.unwrap_or(TableIdList::new());
            };

            // get table id name
            let table_id_to_name_key = TableIdToName { table_id };
            let (table_id_to_name_seq, _): (_, Option<DBIdTableName>) =
                get_pb_value(self, &table_id_to_name_key).await?;
            let db_id_table_name = DBIdTableName {
                db_id: new_db_id,
                table_name: req.new_table_name.clone(),
            };

            {
                // move table id from old table id list to new table id list
                tb_id_list.pop();
                new_tb_id_list.append(table_id);

                let condition = vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
                    txn_cond_seq(&DatabaseId { db_id: new_db_id }, Eq, new_db_meta_seq),
                    // table_name->table_id does not change.
                    // Updating the table meta is ok.
                    txn_cond_seq(&dbid_tbname, Eq, tb_id_seq),
                    txn_cond_seq(&newdbid_newtbname, Eq, 0),
                    // no other table id with the same name is append.
                    txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq),
                    txn_cond_seq(&new_dbid_tbname_idlist, Eq, new_tb_id_list_seq),
                    txn_cond_seq(&table_id_to_name_key, Eq, table_id_to_name_seq),
                ];

                let mut then_ops = vec![
                    txn_op_del(&dbid_tbname), // (db_id, tb_name) -> tb_id
                    txn_op_put(&newdbid_newtbname, serialize_u64(table_id)?), /* (db_id, new_tb_name) -> tb_id */
                    // Changing a table in a db has to update the seq of db_meta,
                    // to block the batch-delete-tables when deleting a db.
                    txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), /* (db_id) -> db_meta */
                    txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?), /* _fd_table_id_list/db_id/old_table_name -> tb_id_list */
                    txn_op_put(&new_dbid_tbname_idlist, serialize_struct(&new_tb_id_list)?), /* _fd_table_id_list/db_id/new_table_name -> tb_id_list */
                    txn_op_put(&table_id_to_name_key, serialize_struct(&db_id_table_name)?), /* __fd_table_id_to_name/db_id/table_name -> DBIdTableName */
                ];

                if db_id != new_db_id {
                    then_ops.push(
                        txn_op_put(
                            &DatabaseId { db_id: new_db_id },
                            serialize_struct(&new_db_meta)?,
                        ), // (db_id) -> db_meta
                    );
                }

                let txn_req = TxnRequest {
                    condition,
                    if_then: then_ops,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(tenant_dbname_tbname),
                    to = as_debug!(&newdbid_newtbname),
                    table_id = as_debug!(&table_id),
                    succ = succ;
                    "rename_table"
                );

                if succ {
                    return Ok(RenameTableReply { table_id });
                }
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("rename_table", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.inner;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();

        // Get db by name to ensure presence

        let res = get_db_or_err(
            self,
            &tenant_dbname,
            format!("get_table: {}", tenant_dbname),
        )
        .await;

        let (_db_id_seq, db_id, _db_meta_seq, db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        let table_id = match db_meta.from_share {
            Some(ref share) => {
                get_table_id_from_share_by_name(self, share, &tenant_dbname_tbname.table_name)
                    .await?
            }
            None => {
                // Get table by tenant,db_id, table_name to assert presence.

                let dbid_tbname = DBIdTableName {
                    db_id,
                    table_name: tenant_dbname_tbname.table_name.clone(),
                };

                let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
                assert_table_exist(tb_id_seq, tenant_dbname_tbname, "get_table")?;

                table_id
            }
        };

        let tbid = TableId { table_id };

        let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(self, &tbid).await?;

        assert_table_exist(
            tb_meta_seq,
            tenant_dbname_tbname,
            format!("get_table meta by: {}", tenant_dbname_tbname),
        )?;

        debug!(
            ident = as_display!(&tbid),
            name = as_display!(tenant_dbname_tbname),
            table_meta = as_debug!(&tb_meta);
            "get_table"
        );

        let db_type = db_meta
            .from_share
            .map_or(DatabaseType::NormalDB, |share_ident| {
                DatabaseType::ShareDB(share_ident)
            });

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id: tbid.table_id,
                seq: tb_meta_seq,
            },
            desc: tenant_dbname_tbname.to_string(),
            name: tenant_dbname_tbname.table_name.clone(),
            // Safe unwrap() because: tb_meta_seq > 0
            meta: tb_meta.unwrap(),
            tenant: req.tenant.clone(),
            db_type,
        };

        return Ok(Arc::new(tb_info));
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_table_history(
        &self,
        req: ListTableReq,
    ) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            tenant_dbname,
            format!("get_table_history: {}", tenant_dbname),
        )
        .await;

        let (_db_id_seq, db_id, _db_meta_seq, db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        // List tables by tenant, db_id, table_name.
        let dbid_tbname_idlist = TableIdListKey {
            db_id,
            table_name: "".to_string(),
        };

        let table_id_list_keys = list_keys(self, &dbid_tbname_idlist).await?;

        let mut tb_info_list = vec![];
        let now = Utc::now();
        let keys: Vec<String> = table_id_list_keys
            .iter()
            .map(|table_id_list_key| {
                TableIdListKey {
                    db_id,
                    table_name: table_id_list_key.table_name.clone(),
                }
                .to_string_key()
            })
            .collect();
        let mut table_id_list_keys_iter = table_id_list_keys.into_iter();
        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let tb_id_list_seq_vec: Vec<(u64, Option<TableIdList>)> =
                mget_pb_values(self, c).await?;
            for (tb_id_list_seq, tb_id_list_opt) in tb_id_list_seq_vec {
                let table_id_list_key = table_id_list_keys_iter.next().unwrap();
                let tb_id_list = if tb_id_list_seq == 0 {
                    continue;
                } else {
                    match tb_id_list_opt {
                        Some(list) => list,
                        None => {
                            continue;
                        }
                    }
                };

                debug!(
                    name = as_display!(&table_id_list_key);
                    "get_table_history"
                );

                let inner_keys: Vec<String> = tb_id_list
                    .id_list
                    .iter()
                    .map(|table_id| {
                        TableId {
                            table_id: *table_id,
                        }
                        .to_string_key()
                    })
                    .collect();
                let mut table_id_iter = tb_id_list.id_list.into_iter();
                for c in inner_keys.chunks(DEFAULT_MGET_SIZE) {
                    let tb_meta_vec: Vec<(u64, Option<TableMeta>)> =
                        mget_pb_values(self, c).await?;
                    for (tb_meta_seq, tb_meta) in tb_meta_vec {
                        let table_id = table_id_iter.next().unwrap();
                        if tb_meta_seq == 0 || tb_meta.is_none() {
                            error!("get_table_history cannot find {:?} table_meta", table_id);
                            continue;
                        }

                        // Safe unwrap() because: tb_meta_seq > 0
                        let tb_meta = tb_meta.unwrap();
                        if is_drop_time_out_of_retention_time(&tb_meta.drop_on, &now) {
                            continue;
                        }

                        let tenant_dbname_tbname: TableNameIdent = TableNameIdent {
                            tenant: tenant_dbname.tenant.clone(),
                            db_name: tenant_dbname.db_name.clone(),
                            table_name: table_id_list_key.table_name.clone(),
                        };

                        let db_type = db_meta
                            .from_share
                            .clone()
                            .map_or(DatabaseType::NormalDB, |share_ident| {
                                DatabaseType::ShareDB(share_ident)
                            });

                        let tb_info = TableInfo {
                            ident: TableIdent {
                                table_id,
                                seq: tb_meta_seq,
                            },
                            desc: tenant_dbname_tbname.to_string(),
                            name: table_id_list_key.table_name.clone(),
                            meta: tb_meta,
                            tenant: tenant_dbname.tenant.clone(),
                            db_type,
                        };

                        tb_info_list.push(Arc::new(tb_info));
                    }
                }
            }
        }

        return Ok(tb_info_list);
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            tenant_dbname,
            format!("list_tables: {}", &tenant_dbname),
        )
        .await;

        let (_db_id_seq, db_id, _db_meta_seq, db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        let tb_infos = match db_meta.from_share {
            None => list_tables_from_unshare_db(self, db_id, tenant_dbname).await?,
            Some(share) => list_tables_from_share_db(self, share, tenant_dbname).await?,
        };

        Ok(tb_infos)
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), KVAppError> {
        debug!(req = as_debug!(&table_id); "SchemaApi: {}", func_name!());

        let tbid = TableId { table_id };

        let (tb_meta_seq, table_meta): (_, Option<TableMeta>) = get_pb_value(self, &tbid).await?;

        debug!(ident = as_display!(&tbid); "get_table_by_id");

        if tb_meta_seq == 0 || table_meta.is_none() {
            return Err(KVAppError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(table_id, "get_table_by_id"),
            )));
        }

        Ok((
            TableIdent::new(table_id, tb_meta_seq),
            Arc::new(table_meta.unwrap()),
        ))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply, KVAppError> {
        let table_id = req.tb_id;
        debug!(req = as_debug!(&table_id); "SchemaApi: {}", func_name!());

        let mut tbcount_found = false;
        let mut tb_count = 0;
        let mut tb_count_seq;
        let tbid = TableId { table_id };
        let mut retry = 0;

        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;

            // Check if table exists.
            let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(self, &tbid).await?;
            if tb_meta_seq == 0 || tb_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "drop_table_by_id failed to find valid tb_meta"),
                )));
            }

            // Get db name, tenant name and related info for tx.
            let table_id_to_name = TableIdToName { table_id };
            let (_, table_name_opt): (_, Option<DBIdTableName>) =
                get_pb_value(self, &table_id_to_name).await?;

            let dbid_tbname =
                table_name_opt.ok_or(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "drop_table_by_id failed to find db_id"),
                )))?;

            let tbname = dbid_tbname.table_name.clone();
            let (tb_id_seq, _) = get_u64_value(self, &dbid_tbname).await?;
            if tb_id_seq == 0 {
                return if req.if_exists {
                    Ok(DropTableReply { spec_vec: None })
                } else {
                    return Err(KVAppError::AppError(AppError::UnknownTable(
                        UnknownTable::new(tbname, "drop_table_by_id"),
                    )));
                };
            }

            let db_id_to_name = DatabaseIdToName {
                db_id: dbid_tbname.db_id,
            };
            let (_, database_name_opt): (_, Option<DatabaseNameIdent>) =
                get_pb_value(self, &db_id_to_name).await?;
            let tenant_dbname =
                database_name_opt.ok_or(KVAppError::AppError(AppError::UnknownDatabaseId(
                    UnknownDatabaseId::new(dbid_tbname.db_id, "drop_table_by_id"),
                )))?;
            let tenant_dbname_tbname = TableNameIdent {
                tenant: tenant_dbname.tenant.clone(),
                db_name: tenant_dbname.db_name.clone(),
                table_name: tbname,
            };

            // get current table count from _fd_table_count/tenant
            let tb_count_key = CountTablesKey {
                tenant: tenant_dbname.tenant.clone(),
            };
            (tb_count_seq, tb_count) = {
                let (seq, count) = get_u64_value(self, &tb_count_key).await?;
                if seq > 0 {
                    (seq, count)
                } else if !tbcount_found {
                    // only count_tables for the first time.
                    tbcount_found = true;
                    (0, count_tables(self, &tb_count_key).await?)
                } else {
                    (0, tb_count)
                }
            };

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "drop_table_by_id").await?;

            // cannot operate on shared database
            if let Some(from_share) = db_meta.from_share {
                return Err(KVAppError::AppError(AppError::ShareHasNoGrantedPrivilege(
                    ShareHasNoGrantedPrivilege::new(&from_share.tenant, &from_share.share_name),
                )));
            }

            debug!(
                ident = as_display!(&tbid),
                name = as_display!(&tenant_dbname_tbname);
                "drop table by id"
            );

            {
                let mut tb_meta = tb_meta.unwrap();
                // drop a table with drop_on time
                if tb_meta.drop_on.is_some() {
                    return Err(KVAppError::AppError(AppError::DropTableWithDropTime(
                        DropTableWithDropTime::new(&dbid_tbname.table_name),
                    )));
                }

                tb_meta.drop_on = Some(Utc::now());
                let mut condition = vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
                    // still this table id
                    txn_cond_seq(&dbid_tbname, Eq, tb_id_seq),
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                    // update table count atomically
                    txn_cond_seq(&tb_count_key, Eq, tb_count_seq),
                ];
                let mut if_then = vec![
                    // Changing a table in a db has to update the seq of db_meta,
                    // to block the batch-delete-tables when deleting a db.
                    txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), /* (db_id) -> db_meta */
                    txn_op_del(&dbid_tbname), // (db_id, tb_name) -> tb_id
                    txn_op_put(&tbid, serialize_struct(&tb_meta)?), /* (tenant, db_id, tb_id) -> tb_meta */
                    txn_op_put(&tb_count_key, serialize_u64(tb_count - 1)?), /* _fd_table_count/tenant -> tb_count */
                ];

                // remove table from share
                let mut spec_vec = Vec::with_capacity(db_meta.shared_by.len());
                let mut mut_share_table_info = Vec::with_capacity(db_meta.shared_by.len());
                for share_id in &db_meta.shared_by {
                    let res = remove_table_from_share(
                        self,
                        *share_id,
                        table_id,
                        &tenant_dbname_tbname,
                        &mut condition,
                        &mut if_then,
                    )
                    .await;

                    match res {
                        Ok((share_name, share_meta, share_table_info)) => {
                            spec_vec.push(
                                convert_share_meta_to_spec(
                                    self,
                                    &share_name,
                                    *share_id,
                                    share_meta,
                                )
                                .await?,
                            );
                            mut_share_table_info.push((share_name.to_string(), share_table_info));
                        }
                        Err(e) => match e {
                            // ignore UnknownShareId error
                            KVAppError::AppError(AppError::UnknownShareId(_)) => {
                                error!(
                                    "UnknownShareId {} when drop_table_by_id {} shared by",
                                    share_id, tenant_dbname_tbname
                                );
                            }
                            _ => return Err(e),
                        },
                    }
                }

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_display!(&tenant_dbname_tbname),
                    id = as_debug!(&tbid),
                    succ = succ;
                    "drop_table_by_id"
                );
                if succ {
                    return Ok(DropTableReply {
                        spec_vec: if spec_vec.is_empty() {
                            None
                        } else {
                            Some((spec_vec, mut_share_table_info))
                        },
                    });
                }
            }
        }
        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("drop_table_by_id", TXN_MAX_RETRY_TIMES),
        )))
    }

    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;

        let tbid = TableId { table_id };

        let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(self, &tbid).await?;

        if tb_meta_seq == 0 {
            return Err(KVAppError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(table_id, ""),
            )));
        }

        debug!(
            ident = as_display!(&tbid),
            table_meta = as_debug!(&tb_meta);
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

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn truncate_table(
        &self,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply, KVAppError> {
        // NOTE: this method read and remove in multiple transactions.
        // It is not atomic, but it is safe because it deletes only the files that matches the seq.

        let ctx = &func_name!();
        debug!(req = as_debug!(&req); "SchemaApi: {}", ctx);

        let table_id = TableId {
            table_id: req.table_id,
        };

        let chunk_size = req.batch_size.unwrap_or(DEFAULT_MGET_SIZE as u64);

        // 1. Grab a snapshot view of the copied files of a table.
        //
        // If table seq is not changed before and after listing, we can be sure the list of copied
        // files is consistent to this version of the table.

        let (mut seq_1, _tb_meta) = get_table_by_id_or_err(self, &table_id, ctx).await?;

        let mut trials = txn_trials(None, ctx);
        let copied_files = loop {
            trials.next().unwrap()?;

            let copied_files = list_table_copied_files(self, table_id.table_id).await?;

            let (seq_2, _tb_meta) = get_table_by_id_or_err(self, &table_id, ctx).await?;

            if seq_1 == seq_2 {
                debug!(
                    "list all copied file of table {}: {:?}",
                    table_id.table_id, copied_files
                );
                break copied_files;
            } else {
                seq_1 = seq_2;
            }
        };

        // 2. Remove the copied files only when the seq of a copied file has not changed.
        //
        // During running this step with several small transaction, other transactions may be
        // modifying the table.
        //
        // - We assert the table seq is not changed in each transaction.
        // - We do not assert the seq of each copied file in each transaction, since we only delete
        //   non-changed ones.

        for chunk in copied_files.chunks(chunk_size as usize) {
            let str_keys: Vec<_> = chunk.iter().map(|f| f.to_string_key()).collect();

            // Load the `seq` of every copied file
            let seqs = {
                let seq_infos: Vec<(u64, Option<TableCopiedFileInfo>)> =
                    mget_pb_values(self, &str_keys).await?;

                seq_infos.into_iter().map(|(seq, _)| seq)
            };

            let mut if_then = vec![];
            for (copied_seq, copied_str_key) in seqs.zip(str_keys) {
                if copied_seq == 0 {
                    continue;
                }

                if_then.push(TxnOp::delete_exact(copied_str_key, Some(copied_seq)));
            }

            let mut trials = txn_trials(None, ctx);
            loop {
                trials.next().unwrap()?;

                let (tb_meta_seq, tb_meta) = get_table_by_id_or_err(self, &table_id, ctx).await?;

                let mut if_then = if_then.clone();

                // Update to increase table meta seq, so that to assert no other process modify the table
                if_then.push(txn_op_put(&table_id, serialize_struct(&tb_meta)?));

                let txn_req = TxnRequest {
                    condition: vec![txn_cond_seq(&table_id, Eq, tb_meta_seq)],
                    if_then,
                    else_then: vec![],
                };

                debug!("submit chunk delete copied files: {:?}", txn_req);

                let (succ, _responses) = send_txn(self, txn_req).await?;
                debug!(
                    id = as_debug!(&table_id),
                    succ = succ,
                    ctx = ctx;
                    ""
                );

                if succ {
                    break;
                }
            }
        }

        Ok(TruncateTableReply {})
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        loop {
            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(self, &tbid).await?;

            debug!(ident = as_display!(&tbid); "upsert_table_option");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "upsert_table_option"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        tb_meta_seq,
                        "upsert_table_option",
                    ),
                )));
            }
            let mut table_meta = table_meta.unwrap();
            // update table options
            let opts = &mut table_meta.options;

            for (k, opt_v) in &req.options {
                match opt_v {
                    None => {
                        opts.remove(k);
                    }
                    Some(v) => {
                        opts.insert(k.to_string_key(), v.to_string_key());
                    }
                }
            }
            let txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id = as_debug!(&tbid),
                succ = succ;
                "upsert_table_option"
            );

            if succ {
                return Ok(UpsertTableOptionReply {
                    share_table_info: get_share_table_info_map(self, &table_meta).await?,
                });
            }
        }
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn update_table_meta(
        &self,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());
        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        let fail_if_duplicated = req
            .copied_files
            .as_ref()
            .map(|v| v.fail_if_duplicated)
            .unwrap_or(false);

        loop {
            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(self, &tbid).await?;

            debug!(ident = as_display!(&tbid); "update_table_meta");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "update_table_meta"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        tb_meta_seq,
                        "update_table_meta",
                    ),
                )));
            }

            let get_table_meta = TxnOp {
                request: Some(Request::Get(TxnGetRequest {
                    key: tbid.to_string_key(),
                })),
            };

            let mut txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&req.new_table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![get_table_meta],
            };

            if let Some(req) = &req.copied_files {
                let (conditions, match_operations) =
                    build_upsert_table_copied_file_info_conditions(
                        &tbid,
                        req,
                        tb_meta_seq,
                        req.fail_if_duplicated,
                    )?;
                txn_req.condition.extend(conditions);
                txn_req.if_then.extend(match_operations)
            }

            if let Some(deduplicated_label) = req.deduplicated_label.clone() {
                txn_req
                    .if_then
                    .push(build_upsert_table_deduplicated_label(deduplicated_label))
            }

            let (succ, responses) = send_txn(self, txn_req).await?;

            debug!(
                id = as_debug!(&tbid),
                succ = succ;
                "update_table_meta"
            );

            if succ {
                return Ok(UpdateTableMetaReply {
                    share_table_info: get_share_table_info_map(self, &table_meta.unwrap()).await?,
                });
            } else {
                let resp = responses
                    .get(0)
                    // fail fast if response is None (which should not happen)
                    .expect("internal error: expect one response if update_table_meta txn failed.");

                if let Some(Response::Get(get_resp)) = &resp.response {
                    // deserialize table version info
                    let (tb_meta_seq, _): (_, Option<TableMeta>) =
                        if let Some(seq_v) = &get_resp.value {
                            (seq_v.seq, Some(deserialize_struct(&seq_v.data)?))
                        } else {
                            (0, None)
                        };

                    // check table version
                    if req_seq.match_seq(tb_meta_seq).is_ok() {
                        // if table version does match, but tx failed,
                        if fail_if_duplicated {
                            // report file duplication error
                            return Err(KVAppError::AppError(AppError::from(
                                DuplicatedUpsertFiles::new(req.table_id, "update_table_meta"),
                            )));
                        } else {
                            // continue and try update the "table copied files"
                            continue;
                        };
                    } else {
                        return Err(KVAppError::AppError(AppError::from(
                            TableVersionMismatched::new(
                                req.table_id,
                                req.seq,
                                tb_meta_seq,
                                "update_table_meta",
                            ),
                        )));
                    }
                } else {
                    unreachable!(
                        "internal error: expect some TxnGetResponseGet, but got {:?}",
                        resp.response
                    );
                }
            }
        }
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());
        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;
        let mut retry = 0;
        while retry < TXN_MAX_RETRY_TIMES {
            retry += 1;
            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(self, &tbid).await?;

            debug!(ident = as_display!(&tbid); "set_table_column_mask_policy");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "set_table_column_mask_policy"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        tb_meta_seq,
                        "set_table_column_mask_policy",
                    ),
                )));
            }

            // upsert column mask policy
            let table_meta = table_meta.unwrap();

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

            let mut txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&new_table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let _ = update_mask_policy(
                self,
                &req.action,
                &mut txn_req.condition,
                &mut txn_req.if_then,
                req.tenant.clone(),
                req.table_id,
            )
            .await;

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id = as_debug!(&tbid),
                succ = succ;
                "set_table_column_mask_policy"
            );

            if succ {
                return Ok(SetTableColumnMaskPolicyReply {
                    share_table_info: get_share_table_info_map(self, &new_table_meta).await?,
                });
            }
        }

        Err(KVAppError::AppError(AppError::TxnRetryMaxTimes(
            TxnRetryMaxTimes::new("set_table_column_mask_policy", TXN_MAX_RETRY_TIMES),
        )))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<ListDroppedTableResp, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let mut drop_table_infos = vec![];
        let mut drop_ids = vec![];

        if let TableInfoFilter::AllDroppedTables(filter_drop_on) = &req.filter {
            let db_infos = self
                .get_database_history(ListDatabaseReq {
                    tenant: req.inner.tenant.clone(),
                    // need to get all db(include drop db)
                    filter: Some(DatabaseInfoFilter::IncludeDropped),
                })
                .await?;

            for db_info in db_infos {
                // ignore db create from share
                if db_info.meta.from_share.is_some() {
                    continue;
                }
                let mut drop_db = false;
                let table_infos = match db_info.meta.drop_on {
                    Some(db_drop_on) => {
                        if let Some(filter_drop_on) = filter_drop_on {
                            let filter = if db_drop_on.timestamp() <= filter_drop_on.timestamp() {
                                // if db drop on before filter time, then get all the db tables.
                                drop_db = true;
                                TableInfoFilter::All
                            } else {
                                // else get all the db tables drop on before filter time.
                                TableInfoFilter::Dropped(Some(*filter_drop_on))
                            };

                            let req = ListDroppedTableReq {
                                inner: db_info.name_ident.clone(),
                                filter,
                            };
                            do_get_table_history(&self, req, db_info.ident.db_id, &db_info.meta)
                                .await?
                        } else {
                            // while filter_drop_on is None, then get all the drop db tables
                            drop_db = true;
                            let req = ListDroppedTableReq {
                                inner: db_info.name_ident.clone(),
                                filter: TableInfoFilter::All,
                            };
                            do_get_table_history(&self, req, db_info.ident.db_id, &db_info.meta)
                                .await?
                        }
                    }
                    None => {
                        let req = ListDroppedTableReq {
                            inner: db_info.name_ident.clone(),
                            filter: TableInfoFilter::Dropped(*filter_drop_on),
                        };
                        // not drop db, only filter drop tables with filter drop on
                        do_get_table_history(&self, req, db_info.ident.db_id, &db_info.meta).await?
                    }
                };
                if drop_db {
                    drop_ids.push(DroppedId::Db(
                        db_info.ident.db_id,
                        db_info.name_ident.db_name.clone(),
                    ));
                } else {
                    table_infos.iter().for_each(|table_info| {
                        drop_ids.push(DroppedId::Table(
                            db_info.ident.db_id,
                            table_info.ident.table_id,
                            table_info.name.clone(),
                        ))
                    });
                }
                drop_table_infos.extend(table_infos);
            }
            return Ok(ListDroppedTableResp {
                drop_table_infos,
                drop_ids,
            });
        }
        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            tenant_dbname,
            format!("get_table_history: {}", tenant_dbname),
        )
        .await;

        let (_db_id_seq, db_id, _db_meta_seq, db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        // ignore db create from share
        if db_meta.from_share.is_some() {
            return Ok(ListDroppedTableResp {
                drop_table_infos: vec![],
                drop_ids: vec![],
            });
        }

        let table_infos = do_get_table_history(&self, req, db_id, &db_meta).await?;
        table_infos.iter().for_each(|table_info| {
            drop_ids.push(DroppedId::Table(
                db_id,
                table_info.ident.table_id,
                table_info.name.clone(),
            ))
        });
        drop_table_infos = table_infos;
        Ok(ListDroppedTableResp {
            drop_table_infos,
            drop_ids,
        })
    }

    async fn gc_drop_tables(
        &self,
        req: GcDroppedTableReq,
    ) -> Result<GcDroppedTableResp, KVAppError> {
        for drop_id in req.drop_ids {
            match drop_id {
                DroppedId::Db(db_id, db_name) => {
                    gc_dropped_db_by_id(self, db_id, req.tenant.clone(), db_name).await?
                }
                DroppedId::Table(db_id, table_id, table_name) => {
                    gc_dropped_table_by_id(self, req.tenant.clone(), db_id, table_id, table_name)
                        .await?
                }
            }
        }
        Ok(GcDroppedTableResp {})
    }

    /// Get the count of tables for one tenant.
    ///
    /// Accept tenant name and returns the count of tables for the tenant.
    ///
    /// It get the count from kv space first,
    /// if not found, it will compute the count by listing all databases and table ids.
    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let key = CountTablesKey {
            tenant: req.tenant.to_string_key(),
        };

        let count = loop {
            let (seq, cnt) = {
                // get the count from kv space first
                let (seq, c) = get_u64_value(self, &key).await?;
                if seq > 0 {
                    // if seq > 0, we can get the count directly
                    break c;
                }

                // if not, we should compute the count from by listing all databases and table ids

                // this line of codes will only be executed once,
                // because if `send_txn` failed, it means another txn will put the count value into the kv space，
                // and then the next loop will get the count value through `get_u64_value`.
                (0, count_tables(self, &key).await?)
            };

            let key = CountTablesKey {
                tenant: req.tenant.clone(),
            };

            let txn_req = TxnRequest {
                // table count should not be changed.
                condition: vec![txn_cond_seq(&key, Eq, seq)],
                if_then: vec![txn_op_put(&key, serialize_u64(cnt)?)],
                else_then: vec![],
            };

            let (succ, _) = send_txn(self, txn_req).await?;
            // if txn succeeds, count can be returned safely
            if succ {
                break cnt;
            }
        };

        debug!(
            tenant = &req.tenant,
            count = count;
            "count tables for a tenant"
        );

        Ok(CountTablesReply { count })
    }

    async fn list_table_lock_revs(&self, req: ListTableLockRevReq) -> Result<Vec<u64>, KVAppError> {
        let prefix = format!("{}/{}", TableLockKey::PREFIX, req.table_id);
        let reply = self.prefix_list_kv(&prefix).await?;

        let mut revisions = vec![];
        for (k, _) in reply.into_iter() {
            let lock_key = TableLockKey::from_str_key(&k).map_err(|e| {
                let inv = InvalidReply::new("list_table_lock_revs", &e);
                let meta_net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(meta_net_err)
            })?;

            revisions.push(lock_key.revision);
        }
        Ok(revisions)
    }

    async fn create_table_lock_rev(
        &self,
        req: CreateTableLockRevReq,
    ) -> Result<CreateTableLockRevReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;
        let tbid = TableId { table_id };

        let revision = fetch_id(self, IdGenerator::table_lock_id()).await?;

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        loop {
            trials.next().unwrap()?;

            let (tb_meta_seq, _) = get_table_by_id_or_err(self, &tbid, ctx).await?;
            let lock_key = TableLockKey { table_id, revision };

            let lock = EmptyProto {};

            let condition = vec![
                // table is not changed
                txn_cond_seq(&tbid, Eq, tb_meta_seq),
                // assumes lock are absent.
                txn_cond_seq(&lock_key, Eq, 0),
            ];

            let if_then = vec![txn_op_put_with_expire(
                &lock_key,
                serialize_struct(&lock)?,
                req.expire_at,
            )];

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                ident = as_display!(&tbid),
                succ = succ;
                "create_table_lock_rev"
            );

            if succ {
                break;
            }
        }

        Ok(CreateTableLockRevReply { revision })
    }

    async fn extend_table_lock_rev(&self, req: ExtendTableLockRevReq) -> Result<(), KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;
        let revision = req.revision;

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);

        loop {
            trials.next().unwrap()?;

            let tbid = TableId { table_id };
            let (tb_meta_seq, _) = get_table_by_id_or_err(self, &tbid, ctx).await?;

            let lock_key = TableLockKey { table_id, revision };
            let (lock_key_seq, _): (_, Option<EmptyProto>) = get_pb_value(self, &lock_key).await?;

            let lock = EmptyProto {};

            let condition = vec![
                // table is not changed
                txn_cond_seq(&tbid, Eq, tb_meta_seq),
                txn_cond_seq(&lock_key, Eq, lock_key_seq),
            ];

            let if_then = vec![txn_op_put_with_expire(
                &lock_key,
                serialize_struct(&lock)?,
                req.expire_at,
            )];

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                ident = as_display!(&tbid),
                succ = succ;
                "extend_table_lock_rev"
            );

            if succ {
                break;
            }
        }
        Ok(())
    }

    async fn delete_table_lock_rev(&self, req: DeleteTableLockRevReq) -> Result<(), KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;
        let revision = req.revision;

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);

        loop {
            trials.next().unwrap()?;

            let lock_key = TableLockKey { table_id, revision };
            let (lock_key_seq, _): (_, Option<EmptyProto>) = get_pb_value(self, &lock_key).await?;
            if lock_key_seq == 0 {
                return Ok(());
            }

            let condition = vec![txn_cond_seq(&lock_key, Eq, lock_key_seq)];
            let if_then = vec![txn_op_del(&lock_key)];

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            let tbid = TableId { table_id };
            debug!(
                ident = as_display!(&tbid),
                succ = succ;
                "delete_table_lock_rev"
            );

            if succ {
                break;
            }
        }

        Ok(())
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn create_catalog(
        &self,
        req: CreateCatalogReq,
    ) -> Result<CreateCatalogReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        let ctx = &func_name!();

        let mut trials = txn_trials(None, ctx);

        let catalog_id = loop {
            trials.next().unwrap()?;

            // Get catalog by name to ensure absence
            let (catalog_id_seq, catalog_id) = get_u64_value(self, name_key).await?;
            debug!(
                catalog_id_seq = catalog_id_seq,
                catalog_id = catalog_id,
                name_key = as_debug!(name_key);
                "get_catalog"
            );

            if catalog_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateCatalogReply { catalog_id })
                } else {
                    Err(KVAppError::AppError(AppError::CatalogAlreadyExists(
                        CatalogAlreadyExists::new(
                            &name_key.catalog_name,
                            format!("create catalog: tenant: {}", name_key.tenant),
                        ),
                    )))
                };
            }

            // Create catalog by inserting these record:
            // (tenant, catalog_name) -> catalog_id
            // (catalog_id) -> catalog_meta
            // (catalog_id) -> (tenant, catalog_name)
            let catalog_id = fetch_id(self, IdGenerator::catalog_id()).await?;
            let id_key = CatalogId { catalog_id };
            let id_to_name_key = CatalogIdToName { catalog_id };

            debug!(catalog_id = catalog_id, name_key = as_debug!(name_key); "new catalog id");

            {
                let condition = vec![
                    txn_cond_seq(name_key, Eq, 0),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                ];
                let if_then = vec![
                    txn_op_put(name_key, serialize_u64(catalog_id)?), /* (tenant, catalog_name) -> catalog_id */
                    txn_op_put(&id_key, serialize_struct(&req.meta)?), /* (catalog_id) -> catalog_meta */
                    txn_op_put(&id_to_name_key, serialize_struct(name_key)?), /* __fd_catalog_id_to_name/<catalog_id> -> (tenant,catalog_name) */
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_catalog"
                );

                if succ {
                    break catalog_id;
                }
            }
        };

        Ok(CreateCatalogReply { catalog_id })
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn get_catalog(&self, req: GetCatalogReq) -> Result<Arc<CatalogInfo>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.inner;

        let (_, catalog_id, _, catalog_meta) =
            get_catalog_or_err(self, name_key, "get_catalog").await?;

        let catalog = CatalogInfo {
            id: CatalogId { catalog_id },
            name_ident: name_key.clone(),
            meta: catalog_meta,
        };

        Ok(Arc::new(catalog))
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn drop_catalog(&self, req: DropCatalogReq) -> Result<DropCatalogReply, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        let ctx = &func_name!();

        let mut trials = txn_trials(None, ctx);

        loop {
            trials.next().unwrap()?;

            let res =
                get_catalog_or_err(self, name_key, format!("drop_catalog: {}", &name_key)).await;

            let (_, catalog_id, catalog_meta_seq, _) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let KVAppError::AppError(AppError::UnknownCatalog(_)) = e {
                        if req.if_exists {
                            return Ok(DropCatalogReply {});
                        }
                    }

                    return Err(e);
                }
            };

            // Delete catalog by deleting these record:
            // (tenant, catalog_name) -> catalog_id
            // (catalog_id) -> catalog_meta
            // (catalog_id) -> (tenant, catalog_name)
            let id_key = CatalogId { catalog_id };
            let id_to_name_key = CatalogIdToName { catalog_id };

            debug!(
                catalog_id = catalog_id,
                name_key = as_debug!(&name_key);
                "catalog keys to delete"
            );

            {
                let condition = vec![txn_cond_seq(&id_key, Eq, catalog_meta_seq)];
                let if_then = vec![
                    txn_op_del(name_key),        // (tenant, catalog_name) -> catalog_id
                    txn_op_del(&id_key),         // (catalog_id) -> catalog_meta
                    txn_op_del(&id_to_name_key), /* __fd_catalog_id_to_name/<catalog_id> -> (tenant,catalog_name) */
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(&name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "drop_catalog"
                );

                if succ {
                    break;
                }
            }
        }

        Ok(DropCatalogReply {})
    }

    #[logcall::logcall("debug")]
    #[minitrace::trace]
    async fn list_catalogs(
        &self,
        req: ListCatalogReq,
    ) -> Result<Vec<Arc<CatalogInfo>>, KVAppError> {
        debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

        let name_key = CatalogNameIdent {
            tenant: req.tenant,
            // Using a empty catalog to to list all
            catalog_name: "".to_string(),
        };

        // Pairs of catalog-name and catalog_id with seq
        let (tenant_catalog_names, catalog_ids) = list_u64_value(self, &name_key).await?;

        // Keys for fetching serialized CatalogMeta from kvapi::KVApi
        let mut kv_keys = Vec::with_capacity(catalog_ids.len());

        for catalog_id in catalog_ids.iter() {
            let k = CatalogId {
                catalog_id: *catalog_id,
            }
            .to_string_key();
            kv_keys.push(k);
        }

        // Batch get all catalog-metas.
        // - A catalog-meta may be already deleted. It is Ok. Just ignore it.

        let seq_metas = self.mget_kv(&kv_keys).await?;
        let mut catalog_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let catalog_meta: CatalogMeta = deserialize_struct(&seq_meta.data)?;

                let catalog_info = CatalogInfo {
                    id: CatalogId {
                        catalog_id: catalog_ids[i],
                    },
                    name_ident: CatalogNameIdent {
                        tenant: name_key.tenant.clone(),
                        catalog_name: tenant_catalog_names[i].catalog_name.clone(),
                    },
                    meta: catalog_meta,
                };
                catalog_infos.push(Arc::new(catalog_info));
            } else {
                debug!(
                    k = as_display!(&kv_keys[i]);
                    "catalog_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(catalog_infos)
    }

    fn name(&self) -> String {
        "SchemaApiImpl".to_string()
    }
}

/// remove copied files for a table.
///
/// Returns number of files that are going to be removed.
async fn remove_table_copied_files(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    table_id: u64,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<usize, KVAppError> {
    let mut n = 0;
    let chunk_size = DEFAULT_MGET_SIZE;

    // `list_keys` list all the `TableCopiedFileNameIdent` of the table.
    // But if a upsert_table_copied_file_info run concurrently, there is chance that
    // `list_keys` may lack of some new inserted TableCopiedFileNameIdent.
    // But since TableCopiedFileNameIdent has expire time, they can be purged by expire time.
    let copied_files = list_table_copied_files(kv_api, table_id).await?;

    for chunk in copied_files.chunks(chunk_size) {
        // Load the `seq` of every copied file
        let seqs = {
            let str_keys: Vec<_> = chunk.iter().map(|f| f.to_string_key()).collect();

            let seq_infos: Vec<(u64, Option<TableCopiedFileInfo>)> =
                mget_pb_values(kv_api, &str_keys).await?;

            seq_infos.into_iter().map(|(seq, _)| seq)
        };

        for (copied_seq, copied_ident) in seqs.zip(chunk) {
            if copied_seq == 0 {
                continue;
            }

            condition.push(txn_cond_seq(copied_ident, Eq, copied_seq));
            if_then.push(txn_op_del(copied_ident));
            n += 1;
        }
    }

    Ok(n)
}

/// List the copied file identities belonging to a table.
async fn list_table_copied_files(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    table_id: u64,
) -> Result<Vec<TableCopiedFileNameIdent>, MetaError> {
    let copied_file_ident = TableCopiedFileNameIdent {
        table_id,
        file: "".to_string(),
    };

    let copied_files = list_keys(kv_api, &copied_file_ident).await?;

    Ok(copied_files)
}

// Return true if drop time is out of `DATA_RETENTION_TIME_IN_DAYS option,
// use DEFAULT_DATA_RETENTION_SECONDS by default.
fn is_drop_time_out_of_retention_time(
    drop_on: &Option<DateTime<Utc>>,
    now: &DateTime<Utc>,
) -> bool {
    if let Some(drop_on) = drop_on {
        return now.timestamp() - drop_on.timestamp() >= DEFAULT_DATA_RETENTION_SECONDS;
    }

    false
}

/// Returns (db_id_seq, db_id, db_meta_seq, db_meta)
pub(crate) async fn get_db_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, DatabaseMeta), KVAppError> {
    let (db_id_seq, db_id) = get_u64_value(kv_api, name_key).await?;
    db_has_to_exist(db_id_seq, name_key, &msg)?;

    let id_key = DatabaseId { db_id };

    let (db_meta_seq, db_meta) = get_pb_value(kv_api, &id_key).await?;
    db_has_to_exist(db_meta_seq, name_key, msg)?;

    Ok((
        db_id_seq,
        db_id,
        db_meta_seq,
        // Safe unwrap(): db_meta_seq > 0 implies db_meta is not None.
        db_meta.unwrap(),
    ))
}

/// Return OK if a db_id or db_meta does not exist by checking the seq.
///
/// Otherwise returns DatabaseAlreadyExists error
fn db_has_to_not_exist(
    seq: u64,
    name_ident: &DatabaseNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        debug!(seq = seq, name_ident = as_debug!(name_ident); "exist");

        Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
            DatabaseAlreadyExists::new(&name_ident.db_name, format!("{}: {}", ctx, name_ident)),
        )))
    }
}

/// Return OK if a table_id or table_meta does not exist by checking the seq.
///
/// Otherwise returns TableAlreadyExists error
fn table_has_to_not_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        debug!(seq = seq, name_ident = as_debug!(name_ident); "exist");

        Err(KVAppError::AppError(AppError::TableAlreadyExists(
            TableAlreadyExists::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    }
}

/// Get the count of tables for one tenant by listing databases and table ids.
///
/// It returns (seq, `u64` value).
/// If the count value is not in the kv space, (0, `u64` value) is returned.
async fn count_tables(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    key: &CountTablesKey,
) -> Result<u64, KVAppError> {
    // For backward compatibility:
    // If the table count of a tenant is not found in kv space,,
    // we should compute the count by listing all tables of the tenant.
    let databases = kv_api
        .list_databases(ListDatabaseReq {
            tenant: key.tenant.clone(),
            filter: None,
        })
        .await?;
    let mut count = 0;
    for db in databases.into_iter() {
        let dbid_tbname = DBIdTableName {
            db_id: db.ident.db_id,
            table_name: "".to_string(),
        };
        let (_, ids) = list_u64_value(kv_api, &dbid_tbname).await?;
        count += ids.len() as u64;
    }
    Ok(count)
}

async fn get_share_table_info_map(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_meta: &TableMeta,
) -> Result<Option<Vec<ShareTableInfoMap>>, KVAppError> {
    if table_meta.shared_by.is_empty() {
        return Ok(None);
    }
    let mut share_table_info_map_vec = vec![];
    for share_id in &table_meta.shared_by {
        let res = get_share_id_to_name_or_err(
            kv_api,
            *share_id,
            format!("get_share_table_info_map: {}", share_id),
        )
        .await;

        let (_seq, share_name) = match res {
            Ok((seq, share_name)) => (seq, share_name),
            Err(e) => match e {
                // ignore UnknownShareId error
                KVAppError::AppError(AppError::UnknownShareId(_)) => {
                    error!("UnknownShareId {} when get_share_table_info_map", share_id);
                    continue;
                }
                _ => return Err(e),
            },
        };
        let res = get_share_meta_by_id_or_err(
            kv_api,
            *share_id,
            format!("get_share_table_info_map: {}", share_id),
        )
        .await;

        let (_share_meta_seq, share_meta) = match res {
            Ok((seq, share_meta)) => (seq, share_meta),
            Err(e) => match e {
                // ignore UnknownShareId error
                KVAppError::AppError(AppError::UnknownShareId(_)) => {
                    error!("UnknownShareId {} when get_share_table_info_map", share_id);
                    continue;
                }
                _ => return Err(e),
            },
        };
        share_table_info_map_vec
            .push(get_share_table_info(kv_api, &share_name, &share_meta).await?);
    }

    Ok(Some(share_table_info_map_vec))
}

async fn get_table_id_from_share_by_name(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    share: &ShareNameIdent,
    table_name: &String,
) -> Result<u64, KVAppError> {
    let res = get_share_or_err(
        kv_api,
        share,
        format!("list_tables_from_share_db: {}", &share),
    )
    .await;

    let (share_id_seq, _share_id, _share_meta_seq, share_meta) = match res {
        Ok(x) => x,
        Err(e) => {
            return Err(e);
        }
    };
    if share_id_seq == 0 {
        return Err(KVAppError::AppError(AppError::WrongShare(WrongShare::new(
            share.to_string_key(),
        ))));
    }

    let mut ids = Vec::with_capacity(share_meta.entries.len());
    for (_, entry) in share_meta.entries.iter() {
        if let ShareGrantObject::Table(table_id) = entry.object {
            ids.push(table_id);
        }
    }

    let table_names = get_table_names_by_ids(kv_api, &ids).await?;
    match table_names.binary_search(table_name) {
        Ok(i) => Ok(ids[i]),
        Err(_) => Err(KVAppError::AppError(AppError::WrongShareObject(
            WrongShareObject::new(table_name.to_string_key()),
        ))),
    }
}

fn build_upsert_table_copied_file_info_conditions(
    table_id: &TableId,
    req: &UpsertTableCopiedFileReq,
    tb_meta_seq: u64,
    fail_if_duplicated: bool,
) -> Result<(Vec<TxnCondition>, Vec<TxnOp>), KVAppError> {
    let mut condition = vec![txn_cond_seq(table_id, Eq, tb_meta_seq)];
    let mut if_then = vec![];

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

    let file_name_infos = req.file_info.clone().into_iter();

    for (file_name, file_info) in file_name_infos {
        let key = TableCopiedFileNameIdent {
            table_id: table_id.table_id,
            file: file_name.to_owned(),
        };
        if fail_if_duplicated {
            // "fail_if_duplicated" mode, assumes files are absent
            condition.push(txn_cond_seq(&key, Eq, 0));
        }
        set_update_expire_operation(&key, &file_info, &req.expire_at, &mut if_then)?;
    }
    Ok((condition, if_then))
}

fn build_upsert_table_deduplicated_label(deduplicated_label: String) -> TxnOp {
    let expire_at = Some(SeqV::<()>::now_ms() / 1000 + 24 * 60 * 60);
    TxnOp {
        request: Some(Request::Put(TxnPutRequest {
            key: deduplicated_label,
            value: 1_i8.to_le_bytes().to_vec(),
            prev_value: false,
            expire_at,
        })),
    }
}

fn set_update_expire_operation(
    key: &TableCopiedFileNameIdent,
    file_info: &TableCopiedFileInfo,
    expire_at_opt: &Option<u64>,
    then_branch: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    match expire_at_opt {
        Some(expire_at) => {
            then_branch.push(txn_op_put_with_expire(
                key,
                serialize_struct(file_info)?,
                *expire_at,
            ));
        }
        None => {
            then_branch.push(txn_op_put(key, serialize_struct(file_info)?));
        }
    }
    Ok(())
}

#[logcall::logcall("debug")]
#[minitrace::trace]
async fn do_get_table_history(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    req: ListDroppedTableReq,
    db_id: u64,
    db_meta: &DatabaseMeta,
) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
    debug!(req = as_debug!(&req); "SchemaApi: {}", func_name!());

    let tenant_dbname = &req.inner;

    // List tables by tenant, db_id, table_name.
    let dbid_tbname_idlist = TableIdListKey {
        db_id,
        table_name: "".to_string(),
    };

    let table_id_list_keys = list_keys(kv_api, &dbid_tbname_idlist).await?;

    let mut tb_info_list = vec![];
    let keys: Vec<String> = table_id_list_keys
        .iter()
        .map(|table_id_list_key| {
            TableIdListKey {
                db_id,
                table_name: table_id_list_key.table_name.clone(),
            }
            .to_string_key()
        })
        .collect();
    let mut table_id_list_keys_iter = table_id_list_keys.into_iter();

    for c in keys.chunks(DEFAULT_MGET_SIZE) {
        let tb_id_list_seq_vec: Vec<(u64, Option<TableIdList>)> = mget_pb_values(kv_api, c).await?;
        for (tb_id_list_seq, tb_id_list_opt) in tb_id_list_seq_vec {
            let table_id_list_key = table_id_list_keys_iter.next().unwrap();
            let tb_id_list = if tb_id_list_seq == 0 {
                continue;
            } else {
                match tb_id_list_opt {
                    Some(list) => list,
                    None => {
                        continue;
                    }
                }
            };

            debug!(
                name = as_display!(&table_id_list_key);
                "get_table_history"
            );

            let inner_keys: Vec<String> = tb_id_list
                .id_list
                .iter()
                .map(|table_id| {
                    TableId {
                        table_id: *table_id,
                    }
                    .to_string_key()
                })
                .collect();
            let mut table_id_iter = tb_id_list.id_list.into_iter();
            for c in inner_keys.chunks(DEFAULT_MGET_SIZE) {
                let tb_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(kv_api, c).await?;
                for (tb_meta_seq, tb_meta) in tb_meta_vec {
                    let table_id = table_id_iter.next().unwrap();
                    if tb_meta_seq == 0 || tb_meta.is_none() {
                        error!("get_table_history cannot find {:?} table_meta", table_id);
                        continue;
                    }

                    // Safe unwrap() because: tb_meta_seq > 0
                    let tb_meta = tb_meta.unwrap();

                    let tenant_dbname_tbname: TableNameIdent = TableNameIdent {
                        tenant: tenant_dbname.tenant.clone(),
                        db_name: tenant_dbname.db_name.clone(),
                        table_name: table_id_list_key.table_name.clone(),
                    };

                    let db_type = db_meta
                        .from_share
                        .clone()
                        .map_or(DatabaseType::NormalDB, |share_ident| {
                            DatabaseType::ShareDB(share_ident)
                        });

                    let tb_info = TableInfo {
                        ident: TableIdent {
                            table_id,
                            seq: tb_meta_seq,
                        },
                        desc: tenant_dbname_tbname.to_string(),
                        name: table_id_list_key.table_name.clone(),
                        meta: tb_meta,
                        tenant: tenant_dbname.tenant.clone(),
                        db_type,
                    };

                    tb_info_list.push(Arc::new(tb_info));
                }
            }
        }
    }

    let filter_tb_infos = tb_info_list
        .clone()
        .into_iter()
        .filter(|tb_info| match req.filter {
            TableInfoFilter::Dropped(drop_on) => tb_info.meta.drop_on.is_some_and(|tb_drop_on| {
                drop_on.map_or(true, |drop_on| {
                    tb_drop_on.timestamp() <= drop_on.timestamp()
                })
            }),
            TableInfoFilter::All => true,
            _ => {
                unreachable!("unreachable");
            }
        })
        .collect::<Vec<_>>();

    Ok(filter_tb_infos)
}

/// Returns (index_id_seq, index_id, index_meta_seq, index_meta)
pub(crate) async fn get_index_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &IndexNameIdent,
) -> Result<(u64, u64, u64, Option<IndexMeta>), KVAppError> {
    let (index_id_seq, index_id) = get_u64_value(kv_api, name_key).await?;
    let id_key = IndexId { index_id };
    let (index_meta_seq, index_meta) = get_pb_value(kv_api, &id_key).await?;

    Ok((index_id_seq, index_id, index_meta_seq, index_meta))
}

async fn gc_dropped_db_by_id(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    db_id: u64,
    tenant: String,
    db_name: String,
) -> Result<(), KVAppError> {
    // List tables by tenant, db_id, table_name.
    let dbid_idlist = DbIdListKey {
        tenant: tenant.clone(),
        db_name,
    };
    let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
        get_pb_value(kv_api, &dbid_idlist).await?;

    let mut db_id_list = match db_id_list_opt {
        Some(list) => list,
        None => return Ok(()),
    };
    for (i, dbid) in db_id_list.id_list.iter().enumerate() {
        if *dbid != db_id {
            continue;
        }
        let dbid = DatabaseId { db_id };
        let (db_meta_seq, _db_meta): (_, Option<DatabaseMeta>) =
            get_pb_value(kv_api, &dbid).await?;
        if db_meta_seq == 0 {
            return Ok(());
        }
        let id_to_name = DatabaseIdToName { db_id };
        let (name_ident_seq, _name_ident): (_, Option<DatabaseNameIdent>) =
            get_pb_value(kv_api, &id_to_name).await?;
        if name_ident_seq == 0 {
            return Ok(());
        }

        let dbid_tbname_idlist = TableIdListKey {
            db_id,
            table_name: "".to_string(),
        };

        let table_id_list_keys = list_keys(kv_api, &dbid_tbname_idlist).await?;
        let keys: Vec<String> = table_id_list_keys
            .iter()
            .map(|table_id_list_key| {
                TableIdListKey {
                    db_id,
                    table_name: table_id_list_key.table_name.clone(),
                }
                .to_string_key()
            })
            .collect();
        let mut condition = vec![];
        let mut if_then = vec![];

        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let tb_id_list_seq_vec: Vec<(u64, Option<TableIdList>)> =
                mget_pb_values(kv_api, c).await?;
            let mut iter = c.iter();
            for (tb_id_list_seq, tb_id_list_opt) in tb_id_list_seq_vec {
                let tb_id_list = match tb_id_list_opt {
                    Some(list) => list,
                    None => {
                        continue;
                    }
                };

                for tb_id in tb_id_list.id_list {
                    gc_dropped_table_data(kv_api, tb_id, &mut condition, &mut if_then).await?;
                    gc_dropped_table_index(kv_api, &tenant, tb_id, &mut if_then).await?;
                }

                let id_key = iter.next().unwrap();
                if_then.push(txn_op_del(id_key));
                condition.push(txn_cond_seq(id_key, Eq, tb_id_list_seq));
            }

            // for id_key in c {
            // if_then.push(txn_op_del(id_key));
            // }
        }
        db_id_list.id_list.remove(i);
        condition.push(txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq));
        // save new db id list
        if_then.push(txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?));

        condition.push(txn_cond_seq(&dbid, Eq, db_meta_seq));
        if_then.push(txn_op_del(&dbid));
        condition.push(txn_cond_seq(&id_to_name, Eq, name_ident_seq));
        if_then.push(txn_op_del(&id_to_name));

        let txn_req = TxnRequest {
            condition,
            if_then,
            else_then: vec![],
        };
        let _resp = kv_api.transaction(txn_req).await?;
        break;
    }

    Ok(())
}

async fn gc_dropped_table_by_id(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    tenant: String,
    db_id: u64,
    table_id: u64,
    table_name: String,
) -> Result<(), KVAppError> {
    // first get TableIdList
    let dbid_tbname_idlist = TableIdListKey { db_id, table_name };
    let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
        get_pb_value(kv_api, &dbid_tbname_idlist).await?;
    let mut tb_id_list = match tb_id_list_opt {
        Some(list) => list,
        None => return Ok(()),
    };

    for (i, tb_id) in tb_id_list.id_list.iter().enumerate() {
        if *tb_id != table_id {
            continue;
        }

        tb_id_list.id_list.remove(i);
        // construct the txn request
        let mut condition = vec![
            // condition: table id list not changed
            txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq),
        ];
        let mut if_then = vec![
            // save new table id list
            txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?),
        ];
        gc_dropped_table_data(kv_api, table_id, &mut condition, &mut if_then).await?;
        gc_dropped_table_index(kv_api, &tenant, table_id, &mut if_then).await?;

        let txn_req = TxnRequest {
            condition,
            if_then,
            else_then: vec![],
        };
        let _resp = kv_api.transaction(txn_req).await?;
        break;
    }

    Ok(())
}

async fn gc_dropped_table_data(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    table_id: u64,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let tbid = TableId { table_id };
    let id_to_name = TableIdToName { table_id };

    // Get meta data
    let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(kv_api, &tbid).await?;

    if tb_meta_seq == 0 || tb_meta.is_none() {
        error!(
            "gc_dropped_table_by_id cannot find {:?} table_meta",
            table_id
        );
        return Ok(());
    }

    // Get id -> name mapping
    let (name_seq, _name): (_, Option<DBIdTableName>) = get_pb_value(kv_api, &id_to_name).await?;

    // table id not changed
    condition.push(txn_cond_seq(&tbid, Eq, tb_meta_seq));
    // consider only when TableIdToName exist
    if name_seq != 0 {
        // table id to name not changed
        condition.push(txn_cond_seq(&id_to_name, Eq, name_seq));
        // remove table id to name
        if_then.push(txn_op_del(&id_to_name));
    }
    // remove table meta
    if_then.push(txn_op_del(&tbid));

    remove_table_copied_files(kv_api, table_id, condition, if_then).await?;

    Ok(())
}

async fn gc_dropped_table_index(
    kv_api: &impl kvapi::KVApi<Error = MetaError>,
    tenant: &str,
    table_id: u64,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    // Get index id list by `prefix_list` "<prefix>/<tenant>"
    let prefix_key = kvapi::KeyBuilder::new_prefixed(IndexNameIdent::PREFIX)
        .push_str(tenant)
        .done();

    let id_list = kv_api.prefix_list_kv(&prefix_key).await?;
    let mut id_name_list = Vec::with_capacity(id_list.len());
    for (key, seq) in id_list.iter() {
        let name_ident = IndexNameIdent::from_str_key(key).map_err(|e| {
            KVAppError::MetaError(MetaError::from(InvalidReply::new("list_indexes", &e)))
        })?;
        let index_id = deserialize_u64(&seq.data)?;
        id_name_list.push((index_id.0, name_ident.index_name));
    }

    if id_name_list.is_empty() {
        return Ok(());
    }

    // Get index ids of this table
    let index_ids = {
        let index_metas = get_index_metas_by_ids(kv_api, id_name_list).await?;
        index_metas
            .into_iter()
            .filter(|(_, _, meta)| table_id == meta.table_id)
            .map(|(id, _, _)| id)
            .collect::<Vec<_>>()
    };

    let id_to_name_keys = index_ids
        .iter()
        .map(|id| IndexIdToName { index_id: *id }.to_string_key())
        .collect::<Vec<_>>();

    // Get (tenant, index_name) list by index ids
    let index_name_list: Result<Vec<IndexNameIdent>, MetaNetworkError> = kv_api
        .mget_kv(&id_to_name_keys)
        .await?
        .iter()
        .filter(|seq_v| seq_v.is_some())
        .map(|seq_v| {
            let index_name_ident: IndexNameIdent =
                deserialize_struct(&seq_v.as_ref().unwrap().data)?;
            Ok(index_name_ident)
        })
        .collect();

    let index_name_list = index_name_list?;

    debug_assert_eq!(index_ids.len(), index_name_list.len());

    for (index_id, index_name_ident) in index_ids.iter().zip(index_name_list.iter()) {
        let id_key = IndexId {
            index_id: *index_id,
        };
        let id_to_name_key = IndexIdToName {
            index_id: *index_id,
        };
        if_then.push(txn_op_del(&id_key)); // (index_id) -> index_meta
        if_then.push(txn_op_del(&id_to_name_key)); // __fd_index_id_to_name/<index_id> -> (tenant,index_name)
        if_then.push(txn_op_del(index_name_ident)); // (tenant, index_name) -> index_id
    }

    Ok(())
}

/// Returns (catalog_id_seq, catalog_id, db_meta_seq, catalog_meta)
pub(crate) async fn get_catalog_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &CatalogNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, CatalogMeta), KVAppError> {
    let (catalog_id_seq, catalog_id) = get_u64_value(kv_api, name_key).await?;
    catalog_has_to_exist(catalog_id_seq, name_key, &msg)?;

    let id_key = CatalogId { catalog_id };

    let (catalog_meta_seq, catalog_meta) = get_pb_value(kv_api, &id_key).await?;
    catalog_has_to_exist(catalog_meta_seq, name_key, msg)?;

    Ok((
        catalog_id_seq,
        catalog_id,
        catalog_meta_seq,
        // Safe unwrap(): catalog_meta_seq > 0 implies db_meta is not None.
        catalog_meta.unwrap(),
    ))
}

/// Return OK if a catalog_id or catalog_meta exists by checking the seq.
///
/// Otherwise returns UnknownCatalog error
pub fn catalog_has_to_exist(
    seq: u64,
    catalog_name_ident: &CatalogNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, catalog_name_ident = as_debug!(catalog_name_ident); "catalog does not exist");

        Err(KVAppError::AppError(AppError::UnknownCatalog(
            UnknownCatalog::new(
                &catalog_name_ident.catalog_name,
                format!("{}: {}", msg, catalog_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

async fn update_mask_policy_table_id_list(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
    tenant: String,
    name: String,
    table_id: u64,
    add: bool,
) -> Result<(), KVAppError> {
    let id_list_key = MaskpolicyTableIdListKey { tenant, name };

    let (id_list_seq, id_list_opt): (_, Option<MaskpolicyTableIdList>) =
        get_pb_value(kv_api, &id_list_key).await?;
    if let Some(mut id_list) = id_list_opt {
        if add {
            id_list.id_list.insert(table_id);
        } else {
            id_list.id_list.remove(&table_id);
        }

        condition.push(txn_cond_seq(&id_list_key, Eq, id_list_seq));
        if_then.push(txn_op_put(&id_list_key, serialize_struct(&id_list)?));
    }

    Ok(())
}

async fn update_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    action: &SetTableColumnMaskPolicyAction,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
    tenant: String,
    table_id: u64,
) -> Result<(), KVAppError> {
    match action {
        SetTableColumnMaskPolicyAction::Set(new_mask_name, old_mask_name_opt) => {
            update_mask_policy_table_id_list(
                kv_api,
                condition,
                if_then,
                tenant.clone(),
                new_mask_name.clone(),
                table_id,
                true,
            )
            .await?;
            if let Some(old_mask_name) = old_mask_name_opt {
                update_mask_policy_table_id_list(
                    kv_api,
                    condition,
                    if_then,
                    tenant.clone(),
                    old_mask_name.clone(),
                    table_id,
                    false,
                )
                .await?;
            }
        }
        SetTableColumnMaskPolicyAction::Unset(mask_name) => {
            update_mask_policy_table_id_list(
                kv_api,
                condition,
                if_then,
                tenant.clone(),
                mask_name.clone(),
                table_id,
                false,
            )
            .await?;
        }
    }

    Ok(())
}
