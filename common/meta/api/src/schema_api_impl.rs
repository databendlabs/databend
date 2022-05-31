// Copyright 2021 Datafuse Labs.
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

use std::fmt::Display;
use std::sync::Arc;

use anyerror::AnyError;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DBIdTableName;
use common_meta_app::schema::DatabaseId;
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::DbIdList;
use common_meta_app::schema::DbIdListKey;
use common_meta_app::schema::DropDatabaseReply;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::ListDatabaseReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdList;
use common_meta_app::schema::TableIdListKey;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::app_error::AppError;
use common_meta_types::app_error::CreateDatabaseWithDropTime;
use common_meta_types::app_error::CreateTableWithDropTime;
use common_meta_types::app_error::DatabaseAlreadyExists;
use common_meta_types::app_error::DropDbWithDropTime;
use common_meta_types::app_error::DropTableWithDropTime;
use common_meta_types::app_error::TableAlreadyExists;
use common_meta_types::app_error::TableVersionMismatched;
use common_meta_types::app_error::UndropDbHasNoHistory;
use common_meta_types::app_error::UndropDbWithNoDropTime;
use common_meta_types::app_error::UndropTableAlreadyExists;
use common_meta_types::app_error::UndropTableHasNoHistory;
use common_meta_types::app_error::UndropTableWithNoDropTime;
use common_meta_types::app_error::UnknownDatabase;
use common_meta_types::app_error::UnknownTable;
use common_meta_types::app_error::UnknownTableId;
use common_meta_types::txn_condition;
use common_meta_types::txn_op::Request;
use common_meta_types::ConditionResult;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::MetaError;
use common_meta_types::MetaId;
use common_meta_types::Operation;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReq;
use common_proto_conv::FromToProto;
use common_tracing::tracing;
use txn_condition::Target;
use ConditionResult::Eq;

use crate::DatabaseIdGen;
use crate::KVApi;
use crate::KVApiKey;
use crate::SchemaApi;
use crate::TableIdGen;

const DEFAULT_DATA_RETENTION_SECONDS: i64 = 24 * 60 * 60;

/// SchemaApi is implemented upon KVApi.
/// Thus every type that impl KVApi impls SchemaApi.
#[tonic::async_trait]
impl<KV: KVApi> SchemaApi for KV {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, MetaError> {
        let name_key = &req.name_ident;

        if req.meta.drop_on.is_some() {
            return Err(MetaError::AppError(AppError::CreateDatabaseWithDropTime(
                CreateDatabaseWithDropTime::new(&name_key.db_name),
            )));
        }

        loop {
            // Get db by name to ensure absence
            let (db_id_seq, db_id) = get_id_value(self, name_key).await?;
            tracing::debug!(db_id_seq, db_id, ?name_key, "get_database");

            if db_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateDatabaseReply { db_id })
                } else {
                    Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
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
                get_struct_value(self, &dbid_idlist).await?;

            let mut db_id_list = if db_id_list_seq == 0 {
                DbIdList::new()
            } else {
                match db_id_list_opt {
                    Some(list) => list,
                    None => DbIdList::new(),
                }
            };

            // Create db by inserting these record:
            // (tenant, db_name) -> db_id
            // (db_id) -> db_meta
            // append db_id into _fd_db_id_list/<tenant>/<db_name>

            let db_id = fetch_id(self, DatabaseIdGen {}).await?;
            let id_key = DatabaseId { db_id };

            tracing::debug!(db_id, name_key = debug(&name_key), "new database id");

            {
                // append db_id into db_id_list
                db_id_list.append(db_id);

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0)?,
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq)?,
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_id(db_id)?)?, // (tenant, db_name) -> db_id
                        txn_op_put(&id_key, serialize_struct(&req.meta)?)?, // (db_id) -> db_meta
                        txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?)?, // _fd_db_id_list/<tenant>/<db_name> -> db_id_list
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "create_database"
                );

                if succ {
                    return Ok(CreateDatabaseReply { db_id });
                }
            }
        }
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, MetaError> {
        let tenant_dbname = &req.name_ident;

        loop {
            let res = get_db_or_err(
                self,
                tenant_dbname,
                format!("drop_database: {}", &tenant_dbname),
            )
            .await;

            let (db_id_seq, db_id, db_meta_seq, mut db_meta) = match res {
                Ok(x) => x,
                Err(e) => {
                    if let MetaError::AppError(AppError::UnknownDatabase(_)) = e {
                        if req.if_exists {
                            return Ok(DropDatabaseReply {});
                        }
                    }

                    return Err(e);
                }
            };

            // Delete db by these operations:
            // del (tenant, db_name) -> db_id
            // set db_meta.drop_on = now and update (db_id) -> db_meta

            let db_id_key = DatabaseId { db_id };

            tracing::debug!(db_id, name_key = debug(&tenant_dbname), "drop_database");

            {
                // drop a table with drop time
                if db_meta.drop_on.is_some() {
                    return Err(MetaError::AppError(AppError::DropDbWithDropTime(
                        DropDbWithDropTime::new(&tenant_dbname.db_name),
                    )));
                }
                // update drop on time
                db_meta.drop_on = Some(Utc::now());

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(tenant_dbname, Eq, db_id_seq)?,
                        txn_cond_seq(&db_id_key, Eq, db_meta_seq)?,
                    ],
                    if_then: vec![
                        txn_op_del(tenant_dbname)?, // (tenant, db_name) -> db_id
                        txn_op_put(&db_id_key, serialize_struct(&db_meta)?)?, // (db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname),
                    id = debug(&db_id_key),
                    succ = display(succ),
                    "drop_database"
                );

                if succ {
                    return Ok(DropDatabaseReply {});
                }
            }
        }
    }

    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> Result<UndropDatabaseReply, MetaError> {
        let name_key = &req.name_ident;

        loop {
            let res =
                get_db_or_err(self, name_key, format!("undrop_database: {}", &name_key)).await;

            if res.is_ok() {
                return Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
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
                get_struct_value(self, &dbid_idlist).await?;

            let mut db_id_list = if db_id_list_seq == 0 {
                return Err(MetaError::AppError(AppError::UndropDbHasNoHistory(
                    UndropDbHasNoHistory::new(&name_key.db_name),
                )));
            } else {
                match db_id_list_opt {
                    Some(list) => list,
                    None => {
                        return Err(MetaError::AppError(AppError::UndropDbHasNoHistory(
                            UndropDbHasNoHistory::new(&name_key.db_name),
                        )))
                    }
                }
            };

            // Return error if there is no db id history.
            let db_id = match db_id_list.last() {
                Some(db_id) => *db_id,
                None => {
                    return Err(MetaError::AppError(AppError::UndropDbHasNoHistory(
                        UndropDbHasNoHistory::new(&name_key.db_name),
                    )))
                }
            };

            // get db_meta of the last db id
            let dbid = DatabaseId { db_id };
            let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) =
                get_struct_value(self, &dbid).await?;

            tracing::debug!(db_id, name_key = debug(&name_key), "undrop_database");

            {
                // reset drop on time
                let mut db_meta = db_meta.unwrap();
                // undrop a table with no drop time
                if db_meta.drop_on.is_none() {
                    return Err(MetaError::AppError(AppError::UndropDbWithNoDropTime(
                        UndropDbWithNoDropTime::new(&name_key.db_name),
                    )));
                }
                db_meta.drop_on = None;

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0)?,
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq)?,
                        txn_cond_seq(&dbid, Eq, db_meta_seq)?,
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_id(db_id)?)?, // (tenant, db_name) -> db_id
                        txn_op_put(&dbid, serialize_struct(&db_meta)?)?, // (db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name_key = debug(&name_key),
                    succ = display(succ),
                    "undrop_database"
                );

                if succ {
                    return Ok(UndropDatabaseReply {});
                }
            }
        }
    }

    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, MetaError> {
        let tenant_dbname = &req.name_ident;
        let tenant_newdbname = DatabaseNameIdent {
            tenant: tenant_dbname.tenant.clone(),
            db_name: req.new_db_name.clone(),
        };

        loop {
            // get old db, not exists return err
            let (old_db_id_seq, old_db_id) = get_id_value(self, tenant_dbname).await?;
            if req.if_exists {
                if old_db_id_seq == 0 {
                    return Ok(RenameDatabaseReply {});
                }
            } else {
                db_has_to_exist(old_db_id_seq, tenant_dbname, "rename_database: src (db)")?;
            }

            tracing::debug!(
                old_db_id,
                tenant_dbname = debug(&tenant_dbname),
                "rename_database"
            );

            // get new db, exists return err
            let (db_id_seq, _db_id) = get_id_value(self, &tenant_newdbname).await?;
            db_has_to_not_exist(db_id_seq, &tenant_newdbname, "rename_database")?;

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist = DbIdListKey {
                tenant: tenant_dbname.tenant.clone(),
                db_name: tenant_dbname.db_name.clone(),
            };
            let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
                get_struct_value(self, &dbid_idlist).await?;
            let mut db_id_list: DbIdList;
            if db_id_list_seq == 0 {
                // may the the database is created before add db_id_list, so we just add the id into the list.
                db_id_list = DbIdList::new();
                db_id_list.append(old_db_id);
            } else {
                match db_id_list_opt {
                    Some(list) => db_id_list = list,
                    None => {
                        // may the the database is created before add db_id_list, so we just add the id into the list.
                        db_id_list = DbIdList::new();
                        db_id_list.append(old_db_id);
                    }
                }
            };

            if let Some(last_db_id) = db_id_list.last() {
                if *last_db_id != old_db_id {
                    return Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            &tenant_dbname.db_name,
                            format!("rename_database: {} with a wrong db id", tenant_dbname),
                        ),
                    )));
                }
            } else {
                return Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
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
                get_struct_value(self, &new_dbid_idlist).await?;
            let mut new_db_id_list: DbIdList;
            if new_db_id_list_seq == 0 {
                new_db_id_list = DbIdList::new();
            } else {
                match new_db_id_list_opt {
                    Some(list) => new_db_id_list = list,
                    None => {
                        new_db_id_list = DbIdList::new();
                    }
                }
            };

            // rename database
            {
                // move db id from old db id list to new db id list
                db_id_list.pop();
                new_db_id_list.append(old_db_id);

                let txn_req = TxnRequest {
                    condition: vec![
                        // Prevent renaming or deleting in other threads.
                        txn_cond_seq(tenant_dbname, Eq, old_db_id_seq)?,
                        txn_cond_seq(&tenant_newdbname, Eq, 0)?,
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq)?,
                        txn_cond_seq(&new_dbid_idlist, Eq, new_db_id_list_seq)?,
                    ],
                    if_then: vec![
                        txn_op_del(tenant_dbname)?, // del old_db_name
                        //Renaming db should not affect the seq of db_meta. Just modify db name.
                        txn_op_put(&tenant_newdbname, serialize_id(old_db_id)?)?, // (tenant, new_db_name) -> old_db_id
                        txn_op_put(&new_dbid_idlist, serialize_struct(&new_db_id_list)?)?, // _fd_db_id_list/tenant/new_db_name -> new_db_id_list
                        txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?)?, // _fd_db_id_list/tenant/db_name -> db_id_list
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname),
                    to = debug(&tenant_newdbname),
                    database_id = debug(&old_db_id),
                    succ = display(succ),
                    "rename_database"
                );

                if succ {
                    return Ok(RenameDatabaseReply {});
                }
            }
        }
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, MetaError> {
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

    async fn get_database_history(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        // List tables by tenant, db_id, table_name.
        let dbid_tbname_idlist = DbIdListKey {
            tenant: req.tenant,
            // Using a empty db to to list all
            db_name: "".to_string(),
        };
        let db_id_list_keys = list_keys(self, &dbid_tbname_idlist).await?;

        let mut db_info_list = vec![];
        let utc: DateTime<Utc> = Utc::now();
        for db_id_list_key in db_id_list_keys.iter() {
            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist = DbIdListKey {
                tenant: db_id_list_key.tenant.clone(),
                db_name: db_id_list_key.db_name.clone(),
            };
            let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
                get_struct_value(self, &dbid_idlist).await?;

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

            for db_id in db_id_list.id_list.iter() {
                let dbid = DatabaseId { db_id: *db_id };

                let (db_meta_seq, db_meta): (_, Option<DatabaseMeta>) =
                    get_struct_value(self, &dbid).await?;
                if db_meta_seq == 0 || db_meta.is_none() {
                    tracing::error!("get_database_history cannot find {:?} db_meta", db_id);
                    continue;
                }
                let db_meta = db_meta.unwrap();
                if is_db_out_of_retention_time(&db_meta, &utc) {
                    continue;
                }

                let db = DatabaseInfo {
                    ident: DatabaseIdent {
                        db_id: *db_id,
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

        return Ok(db_info_list);
    }

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        let name_key = DatabaseNameIdent {
            tenant: req.tenant,
            // Using a empty db to to list all
            db_name: "".to_string(),
        };

        // Pairs of db-name and db_id with seq
        let (tenant_dbnames, db_ids) = list_id_value(self, &name_key).await?;

        // Keys for fetching serialized DatabaseMeta from KVApi
        let mut kv_keys = Vec::with_capacity(db_ids.len());

        for db_id in db_ids.iter() {
            let k = DatabaseId { db_id: *db_id }.to_key();
            kv_keys.push(k);
        }

        // Batch get all db-metas.
        // - A db-meta may be already deleted. It is Ok. Just ignore it.

        let seq_metas = self.mget_kv(&kv_keys).await?;
        let mut db_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let db_meta: DatabaseMeta =
                    deserialize_struct(&seq_meta.data).map_err(meta_encode_err)?;

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
                tracing::debug!(
                    k = display(&kv_keys[i]),
                    "db_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(db_infos)
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, MetaError> {
        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();

        if req.table_meta.drop_on.is_some() {
            return Err(MetaError::AppError(AppError::CreateTableWithDropTime(
                CreateTableWithDropTime::new(&tenant_dbname_tbname.table_name),
            )));
        }

        loop {
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "create_table").await?;

            // Get table by tenant,db_id, table_name to assert absence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            let (tb_id_seq, tb_id) = get_id_value(self, &dbid_tbname).await?;
            if tb_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateTableReply { table_id: tb_id })
                } else {
                    Err(MetaError::AppError(AppError::TableAlreadyExists(
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
                get_struct_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list = if tb_id_list_seq == 0 {
                TableIdList::new()
            } else {
                match tb_id_list_opt {
                    Some(list) => list,
                    None => TableIdList::new(),
                }
            };

            // Create table by inserting these record:
            // (db_id, table_name) -> table_id
            // (table_id) -> table_meta
            // append table_id into _fd_table_id_list/db_id/table_name

            let table_id = fetch_id(self, TableIdGen {}).await?;

            let tbid = TableId { table_id };

            tracing::debug!(
                table_id,
                name = debug(&tenant_dbname_tbname),
                "new table id"
            );

            {
                // append new table_id into list
                tb_id_list.append(table_id);

                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq)?,
                        // no other table with the same name is inserted.
                        txn_cond_seq(&dbid_tbname, Eq, 0)?,
                        // no other table id with the same name is append.
                        txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq)?,
                    ],
                    if_then: vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        // TODO: test this when old metasrv is replaced with kv-txn based SchemaApi.
                        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?)?, // (db_id) -> db_meta
                        txn_op_put(&dbid_tbname, serialize_id(table_id)?)?, // (tenant, db_id, tb_name) -> tb_id
                        txn_op_put(&tbid, serialize_struct(&req.table_meta)?)?, // (tenant, db_id, tb_id) -> tb_meta
                        txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?)?, // _fd_table_id_list/db_id/table_name -> tb_id_list
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname_tbname),
                    id = debug(&tbid),
                    succ = display(succ),
                    "create_table"
                );

                if succ {
                    return Ok(CreateTableReply { table_id });
                }
            }
        }
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply, MetaError> {
        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();

        loop {
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "drop_table").await?;

            // Get table by tenant,db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            let (tb_id_seq, table_id) = get_id_value(self, &dbid_tbname).await?;
            if tb_id_seq == 0 {
                return if req.if_exists {
                    Ok(DropTableReply {})
                } else {
                    Err(MetaError::AppError(AppError::UnknownTable(
                        UnknownTable::new(
                            &tenant_dbname_tbname.table_name,
                            format!("drop_table: {}", tenant_dbname_tbname),
                        ),
                    )))
                };
            }

            let tbid = TableId { table_id };

            let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) =
                get_struct_value(self, &tbid).await?;

            // Delete table by these operations:
            // del (db_id, table_name) -> table_id
            // set table_meta.drop_on = now and update (table_id) -> table_meta

            tracing::debug!(
                ident = display(&tbid),
                name = display(&tenant_dbname_tbname),
                "drop table"
            );

            {
                // update drop on time
                let mut tb_meta = tb_meta.unwrap();
                // drop a table with drop_on time
                if tb_meta.drop_on.is_some() {
                    return Err(MetaError::AppError(AppError::DropTableWithDropTime(
                        DropTableWithDropTime::new(&tenant_dbname_tbname.table_name),
                    )));
                }

                tb_meta.drop_on = Some(Utc::now());

                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq)?,
                        // still this table id
                        txn_cond_seq(&dbid_tbname, Eq, tb_id_seq)?,
                        // table is not changed
                        txn_cond_seq(&tbid, Eq, tb_meta_seq)?,
                    ],
                    if_then: vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        // TODO: test this when old metasrv is replaced with kv-txn based SchemaApi.
                        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?)?, // (db_id) -> db_meta
                        txn_op_del(&dbid_tbname)?, // (db_id, tb_name) -> tb_id
                        txn_op_put(&tbid, serialize_struct(&tb_meta)?)?, // (tenant, db_id, tb_id) -> tb_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname_tbname),
                    id = debug(&tbid),
                    succ = display(succ),
                    "drop_table"
                );

                if succ {
                    return Ok(DropTableReply {});
                }
            }
        }
    }

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply, MetaError> {
        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();

        loop {
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "undrop_table").await?;

            // Get table by tenant,db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            // If table id already exists, return error.
            let (tb_id_seq, table_id) = get_id_value(self, &dbid_tbname).await?;
            if tb_id_seq > 0 || table_id > 0 {
                return Err(MetaError::AppError(AppError::UndropTableAlreadyExists(
                    UndropTableAlreadyExists::new(&tenant_dbname_tbname.table_name),
                )));
            }

            // get table id list from _fd_table_id_list/db_id/table_name
            let dbid_tbname_idlist = TableIdListKey {
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };
            let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
                get_struct_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list = if tb_id_list_seq == 0 {
                return Err(MetaError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                )));
            } else {
                match tb_id_list_opt {
                    Some(list) => list,
                    None => {
                        return Err(MetaError::AppError(AppError::UndropTableHasNoHistory(
                            UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                        )))
                    }
                }
            };

            // Return error if there is no table id history.
            let table_id = match tb_id_list.last() {
                Some(table_id) => *table_id,
                None => {
                    return Err(MetaError::AppError(AppError::UndropTableHasNoHistory(
                        UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                    )))
                }
            };

            // get tb_meta of the last table id
            let tbid = TableId { table_id };
            let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) =
                get_struct_value(self, &tbid).await?;

            // add drop_on time on table meta
            // (db_id, table_name) -> table_id

            tracing::debug!(
                ident = display(&tbid),
                name = display(&tenant_dbname_tbname),
                "undrop table"
            );

            {
                // reset drop on time
                let mut tb_meta = tb_meta.unwrap();
                // undrop a table with no drop_on time
                if tb_meta.drop_on.is_none() {
                    return Err(MetaError::AppError(AppError::UndropTableWithNoDropTime(
                        UndropTableWithNoDropTime::new(&tenant_dbname_tbname.table_name),
                    )));
                }
                tb_meta.drop_on = None;

                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq)?,
                        // still this table id
                        txn_cond_seq(&dbid_tbname, Eq, tb_id_seq)?,
                        // table is not changed
                        txn_cond_seq(&tbid, Eq, tb_meta_seq)?,
                    ],
                    if_then: vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        // TODO: test this when old metasrv is replaced with kv-txn based SchemaApi.
                        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?)?, // (db_id) -> db_meta
                        txn_op_put(&dbid_tbname, serialize_id(table_id)?)?, // (tenant, db_id, tb_name) -> tb_id
                        //txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?)?, // _fd_table_id_list/db_id/table_name -> tb_id_list
                        txn_op_put(&tbid, serialize_struct(&tb_meta)?)?, // (tenant, db_id, tb_id) -> tb_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname_tbname),
                    id = debug(&tbid),
                    succ = display(succ),
                    "undrop_table"
                );

                if succ {
                    return Ok(UndropTableReply {});
                }
            }
        }
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, MetaError> {
        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();
        let tenant_newdbname_newtbname = TableNameIdent {
            tenant: tenant_dbname_tbname.tenant.clone(),
            db_name: req.new_db_name.clone(),
            table_name: req.new_table_name.clone(),
        };

        loop {
            // Get db by name to ensure presence

            let (_, db_id, db_meta_seq, db_meta) =
                get_db_or_err(self, &tenant_dbname, "rename_table").await?;

            // Get table by db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let (tb_id_seq, table_id) = get_id_value(self, &dbid_tbname).await?;
            if req.if_exists {
                if tb_id_seq == 0 {
                    // TODO: table does not exist, can not return table id.
                    return Ok(RenameTableReply { table_id: 0 });
                }
            } else {
                table_has_to_exist(
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
                get_struct_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list: TableIdList;
            if tb_id_list_seq == 0 {
                // may the the table is created before add db_id_list, so we just add the id into the list.
                tb_id_list = TableIdList::new();
                tb_id_list.append(table_id);
            } else {
                match tb_id_list_opt {
                    Some(list) => tb_id_list = list,
                    None => {
                        // may the the table is created before add db_id_list, so we just add the id into the list.
                        tb_id_list = TableIdList::new();
                        tb_id_list.append(table_id);
                    }
                }
            };

            if let Some(last_table_id) = tb_id_list.last() {
                if *last_table_id != table_id {
                    return Err(MetaError::AppError(AppError::UnknownTable(
                        UnknownTable::new(
                            &req.name_ident.table_name,
                            format!("{}: {}", "rename table", tenant_dbname_tbname),
                        ),
                    )));
                }
            } else {
                return Err(MetaError::AppError(AppError::UnknownTable(
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
            let (new_tb_id_seq, _new_tb_id) = get_id_value(self, &newdbid_newtbname).await?;
            table_has_to_not_exist(new_tb_id_seq, &tenant_newdbname_newtbname, "rename_table")?;

            let new_dbid_tbname_idlist = TableIdListKey {
                db_id: new_db_id,
                table_name: req.new_table_name.clone(),
            };
            let (new_tb_id_list_seq, new_tb_id_list_opt): (_, Option<TableIdList>) =
                get_struct_value(self, &new_dbid_tbname_idlist).await?;

            let mut new_tb_id_list: TableIdList;
            if new_tb_id_list_seq == 0 {
                new_tb_id_list = TableIdList::new();
            } else {
                match new_tb_id_list_opt {
                    Some(list) => new_tb_id_list = list,
                    None => {
                        new_tb_id_list = TableIdList::new();
                    }
                }
            };

            {
                // move table id from old table id list to new table id list
                tb_id_list.pop();
                new_tb_id_list.append(table_id);

                let condition = vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq)?,
                    txn_cond_seq(&DatabaseId { db_id: new_db_id }, Eq, new_db_meta_seq)?,
                    // table_name->table_id does not change.
                    // Updating the table meta is ok.
                    txn_cond_seq(&dbid_tbname, Eq, tb_id_seq)?,
                    txn_cond_seq(&newdbid_newtbname, Eq, 0)?,
                    // no other table id with the same name is append.
                    txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq)?,
                    txn_cond_seq(&new_dbid_tbname_idlist, Eq, new_tb_id_list_seq)?,
                ];

                let mut then_ops = vec![
                    txn_op_del(&dbid_tbname)?, // (db_id, tb_name) -> tb_id
                    txn_op_put(&newdbid_newtbname, serialize_id(table_id)?)?, // (db_id, new_tb_name) -> tb_id
                    // Changing a table in a db has to update the seq of db_meta,
                    // to block the batch-delete-tables when deleting a db.
                    // TODO: test this when old metasrv is replaced with kv-txn based SchemaApi.
                    txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?)?, // (db_id) -> db_meta
                    txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?)?, // _fd_table_id_list/db_id/old_table_name -> tb_id_list
                    txn_op_put(&new_dbid_tbname_idlist, serialize_struct(&new_tb_id_list)?)?, // _fd_table_id_list/db_id/new_table_name -> tb_id_list
                ];

                if db_id != new_db_id {
                    then_ops.push(
                        // TODO: test this when old metasrv is replaced with kv-txn based SchemaApi.
                        txn_op_put(
                            &DatabaseId { db_id: new_db_id },
                            serialize_struct(&new_db_meta)?,
                        )?, // (db_id) -> db_meta
                    );
                }

                let txn_req = TxnRequest {
                    condition,
                    if_then: then_ops,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname_tbname),
                    to = debug(&newdbid_newtbname),
                    table_id = debug(&table_id),
                    succ = display(succ),
                    "rename_table"
                );

                if succ {
                    return Ok(RenameTableReply { table_id });
                }
            }
        }
    }

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, MetaError> {
        let tenant_dbname_tbname = &req.inner;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();

        // Get db by name to ensure presence

        let (db_id_seq, db_id) = get_id_value(self, &tenant_dbname).await?;
        tracing::debug!(db_id_seq, db_id, ?tenant_dbname_tbname, "get database");

        db_has_to_exist(
            db_id_seq,
            &tenant_dbname,
            format!("get_table: {}", tenant_dbname_tbname),
        )?;

        // Get table by tenant,db_id, table_name to assert presence.

        let dbid_tbname = DBIdTableName {
            db_id,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };

        let (tb_id_seq, table_id) = get_id_value(self, &dbid_tbname).await?;
        table_has_to_exist(tb_id_seq, tenant_dbname_tbname, "get_table")?;

        let tbid = TableId { table_id };

        let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_struct_value(self, &tbid).await?;

        table_has_to_exist(
            tb_meta_seq,
            tenant_dbname_tbname,
            format!("get_table meta by: {}", tbid),
        )?;

        tracing::debug!(
            ident = display(&tbid),
            name = display(&tenant_dbname_tbname),
            table_meta = debug(&tb_meta),
            "get_table"
        );

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id,
                seq: tb_meta_seq,
            },
            desc: tenant_dbname_tbname.to_string(),
            name: tenant_dbname_tbname.table_name.clone(),
            // Safe unwrap() because: tb_meta_seq > 0
            meta: tb_meta.unwrap(),
        };

        return Ok(Arc::new(tb_info));
    }

    async fn get_table_history(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence

        let (db_id_seq, db_id) = get_id_value(self, tenant_dbname).await?;
        tracing::debug!(
            db_id_seq,
            db_id,
            ?tenant_dbname,
            "get database for listing table"
        );

        db_has_to_exist(db_id_seq, tenant_dbname, "list_tables")?;

        // List tables by tenant, db_id, table_name.
        let dbid_tbname_idlist = TableIdListKey {
            db_id,
            table_name: "".to_string(),
        };

        let table_id_list_keys = list_keys(self, &dbid_tbname_idlist).await?;

        let mut tb_info_list = vec![];
        let utc: DateTime<Utc> = Utc::now();
        for table_id_list_key in table_id_list_keys.iter() {
            // get table id list from _fd_table_id_list/db_id/table_name
            let dbid_tbname_idlist = TableIdListKey {
                db_id,
                table_name: table_id_list_key.table_name.clone(),
            };
            let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
                get_struct_value(self, &dbid_tbname_idlist).await?;

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

            tracing::debug!(name = display(&table_id_list_key), "get_table_history");

            for table_id in tb_id_list.id_list.iter() {
                let tbid = TableId {
                    table_id: *table_id,
                };

                let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) =
                    get_struct_value(self, &tbid).await?;
                if tb_meta_seq == 0 || tb_meta.is_none() {
                    tracing::error!("get_table_history cannot find {:?} table_meta", table_id);
                    continue;
                }

                // Safe unwrap() because: tb_meta_seq > 0
                let tb_meta = tb_meta.unwrap();
                if is_table_out_of_retention_time(&tb_meta, &utc) {
                    continue;
                }

                let tenant_dbname_tbname: TableNameIdent = TableNameIdent {
                    tenant: tenant_dbname.tenant.clone(),
                    db_name: tenant_dbname.db_name.clone(),
                    table_name: table_id_list_key.table_name.clone(),
                };

                let tb_info = TableInfo {
                    ident: TableIdent {
                        table_id: *table_id,
                        seq: tb_meta_seq,
                    },
                    desc: tenant_dbname_tbname.to_string(),
                    name: table_id_list_key.table_name.clone(),
                    meta: tb_meta,
                };

                tb_info_list.push(Arc::new(tb_info));
            }
        }

        return Ok(tb_info_list);
    }

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence

        let (db_id_seq, db_id) = get_id_value(self, tenant_dbname).await?;
        tracing::debug!(
            db_id_seq,
            db_id,
            ?tenant_dbname,
            "get database for listing table"
        );

        db_has_to_exist(db_id_seq, tenant_dbname, "list_tables")?;

        // List tables by tenant, db_id, table_name.

        let dbid_tbname = DBIdTableName {
            db_id,
            // Use empty name to scan all tables
            table_name: "".to_string(),
        };

        let (dbid_tbnames, ids) = list_id_value(self, &dbid_tbname).await?;

        let mut tb_meta_keys = Vec::with_capacity(ids.len());
        for (i, _name_key) in dbid_tbnames.iter().enumerate() {
            let tbid = TableId { table_id: ids[i] };

            tb_meta_keys.push(tbid.to_key());
        }

        // mget() corresponding table_metas

        let seq_tb_metas = self.mget_kv(&tb_meta_keys).await?;

        let mut tb_infos = Vec::with_capacity(ids.len());

        for (i, seq_meta_opt) in seq_tb_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let tb_meta: TableMeta =
                    deserialize_struct(&seq_meta.data).map_err(meta_encode_err)?;

                let tb_info = TableInfo {
                    ident: TableIdent {
                        table_id: ids[i],
                        seq: seq_meta.seq,
                    },
                    desc: format!(
                        "'{}'.'{}'",
                        tenant_dbname.db_name, dbid_tbnames[i].table_name
                    ),
                    meta: tb_meta,
                    name: dbid_tbnames[i].table_name.clone(),
                };
                tb_infos.push(Arc::new(tb_info));
            } else {
                tracing::debug!(
                    k = display(&tb_meta_keys[i]),
                    "db_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(tb_infos)
    }

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), MetaError> {
        let tbid = TableId { table_id };

        let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
            get_struct_value(self, &tbid).await?;

        tracing::debug!(ident = display(&tbid), "get_table_by_id");

        if tb_meta_seq == 0 || table_meta.is_none() {
            return Err(MetaError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(table_id, "get_table_by_id"),
            )));
        }

        Ok((
            TableIdent::new(table_id, tb_meta_seq),
            Arc::new(table_meta.unwrap()),
        ))
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, MetaError> {
        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        loop {
            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_struct_value(self, &tbid).await?;

            tracing::debug!(ident = display(&tbid), "upsert_table_option");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(MetaError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "upsert_table_option"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                return Err(MetaError::AppError(AppError::from(
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
                        opts.insert(k.to_string(), v.to_string());
                    }
                }
            }
            let txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq)?,
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&table_meta)?)?, // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            tracing::debug!(
                id = debug(&tbid),
                succ = display(succ),
                "upsert_table_option"
            );

            if succ {
                return Ok(UpsertTableOptionReply {});
            }
        }
    }

    async fn update_table_meta(
        &self,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply, MetaError> {
        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        loop {
            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_struct_value(self, &tbid).await?;

            tracing::debug!(ident = display(&tbid), "update_table_meta");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(MetaError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "update_table_meta"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                return Err(MetaError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        tb_meta_seq,
                        "update_table_meta",
                    ),
                )));
            }

            let txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq)?,
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&req.new_table_meta)?)?, // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            tracing::debug!(id = debug(&tbid), succ = display(succ), "update_table_meta");

            if succ {
                return Ok(UpdateTableMetaReply {});
            }
        }
    }

    fn name(&self) -> String {
        "SchemaApiImpl".to_string()
    }
}

// Return true if table is out of `DATA_RETENTION_TIME_IN_DAYS option,
// use DEFAULT_DATA_RETENTION_SECONDS by default.
fn is_table_out_of_retention_time(table_meta: &TableMeta, now: &DateTime<Utc>) -> bool {
    if let Some(drop_on) = table_meta.drop_on {
        return now.timestamp() - drop_on.timestamp() > DEFAULT_DATA_RETENTION_SECONDS;
    }

    false
}

// Return true if db is out of `DATA_RETENTION_TIME_IN_DAYS option,
// use DEFAULT_DATA_RETENTION_SECONDS by default.
fn is_db_out_of_retention_time(db_meta: &DatabaseMeta, now: &DateTime<Utc>) -> bool {
    if let Some(drop_on) = db_meta.drop_on {
        return now.timestamp() - drop_on.timestamp() > DEFAULT_DATA_RETENTION_SECONDS;
    }

    false
}

/// Returns (db_id_seq, db_id, db_meta_seq, db_meta)
async fn get_db_or_err(
    kv_api: &impl KVApi,
    name_key: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, DatabaseMeta), MetaError> {
    let (db_id_seq, db_id) = get_id_value(kv_api, name_key).await?;
    db_has_to_exist(db_id_seq, name_key, &msg)?;

    let id_key = DatabaseId { db_id };

    let (db_meta_seq, db_meta) = get_struct_value(kv_api, &id_key).await?;
    db_has_to_exist(db_meta_seq, name_key, msg)?;

    Ok((
        db_id_seq,
        db_id,
        db_meta_seq,
        // Safe unwrap(): db_meta_seq > 0 implies db_meta is not None.
        db_meta.unwrap(),
    ))
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatabase error
fn db_has_to_exist(
    seq: u64,
    db_name_ident: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(), MetaError> {
    if seq == 0 {
        tracing::debug!(seq, ?db_name_ident, "db does not exist");

        Err(MetaError::AppError(AppError::UnknownDatabase(
            UnknownDatabase::new(
                &db_name_ident.db_name,
                format!("{}: {}", msg, db_name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a table_id or table_meta exists by checking the seq.
///
/// Otherwise returns UnknownTable error
fn table_has_to_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), MetaError> {
    if seq == 0 {
        tracing::debug!(seq, ?name_ident, "does not exist");

        Err(MetaError::AppError(AppError::UnknownTable(
            UnknownTable::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    } else {
        Ok(())
    }
}

/// Return OK if a db_id or db_meta does not exist by checking the seq.
///
/// Otherwise returns DatabaseAlreadyExists error
fn db_has_to_not_exist(
    seq: u64,
    name_ident: &DatabaseNameIdent,
    ctx: impl Display,
) -> Result<(), MetaError> {
    if seq == 0 {
        Ok(())
    } else {
        tracing::debug!(seq, ?name_ident, "exist");

        Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
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
) -> Result<(), MetaError> {
    if seq == 0 {
        Ok(())
    } else {
        tracing::debug!(seq, ?name_ident, "exist");

        Err(MetaError::AppError(AppError::TableAlreadyExists(
            TableAlreadyExists::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    }
}

/// Get value that is formatted as it is an `id`, i.e., `u64`.
///
/// It expects the kv-value is an id, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`, or
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`.
///
/// It returns (seq, xx_id).
/// If not found, (0,0) is returned.
async fn get_id_value<T: KVApiKey>(
    kv_api: &impl KVApi,
    name_ident: &T,
) -> Result<(u64, u64), MetaError> {
    let res = kv_api.get_kv(&name_ident.to_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, deserialize_id(&seq_v.data)?))
    } else {
        Ok((0, 0))
    }
}

/// List kvs whose value is formatted as it is an `id`, i.e., `u64`.
///
/// It expects the kv-value is an id, such as:
/// `__fd_table/<db_id>/<table_name> -> (seq, table_id)`, or
/// `__fd_database/<tenant>/<db_name> -> (seq, db_id)`.
///
/// It returns a vec of structured key(such as DatabaseNameIdent) and a vec of id.
async fn list_id_value<K: KVApiKey>(
    kv_api: &impl KVApi,
    key: &K,
) -> Result<(Vec<K>, Vec<u64>), MetaError> {
    let res = kv_api.prefix_list_kv(&key.to_key()).await?;

    let n = res.len();

    let mut structured_keys = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);

    for (str_key, seq_id) in res.iter() {
        let id = deserialize_id(&seq_id.data).map_err(meta_encode_err)?;
        ids.push(id);

        // Parse key and get db_name:

        let struct_key = K::from_key(str_key).map_err(meta_encode_err)?;
        structured_keys.push(struct_key);
    }

    Ok((structured_keys, ids))
}

/// It returns a vec of structured key(such as DatabaseNameIdent), such as:
/// all the `db_name` with prefix `__fd_database/<tenant>/`.
async fn list_keys<K: KVApiKey>(kv_api: &impl KVApi, key: &K) -> Result<Vec<K>, MetaError> {
    let res = kv_api.prefix_list_kv(&key.to_key()).await?;

    let n = res.len();

    let mut structured_keys = Vec::with_capacity(n);

    for (str_key, _seq_id) in res.iter() {
        let struct_key = K::from_key(str_key).map_err(meta_encode_err)?;
        structured_keys.push(struct_key);
    }

    Ok(structured_keys)
}

/// Get a struct value.
///
/// It returns seq number and the data.
async fn get_struct_value<K, PB, T>(
    kv_api: &impl KVApi,
    k: &K,
) -> Result<(u64, Option<T>), MetaError>
where
    K: KVApiKey,
    PB: common_protos::prost::Message + Default,
    T: FromToProto<PB>,
{
    let res = kv_api.get_kv(&k.to_key()).await?;

    if let Some(seq_v) = res {
        Ok((seq_v.seq, Some(deserialize_struct(&seq_v.data)?)))
    } else {
        Ok((0, None))
    }
}

/// Generate an id on metasrv.
///
/// Ids are categorized by generators.
/// Ids may not be consecutive.
async fn fetch_id<T: KVApiKey>(kv_api: &impl KVApi, generator: T) -> Result<u64, MetaError> {
    let res = kv_api
        .upsert_kv(UpsertKVReq {
            key: generator.to_key(),
            seq: MatchSeq::Any,
            value: Operation::Update(b"".to_vec()),
            value_meta: None,
        })
        .await?;

    // seq: MatchSeq::Any always succeeds
    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

/// Build a TxnCondition that compares the seq of a record.
fn txn_cond_seq(
    key: &impl KVApiKey,
    op: ConditionResult,
    seq: u64,
) -> Result<TxnCondition, MetaError> {
    let cond = TxnCondition {
        key: key.to_key(),
        expected: op as i32,
        target: Some(Target::Seq(seq)),
    };
    Ok(cond)
}

/// Build a txn operation that puts a record.
fn txn_op_put(key: &impl KVApiKey, value: Vec<u8>) -> Result<TxnOp, MetaError> {
    let put = TxnOp {
        request: Some(Request::Put(TxnPutRequest {
            key: key.to_key(),
            value,
            prev_value: true,
        })),
    };
    Ok(put)
}

/// Build a txn operation that deletes a record.
fn txn_op_del(key: &impl KVApiKey) -> Result<TxnOp, MetaError> {
    let put = TxnOp {
        request: Some(Request::Delete(TxnDeleteRequest {
            key: key.to_key(),
            prev_value: true,
        })),
    };
    Ok(put)
}

async fn send_txn(
    kv_api: &impl KVApi,
    txn_req: TxnRequest,
) -> Result<(bool, Vec<TxnOpResponse>), MetaError> {
    let tx_reply = kv_api.transaction(txn_req).await?;
    let res: Result<_, MetaError> = tx_reply.into();
    let (succ, responses) = res?;
    Ok((succ, responses))
}

fn serialize_id(id: u64) -> Result<Vec<u8>, MetaError> {
    let v = serde_json::to_vec(&id).map_err(meta_encode_err)?;
    Ok(v)
}

fn deserialize_id(v: &[u8]) -> Result<u64, MetaError> {
    let id = serde_json::from_slice(v).map_err(meta_encode_err)?;
    Ok(id)
}

fn serialize_struct<PB: common_protos::prost::Message>(
    value: &impl FromToProto<PB>,
) -> Result<Vec<u8>, MetaError> {
    let p = value.to_pb().map_err(meta_encode_err)?;
    let mut buf = vec![];
    common_protos::prost::Message::encode(&p, &mut buf).map_err(meta_encode_err)?;
    Ok(buf)
}

fn deserialize_struct<PB, T>(buf: &[u8]) -> Result<T, MetaError>
where
    PB: common_protos::prost::Message + Default,
    T: FromToProto<PB>,
{
    let p: PB = common_protos::prost::Message::decode(buf).map_err(meta_encode_err)?;
    let v: T = FromToProto::from_pb(p).map_err(meta_encode_err)?;

    Ok(v)
}

fn meta_encode_err<E: std::error::Error + 'static>(e: E) -> MetaError {
    MetaError::EncodeError(AnyError::new(&e))
}
