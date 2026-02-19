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

use chrono::Utc;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::CreateDatabaseWithDropTime;
use databend_common_meta_app::app_error::DatabaseAlreadyExists;
use databend_common_meta_app::app_error::DatabaseVersionMismatched;
use databend_common_meta_app::app_error::UndropDbHasNoHistory;
use databend_common_meta_app::app_error::UndropDbWithNoDropTime;
use databend_common_meta_app::app_error::UnknownDatabase;
use databend_common_meta_app::app_error::UnknownDatabaseId;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UpdateDatabaseOptionsReply;
use databend_common_meta_app::schema::UpdateDatabaseOptionsReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult::Eq;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use databend_meta_types::MetaId;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;
use log::error;
use log::warn;
use seq_marked::SeqValue;

use super::data_retention_util::is_drop_time_retainable;
use super::database_util::drop_database_meta;
use super::database_util::get_db_or_err;
use crate::db_has_to_exist;
use crate::error_util::db_has_to_not_exist;
use crate::fetch_id;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::UpsertPB;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_del;
use crate::txn_put_pb;
use crate::txn_put_u64;

impl<KV> DatabaseApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}

/// DatabaseApi defines APIs for database lifecycle and metadata management.
///
/// This trait handles:
/// - Database creation, deletion, and restoration
/// - Database metadata queries and listing
/// - Database name resolution by ID
#[async_trait::async_trait]
pub trait DatabaseApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    // TODO: Move the following method signatures and implementations from SchemaApi:

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        if req.meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateDatabaseWithDropTime(
                CreateDatabaseWithDropTime::new(name_key.database_name()),
            )));
        }

        let mut trials = txn_backoff(None, func_name!());

        loop {
            trials.next().unwrap()?.await;

            // Get db by name to ensure absence
            let curr_seq_db_id = self.get_pb(name_key).await?;
            debug!(curr_seq_db_id :? = curr_seq_db_id, name_key :? =(name_key); "get_database");

            let mut txn = TxnRequest::default();

            if let Some(ref curr_seq_db_id) = curr_seq_db_id {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                            DatabaseAlreadyExists::new(
                                name_key.database_name(),
                                format!("create db: tenant: {}", name_key.tenant_name()),
                            ),
                        )));
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(CreateDatabaseReply {
                            db_id: curr_seq_db_id.data.into_inner(),
                        });
                    }
                    CreateOption::CreateOrReplace => {
                        let _ = drop_database_meta(
                            self,
                            name_key,
                            req.catalog_name.clone(),
                            false,
                            false,
                            &mut txn,
                        )
                        .await?;
                    }
                }
            };

            // get db id list from _fd_db_id_list/db_id
            let dbid_idlist =
                DatabaseIdHistoryIdent::new(name_key.tenant(), name_key.database_name());

            let seq_db_id_list = self.get_pb(&dbid_idlist).await?;
            let db_id_list_seq = seq_db_id_list.seq();
            let mut db_id_list = seq_db_id_list.into_value().unwrap_or_else(DbIdList::new);

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

            debug!(db_id = db_id, name_key :? =(name_key); "new database id");

            {
                // append db_id into db_id_list
                db_id_list.append(db_id);

                txn.condition.extend(vec![
                    txn_cond_seq(name_key, Eq, curr_seq_db_id.seq()),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                    txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                ]);
                txn.if_then.extend(vec![
                    txn_put_u64(name_key, db_id)?, // (tenant, db_name) -> db_id
                    txn_put_pb(&id_key, &req.meta)?, // (db_id) -> db_meta
                    txn_put_pb(&dbid_idlist, &db_id_list)?, /* _fd_db_id_list/<tenant>/<db_name> -> db_id_list */
                    txn_put_pb(&id_to_name_key, &DatabaseNameIdentRaw::from(name_key))?, /* __fd_database_id_to_name/<db_id> -> (tenant,db_name) */
                ]);

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(name_key),
                    id :? =(&id_key),
                    succ = succ;
                    "create_database"
                );

                if succ {
                    return Ok(CreateDatabaseReply { db_id: id_key });
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let db_id =
                drop_database_meta(self, tenant_dbname, None, req.if_exists, true, &mut txn)
                    .await?;

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                name :? =(tenant_dbname),
                succ = succ;
                "drop_database"
            );

            if succ {
                return Ok(DropDatabaseReply { db_id });
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> Result<UndropDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_db_or_err(
                self,
                name_key,
                format!("undrop_database: {}", name_key.display()),
            )
            .await;

            if res.is_ok() {
                return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                    DatabaseAlreadyExists::new(
                        name_key.database_name(),
                        format!(
                            "undrop_database: {} has already existed",
                            name_key.database_name()
                        ),
                    ),
                )));
            }

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist =
                DatabaseIdHistoryIdent::new(name_key.tenant(), name_key.database_name());
            let seq_db_id_list = self.get_pb(&dbid_idlist).await?;

            let Some(seq_db_id_list) = seq_db_id_list else {
                return Err(KVAppError::AppError(AppError::UndropDbHasNoHistory(
                    UndropDbHasNoHistory::new(name_key.database_name()),
                )));
            };

            let db_id_list_seq = seq_db_id_list.seq;
            let db_id_list = seq_db_id_list.data;

            // Return error if there is no db id history.
            let db_id = *db_id_list.last().ok_or_else(|| {
                KVAppError::AppError(AppError::UndropDbHasNoHistory(UndropDbHasNoHistory::new(
                    name_key.database_name(),
                )))
            })?;

            // get db_meta of the last db id
            let dbid = DatabaseId { db_id };

            let seq_meta = self.get_pb(&dbid).await?;
            let db_meta_seq = seq_meta.seq();
            let mut db_meta = seq_meta.into_value().unwrap();

            debug!(db_id = db_id, name_key :? =(name_key); "undrop_database");

            // Ensure that db is not being GC.
            //
            // And since later inside the undrop database kv txn, the sequence number of db_meta will
            // also be checked, it can be ensured that db_meta.gc_in_progress will not transit from
            // TRUE to FALSE in this kv txn.
            if db_meta.gc_in_progress {
                let err = UnknownDatabase::new(
                    name_key.database_name(),
                    "undropping database (gc in progress)".to_owned(),
                );
                return Err(KVAppError::AppError(AppError::from(err)));
            }

            {
                // reset drop on time
                // undrop a table with no drop time
                if db_meta.drop_on.is_none() {
                    return Err(KVAppError::AppError(AppError::UndropDbWithNoDropTime(
                        UndropDbWithNoDropTime::new(name_key.database_name()),
                    )));
                }
                db_meta.drop_on = None;

                let txn_req = TxnRequest::new(
                    vec![
                        txn_cond_seq(name_key, Eq, 0),
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                        txn_cond_seq(&dbid, Eq, db_meta_seq),
                    ],
                    vec![
                        txn_put_u64(name_key, db_id)?, // (tenant, db_name) -> db_id
                        txn_put_pb(&dbid, &db_meta)?,  // (db_id) -> db_meta
                    ],
                );

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name_key :? =(name_key),
                    succ = succ;
                    "undrop_database"
                );

                if succ {
                    return Ok(UndropDatabaseReply {});
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.name_ident;
        let tenant_newdbname = DatabaseNameIdent::new(tenant_dbname.tenant(), &req.new_db_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // get old db, not exists return err
            let old_seq_db_id = self.get_pb(tenant_dbname).await?;

            let Some(old_seq_db_id) = old_seq_db_id else {
                if req.if_exists {
                    return Ok(RenameDatabaseReply {});
                } else {
                    db_has_to_exist(
                        old_seq_db_id.seq(),
                        tenant_dbname,
                        "rename_database: src (db)",
                    )?;
                    unreachable!("rename_database: src (db) should exist")
                }
            };

            let old_db_id = old_seq_db_id.data.into_inner();

            let old_seq_db_meta = self.get_pb(&old_db_id).await?;

            db_has_to_exist(
                old_seq_db_meta.seq(),
                tenant_dbname,
                "rename_database: src (db)",
            )?;

            debug!(
                old_db_id :? = old_db_id,
                tenant_dbname :? =(tenant_dbname);
                "rename_database"
            );

            // get new db, exists return err
            let new_seq_db_id = self.get_pb(&tenant_newdbname).await?;
            db_has_to_not_exist(new_seq_db_id.seq(), &tenant_newdbname, "rename_database")?;

            // get db id -> name
            let db_id_key = DatabaseIdToName { db_id: *old_db_id };
            let seq_db_name = self.get_pb(&db_id_key).await?;
            let db_name_seq = seq_db_name.seq();

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist =
                DatabaseIdHistoryIdent::new(tenant_dbname.tenant(), tenant_dbname.database_name());

            let seq_db_id_list = self.get_pb(&dbid_idlist).await?;
            let db_id_list_seq = seq_db_id_list.seq();

            // may the database is created before add db_id_list, so we just add the id into the list.
            let mut db_id_list = seq_db_id_list.into_value().unwrap_or_else(|| {
                let mut l = DbIdList::new();
                l.append(*old_db_id);
                l
            });

            if let Some(last_db_id) = db_id_list.last() {
                if *last_db_id != *old_db_id {
                    return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            tenant_dbname.database_name(),
                            format!(
                                "rename_database: {} with a wrong db id",
                                tenant_dbname.display()
                            ),
                        ),
                    )));
                }
            } else {
                return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                    DatabaseAlreadyExists::new(
                        tenant_dbname.database_name(),
                        format!(
                            "rename_database: {} with none db id history",
                            tenant_dbname.display()
                        ),
                    ),
                )));
            }

            let new_dbid_idlist =
                DatabaseIdHistoryIdent::new(tenant_dbname.tenant(), &req.new_db_name);

            let seq_idlist = self.get_pb(&new_dbid_idlist).await?;

            let new_db_id_list_seq = seq_idlist.seq();
            let mut new_db_id_list = seq_idlist.into_value().unwrap_or_else(DbIdList::new);

            // rename database
            // move db id from old db id list to new db id list
            db_id_list.pop();
            new_db_id_list.append(*old_db_id);

            let condition = vec![
                // Prevent renaming or deleting in other threads.
                txn_cond_seq(tenant_dbname, Eq, old_seq_db_id.seq),
                txn_cond_seq(&db_id_key, Eq, db_name_seq),
                txn_cond_seq(&tenant_newdbname, Eq, 0),
                txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                txn_cond_seq(&new_dbid_idlist, Eq, new_db_id_list_seq),
            ];
            let if_then = vec![
                txn_del(tenant_dbname), // del old_db_name
                // Renaming db should not affect the seq of db_meta. Just modify db name.
                txn_put_u64(&tenant_newdbname, *old_db_id)?, /* (tenant, new_db_name) -> old_db_id */
                txn_put_pb(&new_dbid_idlist, &new_db_id_list)?, /* _fd_db_id_list/tenant/new_db_name -> new_db_id_list */
                txn_put_pb(&dbid_idlist, &db_id_list)?, /* _fd_db_id_list/tenant/db_name -> db_id_list */
                txn_put_pb(&db_id_key, &DatabaseNameIdentRaw::from(&tenant_newdbname))?, /* __fd_database_id_to_name/<db_id> -> (tenant,db_name) */
            ];

            let txn_req = TxnRequest::new(condition, if_then);

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name :? =(tenant_dbname),
                to :? =(&tenant_newdbname),
                database_id :? =(&old_db_id),
                succ = succ;
                "rename_database"
            );

            if succ {
                return Ok(RenameDatabaseReply {});
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn update_database_options(
        &self,
        req: UpdateDatabaseOptionsReq,
    ) -> Result<UpdateDatabaseOptionsReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let db_id = req.db_id;
        let expected_seq = req.expected_meta_seq;
        let new_options = req.options.clone();
        let db_key = DatabaseId::new(db_id);

        let seq_meta = self.get_pb(&db_key).await?;
        let Some(seq_meta) = seq_meta else {
            return Err(KVAppError::AppError(AppError::UnknownDatabaseId(
                UnknownDatabaseId::new(db_id, "update_database_options"),
            )));
        };

        if seq_meta.seq != expected_seq {
            return Err(KVAppError::AppError(AppError::DatabaseVersionMismatched(
                DatabaseVersionMismatched::new(
                    db_id,
                    MatchSeq::Exact(expected_seq),
                    Some(seq_meta.seq),
                    "update_database_options",
                ),
            )));
        }

        let mut meta = seq_meta.data;
        meta.options = new_options;
        meta.updated_on = Utc::now();

        let upsert = UpsertPB::update_exact(db_key, SeqV::new(expected_seq, meta));
        let transition = self.upsert_pb(&upsert).await?;

        if !transition.is_changed() {
            let curr_seq = transition.prev.map(|seq_v| seq_v.seq);

            return Err(KVAppError::AppError(AppError::DatabaseVersionMismatched(
                DatabaseVersionMismatched::new(
                    db_id,
                    MatchSeq::Exact(expected_seq),
                    curr_seq,
                    "update_database_options",
                ),
            )));
        }

        Ok(UpdateDatabaseOptionsReply {})
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.inner;

        let (seq_db_id, db_meta) = get_db_or_err(self, name_key, "get_database").await?;

        let db = DatabaseInfo {
            database_id: seq_db_id.data,
            name_ident: name_key.clone(),
            meta: db_meta,
        };

        Ok(Arc::new(db))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_tenant_history_databases(
        &self,
        req: ListDatabaseReq,
        include_non_retainable: bool,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_ident = DatabaseIdHistoryIdent::new(&req.tenant, "dummy");
        let dir_name = DirName::new(name_ident);

        let name_idlists = self.list_pb_vec(ListOptions::unlimited(&dir_name)).await?;

        let mut dbs = BTreeMap::new();

        for (db_id_list_key, db_id_list) in name_idlists {
            let ids = db_id_list
                .id_list
                .iter()
                .map(|db_id| DatabaseId { db_id: *db_id });

            let id_metas = self.get_pb_vec(ids).await?;

            for (db_id, db_meta) in id_metas {
                let Some(db_meta) = db_meta else {
                    error!("get_database_history cannot find {:?} db_meta", db_id);
                    continue;
                };

                let db = DatabaseInfo {
                    database_id: db_id,
                    name_ident: DatabaseNameIdent::new_from(db_id_list_key.clone()),
                    meta: db_meta,
                };
                dbs.insert(db_id.db_id, Arc::new(db));
            }
        }

        // Find out dbs that are not included in any DbIdListKey.
        // Because the DbIdListKey function is added after the first release of the system.
        // There may be dbs do not have a corresponding DbIdListKey.

        let list_dbs = self.list_databases(req.clone()).await?;
        for db_info in list_dbs {
            dbs.entry(db_info.database_id.db_id).or_insert_with(|| {
                warn!(
                    "get db history db:{:?}, db_id:{:?} has no DbIdListKey",
                    db_info.name_ident, db_info.database_id.db_id
                );

                db_info
            });
        }

        let now = Utc::now();

        let dbs = dbs
            .into_values()
            .filter(|x| include_non_retainable || is_drop_time_retainable(x.meta.drop_on, now))
            .collect();

        return Ok(dbs);
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = DatabaseNameIdent::new(req.tenant(), "dummy");
        let dir = DirName::new(name_key);

        let name_seq_ids = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;

        let id_idents = name_seq_ids
            .iter()
            .map(|(_k, id)| {
                let db_id = id.data;
                DatabaseId { db_id: *db_id }
            })
            .collect::<Vec<_>>();

        let id_metas = self.get_pb_values_vec(id_idents).await?;

        let name_id_metas = name_seq_ids
            .into_iter()
            .zip(id_metas.into_iter())
            // Remove values that are not found, may be just removed.
            .filter_map(|((name, seq_id), opt_seq_meta)| {
                opt_seq_meta.map(|seq_meta| (name, seq_id.data, seq_meta))
            })
            .map(|(name, db_id, seq_meta)| {
                let db_info = DatabaseInfo {
                    database_id: db_id.into_inner(),
                    name_ident: name,
                    meta: seq_meta,
                };
                Arc::new(db_info)
            })
            .collect::<Vec<_>>();

        Ok(name_id_metas)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_db_name_by_id(&self, db_id: u64) -> Result<String, KVAppError> {
        debug!(req :? =(&db_id); "SchemaApi: {}", func_name!());

        let db_id_to_name_key = DatabaseIdToName { db_id };

        let seq_meta = self.get_pb(&db_id_to_name_key).await?;

        debug!(ident :% =(&db_id_to_name_key); "get_db_name_by_id");

        let Some(seq_meta) = seq_meta else {
            return Err(KVAppError::AppError(AppError::UnknownDatabaseId(
                UnknownDatabaseId::new(db_id, "get_db_name_by_id"),
            )));
        };

        Ok(seq_meta.data.database_name().to_string())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_database_names_by_ids(
        &self,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>, KVAppError> {
        debug!(req :? =(&db_ids); "SchemaApi: {}", func_name!());

        let id_to_name_keys = db_ids.iter().map(|id| DatabaseIdToName { db_id: *id });

        let seq_names = self.get_pb_values_vec(id_to_name_keys).await?;

        let mut db_names = seq_names
            .into_iter()
            .map(|seq_name| seq_name.map(|s| s.data.database_name().to_string()))
            .collect::<Vec<_>>();

        let id_keys = db_ids.iter().map(|id| DatabaseId { db_id: *id });

        let seq_metas = self.get_pb_values_vec(id_keys).await?;

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                if seq_meta.data.drop_on.is_some() {
                    db_names[i] = None;
                }
            } else {
                db_names[i] = None;
            }
        }
        Ok(db_names)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_database_id_or_err(
        &self,
        name_key: &DatabaseNameIdent,
        msg: impl Display + std::fmt::Debug + Send,
    ) -> Result<Result<SeqV<DatabaseId>, UnknownDatabase>, MetaError> {
        let seq_db_id = self.get_pb(name_key).await?;
        let result = seq_db_id.map(|s| s.map(|x| x.into_inner())).ok_or_else(|| {
            UnknownDatabase::new(
                name_key.database_name(),
                format!("{}: {}", msg, name_key.display()),
            )
        });
        Ok(result)
    }
}
