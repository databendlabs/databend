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
use common_meta_api::KVApi;
use common_meta_api::MetaApi;
use common_meta_types::txn_condition;
use common_meta_types::txn_op::Request;
use common_meta_types::AppError;
use common_meta_types::ConditionResult;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateShareReply;
use common_meta_types::CreateShareReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseAlreadyExists;
use common_meta_types::DatabaseIdent;
use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;
use common_meta_types::DatabaseNameIdent;
use common_meta_types::DatabaseTenantIdIdent;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropShareReply;
use common_meta_types::DropShareReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetShareReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;
use common_meta_types::MetaId;
use common_meta_types::Operation;
use common_meta_types::RenameTableReply;
use common_meta_types::RenameTableReq;
use common_meta_types::ShareInfo;
use common_meta_types::TableAlreadyExists;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::TableNameIdent;
use common_meta_types::TenantDBIdTableId;
use common_meta_types::TenantDBIdTableName;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnRequest;
use common_meta_types::UnknownDatabase;
use common_meta_types::UnknownTable;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_proto_conv::FromToProto;
use common_tracing::tracing;
use txn_condition::Target;
use ConditionResult::Eq;

use crate::meta_client_on_kv::DatabaseIdGen;
use crate::meta_client_on_kv::KVApiKey;
use crate::meta_client_on_kv::TableIdGen;
use crate::MetaClientOnKV;

#[tonic::async_trait]
impl MetaApi for MetaClientOnKV {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, MetaError> {
        let name_key = &req.name_ident;

        let mut id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id: 0,
        };

        loop {
            // Get db by name to ensure absence
            let (db_id_seq, db_id) = self.get_db_id_by_name(name_key).await?;
            tracing::debug!(db_id_seq, db_id, ?name_key, "get db");

            if db_id_seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateDatabaseReply { database_id: db_id })
                } else {
                    Err(MetaError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            &name_key.db_name,
                            format!("create db: tenant: {}", name_key.tenant),
                        ),
                    )))
                };
            }

            let db_id = self.fetch_id(DatabaseIdGen {}).await?;
            id_key.db_id = db_id;

            tracing::debug!(db_id, name_key = debug(&name_key), "new database id");

            {
                let txn_req = TxnRequest {
                    condition: vec![self.txn_cond_seq(name_key, Eq, 0)?],
                    if_then: vec![
                        self.txn_op_put(name_key, self.serialize_id(db_id)?)?, // (tenant, db_name) -> db_id
                        self.txn_op_put(&id_key, self.serialize_struct(&req.meta)?)?, // (tenant, db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = self.send_txn(txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "create_database"
                );

                if succ {
                    return Ok(CreateDatabaseReply { database_id: db_id });
                }
            }
        }
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, MetaError> {
        let name_key = &req.name_ident;

        let mut id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id: 0,
        };

        loop {
            let res = self
                .get_db_or_err(name_key, format!("drop_database: {}", &name_key))
                .await;
            let (db_id_seq, db_id, db_meta_seq, _db_meta) = match res {
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

            id_key.db_id = db_id;

            tracing::debug!(db_id, name_key = debug(&name_key), "drop_database");

            {
                let txn_req = TxnRequest {
                    condition: vec![
                        self.txn_cond_seq(name_key, Eq, db_id_seq)?,
                        self.txn_cond_seq(&id_key, Eq, db_meta_seq)?,
                    ],
                    if_then: vec![
                        self.txn_op_del(name_key)?, // (tenant, db_name) -> db_id
                        self.txn_op_del(&id_key)?,  // (tenant, db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = self.send_txn(txn_req).await?;

                tracing::debug!(
                    name = debug(&name_key),
                    id = debug(&id_key),
                    succ = display(succ),
                    "drop_database"
                );

                if succ {
                    return Ok(DropDatabaseReply {});
                }
            }
        }
    }

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, MetaError> {
        let name_key = &req.inner;

        let mut id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id: 0,
        };

        // Get db id by name
        let (db_id_seq, db_id) = self.get_db_id_by_name(name_key).await?;
        self.db_has_to_exist(db_id_seq, name_key, "get_database")?;

        id_key.db_id = db_id;

        // Get db_meta by id
        let (db_meta_seq, db_meta) = self.get_db_by_id(&id_key).await?;
        self.db_has_to_exist(
            db_meta_seq,
            name_key,
            format_args!("get_database by_id: {}", id_key),
        )?;

        let db = DatabaseInfo {
            ident: DatabaseIdent {
                db_id,
                seq: db_meta_seq,
            },
            name_ident: name_key.clone(),
            meta: db_meta.unwrap(),
        };

        Ok(Arc::new(db))
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

        let prefix = name_key.to_key();
        // Pairs of db-name and db_id with seq
        let key_seq_ids = self.inner.prefix_list_kv(&prefix).await?;

        let n = key_seq_ids.len();

        // Keys for fetching serialized DatabaseMeta from KVApi
        let mut kv_keys = Vec::with_capacity(n);
        let mut db_names = Vec::with_capacity(n);
        let mut db_ids = Vec::with_capacity(n);

        for (db_name_key, seq_id) in key_seq_ids.iter() {
            let db_id = self
                .deserialize_id(&seq_id.data)
                .map_err(Self::meta_encode_err)?;
            db_ids.push(db_id);

            // Parse key and get db_name:

            let n = DatabaseNameIdent::from_key(db_name_key).map_err(Self::meta_encode_err)?;
            db_names.push(n.db_name);

            // Build KVApi key for `mget()` db_meta
            let tenant_id_key = DatabaseTenantIdIdent {
                tenant: name_key.tenant.clone(),
                db_id,
            };

            let k = tenant_id_key.to_key();
            kv_keys.push(k);
        }

        // Batch get all db-metas.
        // - A db-meta may be already deleted. It is Ok. Just ignore it.

        let seq_metas = self.inner.mget_kv(&kv_keys).await?;
        let mut db_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let db_meta: DatabaseMeta = self
                    .deserialize_struct(&seq_meta.data)
                    .map_err(Self::meta_encode_err)?;

                let db_info = DatabaseInfo {
                    ident: DatabaseIdent {
                        db_id: db_ids[i],
                        seq: seq_meta.seq,
                    },
                    name_ident: DatabaseNameIdent {
                        tenant: name_key.tenant.clone(),
                        db_name: db_names[i].clone(),
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

        loop {
            // Get db by name to ensure presence

            let (db_id_seq, db_id) = self.get_id_value(&tenant_dbname).await?;
            tracing::debug!(db_id_seq, db_id, ?tenant_dbname_tbname, "get database");

            self.db_has_to_exist(
                db_id_seq,
                &tenant_dbname,
                format!("create_table: {}", tenant_dbname_tbname),
            )?;

            // Get table by tenant,db_id, table_name to assert absence.

            let tenant_dbid_tbname = TenantDBIdTableName {
                tenant: tenant_dbname.tenant.clone(),
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            let (tb_id_seq, tb_id) = self.get_id_value(&tenant_dbid_tbname).await?;
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

            // Create table by inserting two record:
            // (tenant, db_id, table_name) -> table_id
            // (tenant, db_id, table_id) -> table_meta

            let table_id = self.fetch_id(TableIdGen {}).await?;

            let tenant_dbid_tbid = TenantDBIdTableId {
                tenant: tenant_dbname_tbname.tenant.clone(),
                db_id,
                table_id,
            };

            tracing::debug!(
                table_id,
                name = debug(&tenant_dbname_tbname),
                "new table id"
            );

            {
                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change
                        self.txn_cond_seq(&tenant_dbname, Eq, db_id_seq)?,
                        // no other table with the same name is inserted.
                        self.txn_cond_seq(&tenant_dbid_tbname, Eq, 0)?,
                    ],
                    if_then: vec![
                        self.txn_op_put(&tenant_dbid_tbname, self.serialize_id(table_id)?)?, // (tenant, db_id, tb_name) -> tb_id
                        self.txn_op_put(
                            &tenant_dbid_tbid,
                            self.serialize_struct(&req.table_meta)?,
                        )?, // (tenant, db_id, tb_id) -> tb_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = self.send_txn(txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname_tbname),
                    id = debug(&tenant_dbid_tbid),
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

            let (db_id_seq, db_id) = self.get_id_value(&tenant_dbname).await?;
            tracing::debug!(db_id_seq, db_id, ?tenant_dbname_tbname, "get database");

            self.db_has_to_exist(
                db_id_seq,
                &tenant_dbname,
                format!("create_table: {}", tenant_dbname_tbname),
            )?;

            // Get table by tenant,db_id, table_name to assert presence.

            let tenant_dbid_tbname = TenantDBIdTableName {
                tenant: tenant_dbname.tenant.clone(),
                db_id,
                table_name: req.name_ident.table_name.clone(),
            };

            let (tb_id_seq, table_id) = self.get_id_value(&tenant_dbid_tbname).await?;
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

            let tenant_dbid_tbid = TenantDBIdTableId {
                tenant: tenant_dbname_tbname.tenant.clone(),
                db_id,
                table_id,
            };

            let (tb_meta_seq, _tb_meta): (_, Option<TableMeta>) =
                self.get_struct_value(&tenant_dbid_tbid).await?;

            // Delete table by deleting two record:
            // (tenant, db_id, table_name) -> table_id
            // (tenant, db_id, table_id) -> table_meta

            tracing::debug!(
                ident = display(&tenant_dbid_tbid),
                name = display(&tenant_dbname_tbname),
                "drop table"
            );

            {
                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change
                        self.txn_cond_seq(&tenant_dbname, Eq, db_id_seq)?,
                        // still this table id
                        self.txn_cond_seq(&tenant_dbid_tbname, Eq, tb_id_seq)?,
                        // table is not changed
                        self.txn_cond_seq(&tenant_dbid_tbid, Eq, tb_meta_seq)?,
                    ],
                    if_then: vec![
                        self.txn_op_del(&tenant_dbid_tbname)?, // (tenant, db_id, tb_name) -> tb_id
                        self.txn_op_del(&tenant_dbid_tbid)?,   // (tenant, db_id, tb_id) -> tb_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = self.send_txn(txn_req).await?;

                tracing::debug!(
                    name = debug(&tenant_dbname_tbname),
                    id = debug(&tenant_dbid_tbid),
                    succ = display(succ),
                    "drop_table"
                );

                if succ {
                    return Ok(DropTableReply {});
                }
            }
        }
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply, MetaError> {
        todo!()
    }

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, MetaError> {
        let tenant_dbname_tbname = &req.inner;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();

        // Get db by name to ensure presence

        let (db_id_seq, db_id) = self.get_id_value(&tenant_dbname).await?;
        tracing::debug!(db_id_seq, db_id, ?tenant_dbname_tbname, "get database");

        self.db_has_to_exist(
            db_id_seq,
            &tenant_dbname,
            format!("get_table: {}", tenant_dbname_tbname),
        )?;

        // Get table by tenant,db_id, table_name to assert presence.

        let tenant_dbid_tbname = TenantDBIdTableName {
            tenant: tenant_dbname.tenant.clone(),
            db_id,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };

        let (tb_id_seq, table_id) = self.get_id_value(&tenant_dbid_tbname).await?;
        self.table_has_to_exist(tb_id_seq, tenant_dbname_tbname, "get_table")?;

        let tenant_dbid_tbid = TenantDBIdTableId {
            tenant: tenant_dbname_tbname.tenant.clone(),
            db_id,
            table_id,
        };

        let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) =
            self.get_struct_value(&tenant_dbid_tbid).await?;

        self.table_has_to_exist(
            tb_meta_seq,
            tenant_dbname_tbname,
            format!("get_table meta by: {}", tenant_dbid_tbid),
        )?;

        tracing::debug!(
            ident = display(&tenant_dbid_tbid),
            name = display(&tenant_dbname_tbname),
            "get_table"
        );

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id,
                version: tb_meta_seq,
            },
            desc: tenant_dbname_tbname.to_string(),
            name: tenant_dbname_tbname.table_name.clone(),
            // Safe unwrap() because: tb_meta_seq > 0
            meta: tb_meta.unwrap(),
        };

        return Ok(Arc::new(tb_info));
    }

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, MetaError> {
        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence

        let (db_id_seq, db_id) = self.get_id_value(tenant_dbname).await?;
        tracing::debug!(
            db_id_seq,
            db_id,
            ?tenant_dbname,
            "get database for listing table"
        );

        self.db_has_to_exist(db_id_seq, tenant_dbname, "list_tables")?;

        // List tables by tenant, db_id, table_name.

        let tenant_dbid_tbname = TenantDBIdTableName {
            tenant: tenant_dbname.tenant.clone(),
            db_id,
            // Use empty name to scan all tables
            table_name: "".to_string(),
        };

        let (tenant_dbid_tbnames, ids) = self.list_id_value(&tenant_dbid_tbname).await?;

        let mut tb_meta_keys = Vec::with_capacity(ids.len());
        for (i, name_key) in tenant_dbid_tbnames.iter().enumerate() {
            let tenant_dbid_tbid = TenantDBIdTableId {
                tenant: name_key.tenant.clone(),
                db_id: name_key.db_id,
                table_id: ids[i],
            };

            tb_meta_keys.push(tenant_dbid_tbid.to_key());
        }

        // mget() corresponding table_metas

        let seq_tb_metas = self.inner.mget_kv(&tb_meta_keys).await?;

        let mut tb_infos = Vec::with_capacity(ids.len());

        for (i, seq_meta_opt) in seq_tb_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let tb_meta: TableMeta = self
                    .deserialize_struct(&seq_meta.data)
                    .map_err(Self::meta_encode_err)?;

                let tb_info = TableInfo {
                    ident: TableIdent {
                        table_id: ids[i],
                        version: seq_meta.seq,
                    },
                    desc: format!(
                        "'{}'.'{}'",
                        tenant_dbname.db_name, tenant_dbid_tbnames[i].table_name
                    ),
                    meta: tb_meta,
                    name: tenant_dbid_tbnames[i].to_string(),
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
        _table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), MetaError> {
        todo!()
    }

    async fn upsert_table_option(
        &self,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, MetaError> {
        todo!()
    }

    async fn create_share(&self, _req: CreateShareReq) -> Result<CreateShareReply, MetaError> {
        todo!()
    }

    async fn drop_share(&self, _req: DropShareReq) -> Result<DropShareReply, MetaError> {
        todo!()
    }
    async fn get_share(&self, _req: GetShareReq) -> Result<Arc<ShareInfo>, MetaError> {
        todo!()
    }

    fn name(&self) -> String {
        "MetaClientOnKV".to_string()
    }
}

// Supporting utils
impl MetaClientOnKV {
    /// Returns (db_id_seq, db_id, db_meta_seq, db_meta)
    async fn get_db_or_err(
        &self,
        name_key: &DatabaseNameIdent,
        msg: impl Display,
    ) -> Result<(u64, u64, u64, DatabaseMeta), MetaError> {
        let (db_id_seq, db_id) = self.get_id_value(name_key).await?;
        self.db_has_to_exist(db_id_seq, name_key, &msg)?;

        let id_key = DatabaseTenantIdIdent {
            tenant: name_key.tenant.clone(),
            db_id,
        };

        let (db_meta_seq, db_meta) = self.get_db_by_id(&id_key).await?;
        self.db_has_to_exist(db_meta_seq, name_key, msg)?;

        Ok((db_id_seq, db_id, db_meta_seq, db_meta.unwrap()))
    }

    /// Get db id with seq, by name
    ///
    /// It returns (seq, db_id).
    /// If db is not found, (0,0) is returned.
    async fn get_db_id_by_name(
        &self,
        name_ident: &DatabaseNameIdent,
    ) -> Result<(u64, u64), MetaError> {
        let res = self.inner.get_kv(&name_ident.to_key()).await?;

        if let Some(seq_v) = res {
            Ok((seq_v.seq, self.deserialize_id(&seq_v.data)?))
        } else {
            Ok((0, 0))
        }
    }

    /// Get DatabaseMeta by database id.
    ///
    /// It returns seq number and the data.
    async fn get_db_by_id(
        &self,
        tid: &DatabaseTenantIdIdent,
    ) -> Result<(u64, Option<DatabaseMeta>), MetaError> {
        let res = self.inner.get_kv(&tid.to_key()).await?;

        if let Some(seq_v) = res {
            Ok((seq_v.seq, Some(self.deserialize_struct(&seq_v.data)?)))
        } else {
            Ok((0, None))
        }
    }

    /// Return OK if a db_id or db_meta exists by checking the seq.
    ///
    /// Otherwise returns UnknownDatabase error
    fn db_has_to_exist(
        &self,
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
        &self,
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

    /// Get value that is formatted as it is an `id`, i.e., `u64`.
    ///
    /// It expects the kv-value is an id, such as:
    /// `__fd_table/<tenant>/<db_id>/by_name/<table_name> -> (seq, table_id)`, or
    /// `__fd_database/<tenant>/by_name/<db_name> -> (seq, db_id)`.
    ///
    /// It returns (seq, xx_id).
    /// If not found, (0,0) is returned.
    async fn get_id_value<T: KVApiKey>(&self, name_ident: &T) -> Result<(u64, u64), MetaError> {
        let res = self.inner.get_kv(&name_ident.to_key()).await?;

        if let Some(seq_v) = res {
            Ok((seq_v.seq, self.deserialize_id(&seq_v.data)?))
        } else {
            Ok((0, 0))
        }
    }

    /// List kvs whose value is formatted as it is an `id`, i.e., `u64`.
    ///
    /// It expects the kv-value is an id, such as:
    /// `__fd_table/<tenant>/<db_id>/by_name/<table_name> -> (seq, table_id)`, or
    /// `__fd_database/<tenant>/by_name/<db_name> -> (seq, db_id)`.
    ///
    /// It returns a vec of structured key(such as DatabaseNameIdent) and a vec of id.
    async fn list_id_value<K: KVApiKey>(&self, key: &K) -> Result<(Vec<K>, Vec<u64>), MetaError> {
        let res = self.inner.prefix_list_kv(&key.to_key()).await?;

        let n = res.len();

        let mut structured_keys = Vec::with_capacity(n);
        let mut ids = Vec::with_capacity(n);

        for (str_key, seq_id) in res.iter() {
            let id = self
                .deserialize_id(&seq_id.data)
                .map_err(Self::meta_encode_err)?;
            ids.push(id);

            // Parse key and get db_name:

            let struct_key = K::from_key(str_key).map_err(Self::meta_encode_err)?;
            structured_keys.push(struct_key);
        }

        Ok((structured_keys, ids))
    }

    /// Get a struct value.
    ///
    /// It returns seq number and the data.
    async fn get_struct_value<K, PB, T>(&self, k: &K) -> Result<(u64, Option<T>), MetaError>
    where
        K: KVApiKey,
        PB: common_protos::prost::Message + Default,
        T: FromToProto<PB>,
    {
        let res = self.inner.get_kv(&k.to_key()).await?;

        if let Some(seq_v) = res {
            Ok((seq_v.seq, Some(self.deserialize_struct(&seq_v.data)?)))
        } else {
            Ok((0, None))
        }
    }

    /// Generate an id on metasrv.
    ///
    /// Ids are categorized by generators.
    /// Ids may not be consecutive.
    async fn fetch_id<T: KVApiKey>(&self, generator: T) -> Result<u64, MetaError> {
        let res = self
            .inner
            .upsert_kv(UpsertKVAction {
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
        &self,
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
    fn txn_op_put(&self, key: &impl KVApiKey, value: Vec<u8>) -> Result<TxnOp, MetaError> {
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
    fn txn_op_del(&self, key: &impl KVApiKey) -> Result<TxnOp, MetaError> {
        let put = TxnOp {
            request: Some(Request::Delete(TxnDeleteRequest {
                key: key.to_key(),
                prev_value: true,
            })),
        };
        Ok(put)
    }

    async fn send_txn(&self, txn_req: TxnRequest) -> Result<(bool, Vec<TxnOpResponse>), MetaError> {
        let tx_reply = self.inner.transaction(txn_req).await?;
        let res: Result<_, MetaError> = tx_reply.into();
        let (succ, responses) = res?;
        Ok((succ, responses))
    }

    fn serialize_id(&self, id: u64) -> Result<Vec<u8>, MetaError> {
        let v = serde_json::to_vec(&id).map_err(Self::meta_encode_err)?;
        Ok(v)
    }

    fn deserialize_id(&self, v: &[u8]) -> Result<u64, MetaError> {
        let id = serde_json::from_slice(v).map_err(Self::meta_encode_err)?;
        Ok(id)
    }

    fn serialize_struct<PB: common_protos::prost::Message>(
        &self,
        value: &impl FromToProto<PB>,
    ) -> Result<Vec<u8>, MetaError> {
        let p = value.to_pb().map_err(Self::meta_encode_err)?;
        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf).map_err(Self::meta_encode_err)?;
        Ok(buf)
    }

    fn deserialize_struct<PB, T>(&self, buf: &[u8]) -> Result<T, MetaError>
    where
        PB: common_protos::prost::Message + Default,
        T: FromToProto<PB>,
    {
        let p: PB = common_protos::prost::Message::decode(buf).map_err(Self::meta_encode_err)?;
        let v: T = FromToProto::from_pb(p).map_err(Self::meta_encode_err)?;

        Ok(v)
    }

    fn meta_encode_err<E: std::error::Error + 'static>(e: E) -> MetaError {
        MetaError::EncodeError(AnyError::new(&e))
    }
}
