// Copyright 2020 Datafuse Labs.
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
//

use std::convert::TryInto;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_meta_flight::CreateDatabaseAction;
use common_meta_flight::CreateTableAction;
use common_meta_flight::DropDatabaseAction;
use common_meta_flight::DropTableAction;
use common_meta_flight::GetDatabaseAction;
use common_meta_flight::GetDatabasesAction;
use common_meta_flight::GetTableAction;
use common_meta_flight::GetTableExtReq;
use common_meta_flight::GetTablesAction;
use common_meta_flight::UpsertTableOptionReq;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd::CreateDatabase;
use common_meta_types::Cmd::CreateTable;
use common_meta_types::Cmd::DropDatabase;
use common_meta_types::Cmd::DropTable;
use common_meta_types::Cmd::UpsertTableOptions;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::LogEntry;
use common_meta_types::MatchSeq;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use log::info;
use maplit::hashmap;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

// Db
#[async_trait::async_trait]
impl RequestHandler<CreateDatabaseAction> for ActionHandler {
    async fn handle(
        &self,
        act: CreateDatabaseAction,
    ) -> common_exception::Result<CreateDatabaseReply> {
        let plan = act.plan;
        let db_name = &plan.db;
        let if_not_exists = plan.if_not_exists;

        let cr = LogEntry {
            txid: None,
            cmd: CreateDatabase {
                name: db_name.clone(),
            },
        };

        let res = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        let ch: Change<u64> = res.try_into().unwrap();
        let (prev, result) = ch.unpack_data();

        if prev.is_some() && !if_not_exists {
            return Err(ErrorCode::DatabaseAlreadyExists(format!(
                "{} database exists",
                db_name
            )));
        }

        Ok(CreateDatabaseReply {
            // TODO(xp): return DatabaseInfo?
            database_id: result.unwrap(),
        })
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetDatabaseAction> for ActionHandler {
    async fn handle(&self, act: GetDatabaseAction) -> common_exception::Result<DatabaseInfo> {
        let db_name = act.db;
        let db = self
            .meta_node
            .get_state_machine()
            .await
            .get_database(&db_name)?;

        match db {
            Some(db) => Ok(DatabaseInfo {
                database_id: db.data,
                db: db_name.to_string(),
            }),
            None => Err(ErrorCode::UnknownDatabase(db_name)),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropDatabaseAction> for ActionHandler {
    async fn handle(&self, act: DropDatabaseAction) -> common_exception::Result<()> {
        let db_name = &act.plan.db;
        let if_exists = act.plan.if_exists;
        let cr = LogEntry {
            txid: None,
            cmd: DropDatabase {
                name: db_name.clone(),
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        let (prev, _result) = match rst {
            AppliedState::DatabaseId(Change { prev, result }) => (prev, result),
            _ => return Err(ErrorCode::MetaNodeInternalError("not a Database result")),
        };

        if prev.is_some() || if_exists {
            Ok(())
        } else {
            Err(ErrorCode::UnknownDatabase(format!(
                "database not found: {:}",
                db_name
            )))
        }
    }
}

// table
#[async_trait::async_trait]
impl RequestHandler<CreateTableAction> for ActionHandler {
    async fn handle(&self, act: CreateTableAction) -> common_exception::Result<CreateTableReply> {
        let plan = act.plan;
        let db_name = &plan.db;
        let table_name = &plan.table;
        let if_not_exists = plan.if_not_exists;

        info!("create table: {:}: {:?}", &db_name, &table_name);

        let table_meta = plan.table_meta;

        let cr = LogEntry {
            txid: None,
            cmd: CreateTable {
                db_name: db_name.clone(),
                table_name: table_name.clone(),
                table_meta,
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        let (prev, result) = match rst {
            AppliedState::TableIdent { prev, result } => (prev, result),
            _ => return Err(ErrorCode::MetaNodeInternalError("not a Table result")),
        };
        if prev.is_some() && !if_not_exists {
            return Err(ErrorCode::TableAlreadyExists(format!(
                "table exists: {}",
                table_name
            )));
        }

        Ok(CreateTableReply {
            table_id: result.unwrap().table_id,
        })
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropTableAction> for ActionHandler {
    async fn handle(&self, act: DropTableAction) -> common_exception::Result<()> {
        let db_name = &act.plan.db;
        let table_name = &act.plan.table;
        let if_exists = act.plan.if_exists;

        let cr = LogEntry {
            txid: None,
            cmd: DropTable {
                db_name: db_name.clone(),
                table_name: table_name.clone(),
            },
        };

        let res = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        let ch: Change<TableMeta> = res.try_into().unwrap();
        let (prev, _result) = ch.unpack();

        if prev.is_some() || if_exists {
            Ok(())
        } else {
            Err(ErrorCode::UnknownTable(format!(
                "Unknown table: '{:}'",
                table_name
            )))
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableAction> for ActionHandler {
    async fn handle(&self, act: GetTableAction) -> common_exception::Result<TableInfo> {
        let db_name = &act.db;
        let table_name = &act.table;

        let x = self
            .meta_node
            .get_state_machine()
            .await
            .get_database(db_name)?;
        let db = x.ok_or_else(|| {
            ErrorCode::UnknownDatabase(format!("get table: database not found {:}", db_name))
        })?;

        let db_id = db.data;

        let seq_table_id = self
            .meta_node
            .lookup_table_id(db_id, table_name)
            .await?
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{:}'", table_name)))?;

        let table_id = seq_table_id.data.0;
        let result = self.meta_node.get_table_by_id(&table_id).await?;

        match result {
            Some(table) => Ok(TableInfo::new(
                db_name,
                table_name,
                TableIdent::new(table_id, table.seq),
                table.data,
            )),
            None => Err(ErrorCode::UnknownTable(table_name)),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableExtReq> for ActionHandler {
    async fn handle(&self, act: GetTableExtReq) -> common_exception::Result<TableInfo> {
        // TODO duplicated code
        let table_id = act.tbl_id;
        let result = self.meta_node.get_table_by_id(&table_id).await?;
        match result {
            Some(table) => Ok(TableInfo::new(
                "",
                "",
                TableIdent::new(table_id, table.seq),
                table.data,
            )),
            None => Err(ErrorCode::UnknownTable(format!(
                "table of id {} not found",
                act.tbl_id
            ))),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetDatabasesAction> for ActionHandler {
    async fn handle(
        &self,
        _req: GetDatabasesAction,
    ) -> common_exception::Result<Vec<Arc<DatabaseInfo>>> {
        let res = self.meta_node.get_state_machine().await.get_databases()?;

        Ok(res
            .iter()
            .map(|(name, db)| {
                Arc::new(DatabaseInfo {
                    database_id: *db,
                    db: name.to_string(),
                })
            })
            .collect::<Vec<_>>())
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTablesAction> for ActionHandler {
    async fn handle(&self, req: GetTablesAction) -> common_exception::Result<Vec<Arc<TableInfo>>> {
        let res = self.meta_node.get_tables(req.db.as_str()).await?;
        Ok(res.iter().map(|t| Arc::new(t.clone())).collect::<Vec<_>>())
    }
}
#[async_trait::async_trait]
impl RequestHandler<UpsertTableOptionReq> for ActionHandler {
    async fn handle(
        &self,
        req: UpsertTableOptionReq,
    ) -> common_exception::Result<UpsertTableOptionReply> {
        let cr = LogEntry {
            txid: None,
            cmd: UpsertTableOptions {
                table_id: req.table_id,
                seq: MatchSeq::Exact(req.table_version),
                table_options: hashmap! {
                    req.option_key => Some(req.option_value),
                },
            },
        };

        let res = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        if !res.changed() {
            let ch: Change<TableMeta> = res.try_into().unwrap();
            let (prev, _result) = ch.unwrap();

            return Err(ErrorCode::TableVersionMissMatch(format!(
                "targeting version {}, current version {}",
                req.table_version, prev.seq,
            )));
        }

        Ok(())
    }
}
