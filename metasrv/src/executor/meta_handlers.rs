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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::ipc::write::common::IpcWriteOptions;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_schema;
use common_arrow::arrow_flight::FlightData;
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
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Cmd::CreateDatabase;
use common_meta_types::Cmd::CreateTable;
use common_meta_types::Cmd::DropDatabase;
use common_meta_types::Cmd::DropTable;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::Database;
use common_meta_types::DatabaseInfo;
use common_meta_types::GetDatabasesReply;
use common_meta_types::GetTablesReply;
use common_meta_types::LogEntry;
use common_meta_types::Table;
use common_meta_types::TableInfo;
use log::info;

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
                if_not_exists,
                db: Database {
                    database_id: 0,
                    database_engine: plan.engine.clone(),
                    tables: HashMap::new(),
                },
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            AppliedState::DataBase { prev, result } => {
                if let Some(prev) = prev {
                    if if_not_exists {
                        Ok(CreateDatabaseReply {
                            database_id: prev.database_id,
                        })
                    } else {
                        Err(ErrorCode::DatabaseAlreadyExists(format!(
                            "{} database exists",
                            db_name
                        )))
                    }
                } else {
                    Ok(CreateDatabaseReply {
                        database_id: result.unwrap().database_id,
                    })
                }
            }

            _ => Err(ErrorCode::MetaNodeInternalError("not a Database result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetDatabaseAction> for ActionHandler {
    async fn handle(&self, act: GetDatabaseAction) -> common_exception::Result<DatabaseInfo> {
        let db_name = act.db;
        let db = self.meta_node.get_database(&db_name).await;

        match db {
            Some(db) => {
                let rst = DatabaseInfo {
                    database_id: db.database_id,
                    db: db_name,
                    engine: db.database_engine,
                };
                Ok(rst)
            }
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

        match rst {
            AppliedState::DataBase { prev, .. } => {
                if prev.is_some() || if_exists {
                    Ok(())
                } else {
                    Err(ErrorCode::UnknownDatabase(format!(
                        "database not found: {:}",
                        db_name
                    )))
                }
            }
            _ => Err(ErrorCode::MetaNodeInternalError("not a Database result")),
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

        let options = IpcWriteOptions::default();
        let flight_data = flight_data_from_arrow_schema(&plan.schema.to_arrow(), &options);

        let table = Table {
            table_id: 0,
            table_name: table_name.to_string(),
            database_id: 0, // this field is unused during the creation of table
            db_name: db_name.to_string(),
            schema: flight_data.data_header,
            table_engine: plan.engine.clone(),
            table_options: plan.options.clone(),
            parts: Default::default(),
        };

        let cr = LogEntry {
            txid: None,
            cmd: CreateTable {
                db_name: db_name.clone(),
                table_name: table_name.clone(),
                if_not_exists,
                table,
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            AppliedState::Table { prev, result } => {
                if let Some(prev) = prev {
                    if if_not_exists {
                        Ok(CreateTableReply {
                            table_id: prev.table_id,
                        })
                    } else {
                        Err(ErrorCode::TableAlreadyExists(format!(
                            "table exists: {}",
                            table_name
                        )))
                    }
                } else {
                    Ok(CreateTableReply {
                        table_id: result.unwrap().table_id,
                    })
                }
            }
            _ => Err(ErrorCode::MetaNodeInternalError("not a Table result")),
        }
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
                if_exists,
            },
        };

        let rst = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        match rst {
            AppliedState::Table { prev, .. } => {
                if prev.is_some() || if_exists {
                    Ok(())
                } else {
                    Err(ErrorCode::UnknownTable(format!(
                        "table not found: {:}",
                        table_name
                    )))
                }
            }
            _ => Err(ErrorCode::MetaNodeInternalError("not a Table result")),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableAction> for ActionHandler {
    async fn handle(&self, act: GetTableAction) -> common_exception::Result<TableInfo> {
        let db_name = &act.db;
        let table_name = &act.table;

        let db = self.meta_node.get_database(db_name).await.ok_or_else(|| {
            ErrorCode::UnknownDatabase(format!("get table: database not found {:}", db_name))
        })?;

        let table_id = db
            .tables
            .get(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("table not found: {:}", table_name)))?;

        let result = self.meta_node.get_table(table_id).await;

        match result {
            Some(table) => {
                let arrow_schema = ArrowSchema::try_from(&FlightData {
                    data_header: table.schema,
                    ..Default::default()
                })
                .map_err(|e| {
                    ErrorCode::IllegalSchema(format!("invalid schema: {:}", e.to_string()))
                })?;
                let rst = TableInfo {
                    database_id: db.database_id,
                    table_id: table.table_id,
                    version: 0, // placeholder, not yet implemented in meta service
                    db: db_name.clone(),
                    name: table_name.clone(),
                    schema: Arc::new(arrow_schema.into()),
                    engine: table.table_engine.clone(),
                    options: table.table_options,
                };
                Ok(rst)
            }
            None => Err(ErrorCode::UnknownTable(table_name)),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableExtReq> for ActionHandler {
    async fn handle(&self, act: GetTableExtReq) -> common_exception::Result<TableInfo> {
        // TODO duplicated code
        let table_id = act.tbl_id;
        let result = self.meta_node.get_table(&table_id).await;
        match result {
            Some(table) => {
                let arrow_schema = ArrowSchema::try_from(&FlightData {
                    data_header: table.schema,
                    ..Default::default()
                })
                .map_err(|e| {
                    ErrorCode::IllegalSchema(format!("invalid schema: {:}", e.to_string()))
                })?;
                let rst = TableInfo {
                    database_id: table.database_id,
                    table_id: table.table_id,
                    db: table.db_name,
                    name: table.table_name,
                    version: 0,
                    schema: Arc::new(arrow_schema.into()),
                    engine: table.table_engine.clone(),
                    options: table.table_options,
                };
                Ok(rst)
            }
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
    ) -> common_exception::Result<GetDatabasesReply> {
        let res = self.meta_node.get_databases().await;
        Ok(res
            .iter()
            .map(|(name, db)| DatabaseInfo {
                database_id: db.database_id,
                db: name.to_string(),
                engine: db.database_engine.to_string(),
            })
            .collect::<Vec<_>>())
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTablesAction> for ActionHandler {
    async fn handle(&self, req: GetTablesAction) -> common_exception::Result<GetTablesReply> {
        let res = self.meta_node.get_tables(req.db.as_str()).await?;
        Ok(res
            .iter()
            .try_fold(Vec::new(), |mut acc, (id, name, tbl)| {
                let arrow_schema = ArrowSchema::try_from(&FlightData {
                    data_header: tbl.schema.clone(),
                    ..Default::default()
                })
                .map_err(|e| {
                    ErrorCode::IllegalSchema(format!(
                        "invalid schema of table id {}, error: {}",
                        *id,
                        e.to_string()
                    ))
                })?;

                let tbl_info = TableInfo {
                    database_id: tbl.database_id,
                    db: req.db.to_string(),
                    table_id: *id,
                    version: 0,
                    name: name.to_string(),
                    schema: Arc::new(arrow_schema.into()),
                    engine: tbl.table_engine.to_string(),
                    options: tbl.table_options.clone(),
                };

                acc.push(tbl_info);
                Ok::<_, ErrorCode>(acc)
            })?)
    }
}
