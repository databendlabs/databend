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
//

use std::convert::TryInto;
use std::sync::Arc;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_meta_flight::GetTableExtReq;
use common_meta_types::AddResult;
use common_meta_types::Change;
use common_meta_types::Cmd::CreateDatabase;
use common_meta_types::Cmd::CreateTable;
use common_meta_types::Cmd::DropDatabase;
use common_meta_types::Cmd::DropTable;
use common_meta_types::Cmd::UpsertTableOptions;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DatabaseMeta;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::LogEntry;
use common_meta_types::OkOrExist;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

// Db
#[async_trait::async_trait]
impl RequestHandler<CreateDatabaseReq> for ActionHandler {
    async fn handle(
        &self,
        req: CreateDatabaseReq,
    ) -> common_exception::Result<CreateDatabaseReply> {
        let db_name = &req.db;
        let engine = &req.engine;
        let if_not_exists = req.if_not_exists;

        let cr = LogEntry {
            txid: None,
            cmd: CreateDatabase {
                name: db_name.clone(),
                engine: engine.clone(),
            },
        };

        let res = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        let mut ch: Change<DatabaseMeta> = res.try_into().unwrap();
        let db_id = ch.ident.take().expect("Some(db_id)");
        let (prev, _result) = ch.unpack_data();

        if prev.is_some() && !if_not_exists {
            return Err(ErrorCode::DatabaseAlreadyExists(format!(
                "{} database exists",
                db_name
            )));
        }

        Ok(CreateDatabaseReply {
            // TODO(xp): return DatabaseInfo?
            database_id: db_id,
        })
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetDatabaseReq> for ActionHandler {
    async fn handle(
        &self,
        req: GetDatabaseReq,
    ) -> common_exception::Result<Arc<DatabaseInfo>> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropDatabaseReq> for ActionHandler {
    async fn handle(&self, req: DropDatabaseReq) -> common_exception::Result<DropDatabaseReply> {
        let db_name = &req.db;
        let if_exists = req.if_exists;
        let cr = LogEntry {
            txid: None,
            cmd: DropDatabase {
                name: db_name.clone(),
            },
        };

        let res = self
            .meta_node
            .write(cr)
            .await
            .map_err(|e| ErrorCode::MetaNodeInternalError(e.to_string()))?;

        let ch: Change<DatabaseMeta> = res.try_into().unwrap();
        let (prev, _result) = ch.unpack_data();

        if prev.is_some() || if_exists {
            Ok(DropDatabaseReply {})
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
impl RequestHandler<CreateTableReq> for ActionHandler {
    async fn handle(&self, req: CreateTableReq) -> common_exception::Result<CreateTableReply> {
        let db_name = &req.db;
        let table_name = &req.table;
        let if_not_exists = req.if_not_exists;

        tracing::info!("create table: {:}: {:?}", &db_name, &table_name);

        let table_meta = req.table_meta;

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

        let add_res: AddResult<TableMeta, u64> = rst.try_into()?;

        if let OkOrExist::Exists(_) = add_res.res {
            if !if_not_exists {
                return Err(ErrorCode::TableAlreadyExists(format!(
                    "table exists: {}",
                    table_name
                )));
            }
        }

        Ok(CreateTableReply {
            table_id: add_res.id.unwrap(),
        })
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropTableReq> for ActionHandler {
    async fn handle(&self, req: DropTableReq) -> common_exception::Result<DropTableReply> {
        let db_name = &req.db;
        let table_name = &req.table;
        let if_exists = req.if_exists;

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
            Ok(DropTableReply {})
        } else {
            Err(ErrorCode::UnknownTable(format!(
                "Unknown table: '{:}'",
                table_name
            )))
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableReq> for ActionHandler {
    async fn handle(
        &self,
        req: GetTableReq,
    ) -> common_exception::Result<Arc<TableInfo>> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
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
impl RequestHandler<ListDatabaseReq> for ActionHandler {
    async fn handle(
        &self,
        req: ListDatabaseReq,
    ) -> common_exception::Result<Vec<Arc<DatabaseInfo>>> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl RequestHandler<ListTableReq> for ActionHandler {
    async fn handle(
        &self,
        req: ListTableReq,
    ) -> common_exception::Result<Vec<Arc<TableInfo>>> {
        let res = self.meta_node.consistent_read(req).await?;
        Ok(res)
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
            cmd: UpsertTableOptions(req.clone()),
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
                "targeting version {:?}, current version {}",
                req.seq, prev.seq,
            )));
        }

        Ok(UpsertTableOptionReply {})
    }
}
