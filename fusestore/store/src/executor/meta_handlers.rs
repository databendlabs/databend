// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow_flight;
use common_arrow::arrow_flight::FlightData;
use common_exception::ErrorCode;
use common_flights::CreateDatabaseAction;
use common_flights::CreateTableAction;
use common_flights::DropDatabaseAction;
use common_flights::DropTableAction;
use common_flights::GetDatabaseAction;
use common_flights::GetTableAction;
use common_store_api::CreateDatabaseActionResult;
use common_store_api::CreateTableActionResult;
use common_store_api::DropDatabaseActionResult;
use common_store_api::DropTableActionResult;
use common_store_api::GetDatabaseActionResult;
use common_store_api::GetTableActionResult;
use log::info;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;
use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Db;
use crate::protobuf::Table;

// Db

#[async_trait::async_trait]
impl RequestHandler<CreateDatabaseAction> for ActionHandler {
    async fn handle(
        &self,
        act: CreateDatabaseAction,
    ) -> common_exception::Result<CreateDatabaseActionResult> {
        let plan = act.plan;
        let mut meta = self.meta.lock();

        let cmd = CmdCreateDatabase {
            db_name: plan.db,
            db: Some(Db {
                // meta fills it
                db_id: -1,
                ver: -1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            }),
        };

        let database_id = meta.create_database(cmd, plan.if_not_exists)?;

        Ok(CreateDatabaseActionResult { database_id })
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetDatabaseAction> for ActionHandler {
    async fn handle(
        &self,
        act: GetDatabaseAction,
    ) -> common_exception::Result<GetDatabaseActionResult> {
        // TODO(xp): create/drop/get database should base on MetaNode
        let db_name = &act.db;
        let meta = self.meta.lock();

        let db = meta.dbs.get(db_name);

        match db {
            Some(db) => {
                let rst = GetDatabaseActionResult {
                    database_id: db.db_id,
                    db: db_name.clone(),
                };
                Ok(rst)
            }
            None => Err(ErrorCode::UnknownDatabase(db_name.to_string())),
        }
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropDatabaseAction> for ActionHandler {
    async fn handle(
        &self,
        act: DropDatabaseAction,
    ) -> common_exception::Result<DropDatabaseActionResult> {
        let mut meta = self.meta.lock();
        let _ = meta.drop_database(&act.plan.db, act.plan.if_exists)?;
        Ok(DropDatabaseActionResult {})
    }
}
// table

#[async_trait::async_trait]
impl RequestHandler<CreateTableAction> for ActionHandler {
    async fn handle(
        &self,
        act: CreateTableAction,
    ) -> common_exception::Result<CreateTableActionResult> {
        let plan = act.plan;
        let db_name = plan.db;
        let table_name = plan.table;

        info!("create table: {:}: {:?}", db_name, table_name);

        let mut meta = self.meta.lock();

        let options = common_arrow::arrow::ipc::writer::IpcWriteOptions::default();
        let flight_data =
            arrow_flight::utils::flight_data_from_arrow_schema(&plan.schema.to_arrow(), &options);

        let table = Table {
            // the storage engine fills the id.
            table_id: -1,
            ver: -1,
            schema: flight_data.data_header,
            options: plan.options,

            // TODO
            placement_policy: vec![],
        };

        let cmd = CmdCreateTable {
            db_name,
            table_name,
            table: Some(table),
        };

        let table_id = meta.create_table(cmd, plan.if_not_exists)?;

        Ok(CreateTableActionResult { table_id })
    }
}

#[async_trait::async_trait]
impl RequestHandler<DropTableAction> for ActionHandler {
    async fn handle(
        &self,
        act: DropTableAction,
    ) -> common_exception::Result<DropTableActionResult> {
        let mut meta = self.meta.lock();
        let _ = meta.drop_table(&act.plan.db, &act.plan.table, act.plan.if_exists)?;
        Ok(DropTableActionResult {})
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetTableAction> for ActionHandler {
    async fn handle(&self, act: GetTableAction) -> common_exception::Result<GetTableActionResult> {
        let db_name = act.db;
        let table_name = act.table;

        info!("create table: {:}: {:?}", db_name, table_name);

        let mut meta = self.meta.lock();

        let table = meta.get_table(db_name.clone(), table_name.clone())?;

        let arrow_schema = ArrowSchema::try_from(&FlightData {
            data_header: table.schema,
            ..Default::default()
        })
        .map_err(|e| ErrorCode::IllegalSchema(format!("invalid schema: {:}", e.to_string())))?;

        let rst = GetTableActionResult {
            table_id: table.table_id,
            db: db_name,
            name: table_name,
            schema: Arc::new(arrow_schema.into()),
        };

        Ok(rst)
    }
}
