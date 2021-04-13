// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::arrow_flight;
use common_flights::store_do_action::CreateDatabaseActionResult;
use common_flights::store_do_action::StoreDoAction;
use common_flights::CreateDatabaseAction;
use common_flights::CreateTableAction;
use common_flights::CreateTableActionResult;
use common_flights::StoreDoActionResult;
#[allow(unused_imports)]
use log::error;
#[allow(unused_imports)]
use log::info;
use tonic::Status;

use crate::engine::MemEngine;
use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Db;
use crate::protobuf::Table;

pub struct ActionHandler {
    // TODO zbr's proposol
    // catalog: Box<dyn Catalog>,
    // tbl_spec: TableSpec,
    // db_spec: DatabaseSpec,
    meta: Arc<Mutex<MemEngine>>,
}

impl ActionHandler {
    pub fn create() -> Self {
        ActionHandler {
            meta: MemEngine::create(),
        }
    }

    pub async fn execute(&self, action: StoreDoAction) -> Result<StoreDoActionResult, Status> {
        match action {
            StoreDoAction::ReadPlan(_) => Err(Status::internal("Store read plan unimplemented")),
            StoreDoAction::CreateDatabase(a) => self.create_db(a).await,
            StoreDoAction::CreateTable(a) => self.create_table(a).await,
        }
    }

    async fn create_db(&self, act: CreateDatabaseAction) -> Result<StoreDoActionResult, Status> {
        let plan = act.plan;
        let mut meta = self.meta.lock().unwrap();

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

        let database_id = meta
            .create_database(cmd, plan.if_not_exists)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(StoreDoActionResult::CreateDatabase(
            CreateDatabaseActionResult { database_id },
        ))
    }

    async fn create_table(&self, act: CreateTableAction) -> Result<StoreDoActionResult, Status> {
        let plan = act.plan;
        let db_name = plan.db;
        let table_name = plan.table;

        info!("create table: {:}: {:?}", db_name, table_name);

        let mut meta = self.meta.lock().unwrap();

        let options = common_arrow::arrow::ipc::writer::IpcWriteOptions::default();
        let flight_data =
            arrow_flight::utils::flight_data_from_arrow_schema(&plan.schema, &options);

        let table = Table {
            // the storage engine fills the id.
            table_id: -1,
            ver: -1,
            schema: flight_data.data_header,
            // TODO
            placement_policy: vec![],
        };

        let cmd = CmdCreateTable {
            db_name,
            table_name,
            table: Some(table),
        };

        let table_id = meta.create_table(cmd, plan.if_not_exists)?;

        Ok(StoreDoActionResult::CreateTable(CreateTableActionResult {
            table_id,
        }))
    }
}
