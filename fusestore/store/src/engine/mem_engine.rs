// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use tonic::Status;

use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Db;

// MemEngine is a prototype storage that is primarily used for testing purposes.
pub struct MemEngine {
    pub dbs: HashMap<String, Db>,
    pub next_id: i64,
    pub next_ver: i64,
}

impl MemEngine {
    #[allow(dead_code)]
    pub fn create() -> Arc<Mutex<MemEngine>> {
        let e = MemEngine {
            dbs: HashMap::new(),
            next_id: 0,
            next_ver: 0,
        };
        Arc::new(Mutex::new(e))
    }

    #[allow(dead_code)]
    pub fn create_database(
        &mut self,
        cmd: CmdCreateDatabase,
        if_not_exists: bool,
    ) -> anyhow::Result<i64> {
        // TODO: support plan.engine plan.options
        let curr = self.dbs.get(&cmd.db_name);
        if curr.is_some() && if_not_exists {
            return Err(anyhow::anyhow!("database exists"));
        }

        let mut db = cmd
            .db
            .ok_or_else(|| Status::invalid_argument("require field: CmdCreateDatabase::db"))?;

        let db_id = self.create_id();
        db.db_id = db_id;
        db.ver = self.create_ver();

        self.dbs.insert(cmd.db_name, db);

        Ok(db_id)
    }

    #[allow(dead_code)]
    pub fn get_database(&self, db: String) -> anyhow::Result<Db> {
        let x = self
            .dbs
            .get(&db)
            .ok_or_else(|| anyhow::anyhow!("database not found"))?;
        Ok(x.clone())
    }

    // Create a table. It generates a table id and fill it.
    #[allow(dead_code)]
    pub fn create_table(
        &mut self,
        cmd: CmdCreateTable,
        if_not_exists: bool,
    ) -> Result<i64, Status> {
        // TODO: support plan.engine plan.options

        let table_id = self
            .dbs
            .get(&cmd.db_name)
            .ok_or_else(|| Status::invalid_argument("database not found"))?
            .table_name_to_id
            .get(&cmd.table_name);

        if table_id.is_some() && if_not_exists {
            return Err(Status::already_exists("table exists"));
        }

        let mut table = cmd
            .table
            .ok_or_else(|| Status::invalid_argument("require field: CmdCreateTable::table"))?;

        let table_id = self.create_id();
        table.table_id = table_id;
        table.ver = self.create_ver();

        let db = self.dbs.get_mut(&cmd.db_name).unwrap();

        db.table_name_to_id.insert(cmd.table_name, table_id);
        db.tables.insert(table_id, table);

        Ok(table_id)
    }

    pub fn create_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn create_ver(&mut self) -> i64 {
        let ver = self.next_ver;
        self.next_ver += 1;
        ver
    }
}
