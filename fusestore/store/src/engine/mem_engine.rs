// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_flights::AppendResult;
use common_flights::DataPartInfo;
use common_planners::Partition;
use common_planners::Statistics;
use tonic::Status;

use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Db;
use crate::protobuf::Table;

// MemEngine is a prototype storage that is primarily used for testing purposes.
pub struct MemEngine {
    pub dbs: HashMap<String, Db>,
    pub tbl_parts: HashMap<String, HashMap<String, Vec<DataPartInfo>>>,
    pub next_id: i64,
    pub next_ver: i64,
}

impl MemEngine {
    #[allow(dead_code)]
    pub fn create() -> Arc<Mutex<MemEngine>> {
        let e = MemEngine {
            dbs: HashMap::new(),
            tbl_parts: HashMap::new(),
            next_id: 0,
            next_ver: 0,
        };
        Arc::new(Mutex::new(e))
    }

    pub fn create_database(
        &mut self,
        cmd: CmdCreateDatabase,
        if_not_exists: bool,
    ) -> anyhow::Result<i64> {
        // TODO: support plan.engine plan.options
        let curr = self.dbs.get(&cmd.db_name);
        if let Some(curr) = curr {
            return if if_not_exists {
                Ok(curr.db_id)
            } else {
                Err(anyhow::anyhow!("{} database exists", cmd.db_name))
            };
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

    pub fn drop_database(&mut self, db_name: &str, if_exists: bool) -> Result<(), Status> {
        self.remove_db_data_parts(db_name);
        let entry = self.dbs.remove_entry(db_name);
        match (entry, if_exists) {
            (_, true) => Ok(()),
            (Some((_id, _db)), false) => Ok(()),
            (_, false) => Err(Status::not_found(format!("database {} not found", db_name))),
        }
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

        if let Some(table_id) = table_id {
            return if if_not_exists {
                Ok(*table_id)
            } else {
                Err(Status::already_exists("table exists"))
            };
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

    pub fn drop_table(
        &mut self,
        db_name: &str,
        tbl_name: &str,
        if_exists: bool,
    ) -> Result<(), Status> {
        self.remove_table_data_parts(db_name, tbl_name);
        let r = self.dbs.get_mut(db_name).map(|db| {
            let name2id_removed = db.table_name_to_id.remove_entry(tbl_name);
            let id_removed = name2id_removed
                .as_ref()
                .and_then(|(_, id)| db.tables.remove(&id));
            (name2id_removed, id_removed)
        });
        match (r, if_exists) {
            (_, true) => Ok(()),
            (None, false) => Err(Status::not_found(format!("database {} not found", db_name))),
            (Some((None, _)), false) => {
                Err(Status::not_found(format!("table {} not found", tbl_name)))
            }
            (Some((Some(_), Some(_))), false) => Ok(()),
            _ => Err(Status::internal(
                "inconsistent meta state, mappings between names and ids are out-of-sync"
                    .to_string(),
            )),
        }
    }

    pub fn get_table(&mut self, db_name: String, table_name: String) -> Result<Table, Status> {
        let db = self
            .dbs
            .get(&db_name)
            .ok_or_else(|| Status::not_found(format!("database not found: {:}", db_name)))?;

        let table_id = db
            .table_name_to_id
            .get(&table_name)
            .ok_or_else(|| Status::not_found(format!("table not found: {:}", table_name)))?;

        let table = db.tables.get(&table_id).unwrap();
        Ok(table.clone())
    }

    pub fn get_data_parts(&self, db_name: &str, table_name: &str) -> Option<Vec<DataPartInfo>> {
        let parts = self.tbl_parts.get(db_name);
        parts.and_then(|m| m.get(table_name)).map(Clone::clone)
    }

    pub fn append_data_parts(
        &mut self,
        db_name: &str,
        table_name: &str,
        append_res: &AppendResult,
    ) {
        let part_info = || {
            append_res
                .parts
                .iter()
                .map(|p| {
                    let loc = &p.location;
                    DataPartInfo {
                        partition: Partition {
                            name: loc.clone(),
                            version: 0,
                        },
                        stats: Statistics {
                            read_bytes: p.disk_bytes,
                            read_rows: p.rows,
                        },
                    }
                })
                .collect::<Vec<_>>()
        };
        self.tbl_parts
            .entry(db_name.to_string())
            .and_modify(move |e| {
                e.entry(table_name.to_string())
                    .and_modify(|v| v.append(&mut part_info()))
                    .or_insert_with(part_info);
            })
            .or_insert_with(|| {
                [(table_name.to_string(), part_info())]
                    .iter()
                    .cloned()
                    .collect()
            });
    }

    pub fn remove_table_data_parts(&mut self, db_name: &str, table_name: &str) {
        self.tbl_parts
            .remove(db_name)
            .and_then(|mut t| t.remove(table_name));
    }

    pub fn remove_db_data_parts(&mut self, db_name: &str) {
        self.tbl_parts.remove(db_name);
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
