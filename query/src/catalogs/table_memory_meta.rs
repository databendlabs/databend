//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::MetaId;

use crate::storages::Table;

pub struct DbTables {
    name_to_table: RwLock<HashMap<String, Arc<dyn Table>>>,
    id_to_table: RwLock<HashMap<MetaId, Arc<dyn Table>>>,
}

pub struct InMemoryMetas {
    next_table_id: AtomicU64,
    next_db_id: AtomicU64,
    db_tables: RwLock<HashMap<String, DbTables>>,
}

impl InMemoryMetas {
    pub fn create(next_db_id: u64, next_table_id: u64) -> Self {
        InMemoryMetas {
            next_table_id: AtomicU64::new(next_table_id),
            next_db_id: AtomicU64::new(next_db_id),
            db_tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn init_db(&self, db: &str) {
        let mut dbs = self.db_tables.write();
        dbs.insert(db.to_string(), DbTables {
            name_to_table: RwLock::new(HashMap::new()),
            id_to_table: RwLock::new(HashMap::new()),
        });
    }

    /// Get the next db id.
    pub fn next_db_id(&self) -> u64 {
        self.next_db_id.fetch_add(1, Ordering::Relaxed);
        self.next_db_id.load(Ordering::Relaxed) as u64
    }

    /// Get the next table id.
    pub fn next_table_id(&self) -> u64 {
        self.next_table_id.fetch_add(1, Ordering::Relaxed);
        self.next_table_id.load(Ordering::Relaxed) as u64
    }

    pub fn insert(&self, db: &str, tbl_ref: Arc<dyn Table>) {
        if let Some(db_tables) = self.db_tables.write().get(db) {
            let name = tbl_ref.name().to_owned();
            db_tables
                .name_to_table
                .write()
                .insert(name, tbl_ref.clone());
            db_tables
                .id_to_table
                .write()
                .insert(tbl_ref.get_id(), tbl_ref);
        } else {
            panic!("Logical Error: Need create database `{}` first", db)
        }
    }

    pub fn get_by_name(&self, db: &str, name: &str) -> Result<Arc<dyn Table>> {
        if let Some(db_tables) = self.db_tables.read().get(db) {
            db_tables
                .name_to_table
                .read()
                .get(name)
                .cloned()
                .ok_or_else(|| {
                    ErrorCode::UnknownTable(format!("`{}.{}` table is unknown", db, name))
                })
        } else {
            Err(ErrorCode::UnknownDatabase(format!(
                "`{}` database is unknown",
                db
            )))
        }
    }

    pub fn get_by_id(&self, id: &MetaId) -> Option<Arc<dyn Table>> {
        for (_db, db_tables) in self.db_tables.read().iter() {
            if db_tables.id_to_table.read().contains_key(id) {
                return db_tables.id_to_table.read().get(id).cloned();
            }
        }
        None
    }

    pub fn get_all_tables(&self, db: &str) -> Result<Vec<Arc<dyn Table>>> {
        if let Some(db_tables) = self.db_tables.read().get(db) {
            Ok(db_tables.name_to_table.read().values().cloned().collect())
        } else {
            Err(ErrorCode::UnknownDatabase(format!(
                "{} database is unknown",
                db
            )))
        }
    }
}
