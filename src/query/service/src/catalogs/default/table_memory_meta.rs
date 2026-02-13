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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_meta_types::MetaId;

use crate::storages::Table;

pub struct DbTables {
    name_to_table: DashMap<String, Arc<dyn Table>>,
    id_to_table: DashMap<MetaId, Arc<dyn Table>>,
}

pub struct InMemoryMetas {
    next_table_id: AtomicU64,
    next_db_id: AtomicU64,
    db_tables: DashMap<String, DbTables>,
}

impl InMemoryMetas {
    pub fn create(next_db_id: u64, next_table_id: u64) -> Self {
        InMemoryMetas {
            next_table_id: AtomicU64::new(next_table_id),
            next_db_id: AtomicU64::new(next_db_id),
            db_tables: DashMap::new(),
        }
    }

    pub fn init_db(&self, db: &str) {
        self.db_tables.insert(db.to_string(), DbTables {
            name_to_table: DashMap::new(),
            id_to_table: DashMap::new(),
        });
    }

    /// Get the next db id.
    pub fn next_db_id(&self) -> u64 {
        self.next_db_id.fetch_add(1, Ordering::Relaxed);
        self.next_db_id.load(Ordering::Relaxed)
    }

    /// Get the next table id.
    pub fn next_table_id(&self) -> u64 {
        self.next_table_id.fetch_add(1, Ordering::Relaxed);
        self.next_table_id.load(Ordering::Relaxed)
    }

    pub fn insert(&self, db: &str, tbl_ref: Arc<dyn Table>) {
        if let Some(db_tables) = self.db_tables.get_mut(db) {
            let name = tbl_ref.name().to_owned();
            db_tables.name_to_table.insert(name, tbl_ref.clone());
            db_tables.id_to_table.insert(tbl_ref.get_id(), tbl_ref);
        } else {
            panic!("Logical Error: Need create database `{}` first", db)
        }
    }

    pub fn get_by_name(&self, db: &str, name: &str) -> Result<Arc<dyn Table>> {
        if let Some(db_tables) = self.db_tables.get(db) {
            db_tables
                .value()
                .name_to_table
                .get(name)
                .map(|entry| entry.value().clone())
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
        for entry in self.db_tables.iter() {
            let db_tables = entry.value();
            if db_tables.id_to_table.contains_key(id) {
                return db_tables
                    .id_to_table
                    .get(id)
                    .map(|entry| entry.value().clone());
            }
        }
        None
    }

    pub fn get_all_tables(&self, db: &str) -> Result<Vec<Arc<dyn Table>>> {
        if let Some(db_tables) = self.db_tables.get(db) {
            Ok(db_tables
                .value()
                .name_to_table
                .iter()
                .map(|entry| entry.value().clone())
                .collect())
        } else {
            Err(ErrorCode::UnknownDatabase(format!(
                "{} database is unknown",
                db
            )))
        }
    }
}
