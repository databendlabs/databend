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

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::TableEngineType;

use crate::catalogs::impls::LOCAL_TBL_ID_BEGIN;
use crate::catalogs::Database;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::TableMeta;
use crate::datasources::local::CsvTable;
use crate::datasources::local::LocalDatabase;
use crate::datasources::local::MemoryTable;
use crate::datasources::local::NullTable;
use crate::datasources::local::ParquetTable;
use crate::datasources::MetaBackend;

/// The backend of the local database.
pub struct LocalMetaBackend {
    databases: RwLock<HashMap<String, Arc<dyn Database>>>,
    tables: RwLock<HashMap<String, InMemoryMetas>>,
    tbl_id_seq: AtomicU64,
}

impl LocalMetaBackend {
    pub fn create() -> Self {
        let databases: RwLock<HashMap<String, Arc<dyn Database>>> = Default::default();
        let default_database = Arc::new(LocalDatabase::create("default"));
        databases
            .write()
            .insert("default".to_string(), default_database);

        let tbl_id_seq = AtomicU64::new(LOCAL_TBL_ID_BEGIN);
        LocalMetaBackend {
            databases,
            tables: Default::default(),
            tbl_id_seq,
        }
    }

    fn next_db_id(&self) -> u64 {
        // `fetch_add` wraps around on overflow, but as LOCAL_TBL_ID_BEGIN
        // is defined as (1 << 62) + 10000, there are about 13 quintillion ids are reserved
        // for local tables, we do not check overflow here.
        self.tbl_id_seq.fetch_add(1, Ordering::SeqCst)
    }
}

impl MetaBackend for LocalMetaBackend {
    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        let lock = self.tables.read();
        let tables = lock.get(db_name);
        match tables {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some(v) => {
                let table = v.id2meta.get(&table_id).ok_or_else(|| {
                    ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id))
                })?;
                Ok(table.clone())
            }
        }
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let lock = self.tables.read();
        let tables = lock.get(db_name);
        match tables {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some(v) => {
                let table = v.name2meta.get(table_name).ok_or_else(|| {
                    ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name))
                })?;
                Ok(table.clone())
            }
        }
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableMeta>>> {
        let lock = self.tables.read();
        let tables = lock.get(db_name);
        match tables {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some(v) => Ok(v.name2meta.values().cloned().collect()),
        }
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let clone = plan.clone();
        let db_name = clone.db.as_str();
        let table_name = clone.table.as_str();

        let mut lock = self.tables.write();
        let tables = lock.get_mut(db_name);
        match tables {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some(v) => {
                if v.name2meta.get(table_name).is_some() {
                    return if plan.if_not_exists {
                        Ok(())
                    } else {
                        return Err(ErrorCode::UnImplement(format!(
                            "Table: '{}.{}' already exists.",
                            db_name, table_name,
                        )));
                    };
                }

                let table = match &plan.engine {
                    TableEngineType::Parquet => {
                        ParquetTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
                    }
                    TableEngineType::Csv => {
                        CsvTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
                    }
                    TableEngineType::Null => {
                        NullTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
                    }
                    TableEngineType::Memory => {
                        MemoryTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
                    }
                    _ => {
                        return Result::Err(ErrorCode::UnImplement(format!(
                            "Local database does not support '{:?}' table engine",
                            plan.engine
                        )));
                    }
                };

                let table_meta = TableMeta::create(Arc::from(table), self.next_db_id());
                v.insert(table_meta);
                Ok(())
            }
        }
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        let table_name = plan.table.as_str();

        let lock = self.tables.read();
        let tables = lock.get(db_name);

        // Get the table id.
        let tbl_id = match tables {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some(v) => {
                let by_name = v.name2meta.get(table_name);
                match by_name {
                    None => {
                        if plan.if_exists {
                            return Ok(());
                        } else {
                            return Err(ErrorCode::UnknownTable(format!(
                                "Unknown table: '{}.{}'",
                                db_name, table_name
                            )));
                        }
                    }
                    Some(tbl) => tbl.meta_id(),
                }
            }
        };

        // Remove.
        let mut lock = self.tables.write();
        let tables = lock.get_mut(db_name);
        match tables {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some(v) => {
                v.name2meta.remove(table_name);
                v.id2meta.remove(&tbl_id);
            }
        }
        Ok(())
    }

    fn get_database(&self, db_name: &str) -> Result<Option<Arc<dyn Database>>> {
        let lock = self.databases.read();
        let db = lock.get(db_name);
        match db {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        Ok(self.databases.read().values().cloned().collect::<Vec<_>>())
    }

    fn exists_database(&self, db_name: &str) -> Result<bool> {
        Ok(self.databases.read().get(db_name).is_some())
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        if self.exists_database(db_name)? {
            return if plan.if_not_exists {
                Ok(())
            } else {
                Err(ErrorCode::UnknownDatabase(format!(
                    "Database: '{}' already exists.",
                    db_name
                )))
            };
        }

        let database = LocalDatabase::create(db_name);
        self.databases.write().insert(plan.db, Arc::new(database));
        Ok(())
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        if !self.exists_database(db_name)? {
            return if plan.if_exists {
                Ok(())
            } else {
                Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: '{}'",
                    db_name
                )))
            };
        }
        self.databases.write().remove(db_name);
        Ok(())
    }
}
