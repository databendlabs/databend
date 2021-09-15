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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::meta_api_impl::CreateDatabaseActionResult;
use common_flights::meta_api_impl::CreateTableActionResult;
use common_flights::meta_api_impl::DatabaseMetaReply;
use common_flights::meta_api_impl::DropDatabaseActionResult;
use common_flights::meta_api_impl::DropTableActionResult;
use common_flights::meta_api_impl::GetDatabaseActionResult;
use common_flights::meta_api_impl::GetTableActionResult;
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_store_api::CommitTableReply;
use common_store_api::MetaApi;

use crate::catalogs::impls::LOCAL_TBL_ID_BEGIN;
use crate::catalogs::Database;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::MetaBackend;
use crate::catalogs::TableMeta;
use crate::datasources::database::local::CsvTable;
use crate::datasources::database::local::LocalDatabase;
use crate::datasources::database::local::MemoryTable;
use crate::datasources::database::local::NullTable;
use crate::datasources::database::local::ParquetTable;
use crate::datasources::table::fuse::FuseTable;

type Databases = Arc<RwLock<HashMap<String, (Arc<dyn Database>, InMemoryMetas)>>>;

/// The backend of the local database.
/// Maintainer all the database and table information.
#[derive(Clone)]
pub struct LocalMetaBackend {
    databases: Databases,
    tbl_id_seq: Arc<RwLock<u64>>,
}

impl LocalMetaBackend {
    pub fn create() -> Self {
        let tbl_id_seq = Arc::new(RwLock::new(LOCAL_TBL_ID_BEGIN));
        LocalMetaBackend {
            databases: Arc::new(Default::default()),
            tbl_id_seq,
        }
    }

    // Register database.
    pub fn register_database(&self, db_name: &str) {
        let local = LocalDatabase::create(db_name, Arc::new(self.clone()));
        self.databases.write().insert(
            db_name.to_string(),
            (Arc::new(local), InMemoryMetas::create()),
        );
    }

    fn next_db_id(&self) -> u64 {
        // `fetch_add` wraps around on overflow, but as LOCAL_TBL_ID_BEGIN
        // is defined as (1 << 62) + 10000, there are about 13 quintillion ids are reserved
        // for local tables, we do not check overflow here.
        *self.tbl_id_seq.write() += 1;
        let r = self.tbl_id_seq.read();
        *r
    }
}

impl MetaBackend for LocalMetaBackend {
    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        let lock = self.databases.read();
        let v = lock.get(db_name);
        match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some((_, metas)) => {
                let table = metas.id2meta.get(&table_id).ok_or_else(|| {
                    ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id))
                })?;
                Ok(table.clone())
            }
        }
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let lock = self.databases.read();
        let v = lock.get(db_name);
        match v {
            None => Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database: {}",
                db_name
            ))),
            Some((_, metas)) => {
                let table = metas.name2meta.get(table_name).ok_or_else(|| {
                    ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name))
                })?;
                Ok(table.clone())
            }
        }
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableMeta>>> {
        let mut res = vec![];
        let lock = self.databases.read();
        let v = lock.get(db_name);
        match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )));
            }
            Some((_, metas)) => {
                for meta in metas.name2meta.values() {
                    res.push(meta.clone());
                }
            }
        }
        Ok(res)
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let clone = plan.clone();
        let db_name = clone.db.as_str();
        let table_name = clone.table.as_str();

        let table = match plan.engine.to_uppercase().as_str() {
            "PARQUET" => ParquetTable::try_create(plan.db, plan.table, plan.schema, plan.options)?,
            "CSV" => CsvTable::try_create(plan.db, plan.table, plan.schema, plan.options)?,
            "NULL" => NullTable::try_create(plan.db, plan.table, plan.schema, plan.options)?,
            "MEMORY" => MemoryTable::try_create(plan.db, plan.table, plan.schema, plan.options)?,
            "DATABEND_LOCAL" => {
                FuseTable::try_create(plan.db, plan.table, plan.schema, plan.options, self.clone())?
            }
            _ => {
                return Result::Err(ErrorCode::UnImplement(format!(
                    "Local database does not support '{:?}' table engine, table engine must be one of Parquet, JSONEachRow, Null, Memory or CSV",
                    plan.engine
                )));
            }
        };
        let table_meta = TableMeta::create(Arc::from(table), self.next_db_id());

        let mut lock = self.databases.write();
        let v = lock.get_mut(db_name);
        match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )));
            }
            Some((_, metas)) => {
                if metas.name2meta.get(table_name).is_some() {
                    if plan.if_not_exists {
                        return Ok(());
                    } else {
                        return Err(ErrorCode::UnImplement(format!(
                            "Table: '{}.{}' already exists.",
                            db_name, table_name,
                        )));
                    };
                }
                metas.insert(table_meta);
            }
        }

        Ok(())
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        let table_name = plan.table.as_str();

        let mut lock = self.databases.write();
        let v = lock.get(db_name);
        let tbl_id = match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some((_, metas)) => {
                let by_name = metas.name2meta.get(table_name);
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

        let v = lock.get_mut(db_name);
        match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some((_, metas)) => {
                metas.name2meta.remove(table_name);
                metas.id2meta.remove(&tbl_id);
            }
        }

        Ok(())
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        let lock = self.databases.read();
        let db = lock.get(db_name);
        match db {
            None => Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database: '{}'",
                db_name
            ))),
            Some((v, _)) => Ok(v.clone()),
        }
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        let mut res = vec![];
        let lock = self.databases.read();
        let values = lock.values();
        for (db, _) in values {
            res.push(db.clone());
        }
        Ok(res)
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

        let database = LocalDatabase::create(db_name, Arc::new(self.clone()));
        self.databases
            .write()
            .insert(plan.db, (Arc::new(database), InMemoryMetas::create()));
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

#[async_trait::async_trait]
impl MetaApi for LocalMetaBackend {
    async fn create_database(
        &mut self,
        _plan: CreateDatabasePlan,
    ) -> Result<CreateDatabaseActionResult> {
        todo!()
    }

    async fn get_database(&mut self, _db: &str) -> Result<GetDatabaseActionResult> {
        todo!()
    }

    async fn drop_database(&mut self, _plan: DropDatabasePlan) -> Result<DropDatabaseActionResult> {
        todo!()
    }

    async fn create_table(&mut self, _plan: CreateTablePlan) -> Result<CreateTableActionResult> {
        todo!()
    }

    async fn drop_table(&mut self, _plan: DropTablePlan) -> Result<DropTableActionResult> {
        todo!()
    }

    async fn get_table(&mut self, _db: String, _table: String) -> Result<GetTableActionResult> {
        todo!()
    }

    async fn get_table_ext(
        &mut self,
        _table_id: MetaId,
        _db_ver: Option<MetaVersion>,
    ) -> Result<GetTableActionResult> {
        todo!()
    }

    async fn get_database_meta(&mut self, _current_ver: Option<u64>) -> Result<DatabaseMetaReply> {
        todo!()
    }

    async fn commit_table(
        &self,
        _table_id: MetaId,
        _prev_snapshot: String,
        _new_snapshot: String,
    ) -> Result<CommitTableReply> {
        todo!()
    }
}
