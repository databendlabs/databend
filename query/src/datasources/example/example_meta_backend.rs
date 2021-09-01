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
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::catalogs::TableMeta;
use crate::datasources::example::example_table::ExampleTable;
use crate::datasources::example::ExampleDatabase;
use crate::datasources::MetaBackend;

type Databases = Arc<RwLock<HashMap<String, (Arc<dyn Database>, Vec<Arc<dyn Table>>)>>>;

/// The backend of the local database.
/// Maintainer all the database and table information.
#[derive(Clone)]
pub struct ExampleMetaBackend {
    databases: Databases,
}

impl ExampleMetaBackend {
    pub fn create() -> Self {
        ExampleMetaBackend {
            databases: Arc::new(Default::default()),
        }
    }
}

impl MetaBackend for ExampleMetaBackend {
    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        let lock = self.databases.read();
        let v = lock.get(db_name);
        match v {
            None => Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database: {}",
                db_name
            ))),
            Some((_, tables)) => {
                if !tables.is_empty() {
                    Ok(Arc::new(TableMeta::create(tables[0].clone(), 0)))
                } else {
                    Err(ErrorCode::UnknownTable(format!(
                        "Unknown table: '{}'",
                        table_id
                    )))
                }
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
            Some((_, tables)) => {
                for table in tables {
                    if table.name() == table_name {
                        return Ok(Arc::new(TableMeta::create(table.clone(), 0)));
                    }
                }
                return Err(ErrorCode::UnknownTable(format!(
                    "Unknown table: '{}'",
                    table_name
                )));
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
            Some((_, tables)) => {
                for table in tables {
                    let meta = Arc::new(TableMeta::create(table.clone(), 0));
                    res.push(meta);
                }
            }
        }
        Ok(res)
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let clone = plan.clone();
        let db_name = clone.db.as_str();
        let table_name = clone.table.as_str();
        let table = ExampleTable::try_create(
            db_name.to_string(),
            table_name.to_string(),
            plan.schema,
            plan.options,
        )?;

        let mut lock = self.databases.write();
        let v = lock.get_mut(db_name);
        match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )));
            }
            Some((_, tables)) => {
                // TODO(bohu): check the table is exists or not.
                tables.push(Arc::from(table));
            }
        }
        Ok(())
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        let table_name = plan.table.as_str();

        let mut lock = self.databases.write();
        let v = lock.get_mut(db_name);
        match v {
            None => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: {}",
                    db_name
                )))
            }
            Some((_, tables)) => {
                // TODO(bohu): check the table is exists or not.
                if let Some(idx) = tables.iter().position(|x| x.name() == table_name) {
                    tables.remove(idx);
                }
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

        let database = ExampleDatabase::create(db_name, Arc::new(self.clone()));
        self.databases
            .write()
            .insert(plan.db, (Arc::new(database), vec![]));
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
