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
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropDatabasePlan;

use crate::catalogs::catalog::Catalog;
use crate::catalogs::impls::remote_meta_store_client::RemoteMetaStoreClient;
use crate::catalogs::meta_store_client::DBMetaStoreClient;
use crate::catalogs::utils::TableFunctionMeta;
use crate::catalogs::utils::TableMeta;
use crate::configs::Config;
use crate::datasources::local::LocalDatabase;
use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;
use crate::datasources::Database;

// min id for system tables (inclusive)
pub const SYS_TBL_ID_BEGIN: u64 = 1 << 62;
// max id for system tables (exclusive)
pub const SYS_TBL_ID_END: u64 = SYS_TBL_ID_BEGIN + 10000;

// min id for system tables (inclusive)
// max id for local tables is u64:MAX
pub const LOCAL_TBL_ID_BEGIN: u64 = SYS_TBL_ID_END;

// Maintain all the databases of user.
pub struct DatabaseCatalog {
    conf: Config,
    databases: RwLock<HashMap<String, Arc<dyn Database>>>,
    table_functions: RwLock<HashMap<String, Arc<TableFunctionMeta>>>,
    meta_store_cli: Arc<dyn DBMetaStoreClient>,
}

impl DatabaseCatalog {
    pub fn try_create() -> Result<Self> {
        let conf = Config::default();

        let remote_factory = RemoteFactory::new(&conf);
        let store_client_provider = remote_factory.store_client_provider();
        let cli = Arc::new(RemoteMetaStoreClient::create(Arc::new(
            store_client_provider,
        )));
        Self::try_create_with_config(conf, cli)
    }

    pub fn try_create_with_config(
        conf: Config,
        meta_store_cli: Arc<dyn DBMetaStoreClient>,
    ) -> Result<Self> {
        let mut datasource = DatabaseCatalog {
            conf,
            databases: Default::default(),
            table_functions: Default::default(),
            meta_store_cli,
        };

        datasource.register_system_database()?;
        datasource.register_local_database()?;
        datasource.register_default_database()?;
        Ok(datasource)
    }

    fn insert_databases(&mut self, databases: Vec<Arc<dyn Database>>) -> Result<()> {
        let mut db_lock = self.databases.write();
        for database in databases {
            db_lock.insert(database.name().to_lowercase(), database.clone());
            for tbl_func in database.get_table_functions()? {
                self.table_functions
                    .write()
                    .insert(tbl_func.datasource().name().to_string(), tbl_func.clone());
            }
        }
        Ok(())
    }

    // Register local database with System engine.
    fn register_system_database(&mut self) -> Result<()> {
        let factory = SystemFactory::create();
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register local database with Local engine.
    fn register_local_database(&mut self) -> Result<()> {
        let factory = LocalFactory::create();
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register default database with Local engine.
    fn register_default_database(&mut self) -> Result<()> {
        let default_db = LocalDatabase::create();
        self.databases
            .write()
            .insert("default".to_string(), Arc::new(default_db));
        Ok(())
    }
}

#[async_trait::async_trait]
impl Catalog for DatabaseCatalog {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        self.databases.read().get(db_name).map_or_else(
            || {
                if !self.conf.store.store_address.is_empty() {
                    self.meta_store_cli.get_database(db_name)
                } else {
                    Err(ErrorCode::UnknownDatabase(format!(
                        "Unknown database {}",
                        &db_name
                    )))
                }
            },
            |v| Ok(v.clone()),
        )
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        let mut databases = vec![];

        // Local databases.
        let locals = self.databases.read();
        databases.extend(locals.keys().into_iter().cloned());

        // Remote databases.
        if !self.conf.store.store_address.is_empty() {
            let remotes = self.meta_store_cli.get_databases()?;
            databases.extend(remotes.into_iter());
        }

        // Sort.
        databases.sort();
        Ok(databases)
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let database = self.get_database(db_name)?;
        let table = database.get_table(table_name)?;
        Ok(table.clone())
    }

    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        let database = self.get_database(db_name)?;
        let table = database.get_table_by_id(table_id, table_version)?;
        Ok(table.clone())
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>> {
        let mut results = vec![];
        let mut db_names = HashSet::new();
        for (db_name, v) in self.databases.read().iter() {
            let tables = v.get_tables()?;
            for table in tables {
                results.push((db_name.clone(), table.clone()));
            }
            db_names.insert(db_name.clone());
        }

        if !self.conf.store.store_address.is_empty() {
            let mut remotes = self
                .meta_store_cli
                .get_all_tables()?
                .into_iter()
                // local and system dbs should shadow remote db
                .filter(|(n, _)| !db_names.contains(n))
                .collect::<Vec<_>>();
            results.append(&mut remotes);
        }

        Ok(results)
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<TableFunctionMeta>> {
        let table_func_lock = self.table_functions.read();
        let table = table_func_lock.get(name).ok_or_else(|| {
            ErrorCode::UnknownTableFunction(format!("Unknown table function: '{}'", name))
        })?;
        // no function of remote database for the time being
        Ok(table.clone())
    }

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        if self.databases.read().get(db_name).is_some() {
            return if plan.if_not_exists {
                Ok(())
            } else {
                Err(ErrorCode::UnknownDatabase(format!(
                    "Database: '{}' already exists.",
                    plan.db
                )))
            };
        }

        match plan.engine {
            DatabaseEngineType::Local => {
                let database = LocalDatabase::create();
                self.databases.write().insert(plan.db, Arc::new(database));
            }
            DatabaseEngineType::Remote => {
                self.meta_store_cli.create_database(plan).await?;
            }
        }
        Ok(())
    }

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        let db = self.get_database(db_name);
        let database = match db {
            Err(_) => {
                return if plan.if_exists {
                    Ok(())
                } else {
                    Err(ErrorCode::UnknownDatabase(format!(
                        "Unknown database: '{}'",
                        plan.db
                    )))
                }
            }
            Ok(v) => v,
        };

        if database.is_local() {
            self.databases.write().remove(db_name);
        } else {
            self.meta_store_cli.drop_database(plan).await?;
        };

        Ok(())
    }
}
