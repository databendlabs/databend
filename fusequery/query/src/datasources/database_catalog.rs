// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.


use std::collections::BTreeSet;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;


use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropDatabasePlan;


use common_store_api::MetaApi;


use crate::configs::Config;
use crate::datasources::local::LocalDatabase;
use crate::datasources::local::LocalFactory;
use crate::datasources::remote::meta_synchronizer::Synchronizer;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::remote::RemoteFactory;

use crate::datasources::system::SystemFactory;
use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;


pub type MetaId = u64;
pub type MetaVersion = u64;

pub struct DatasourceWrapper<T> {
    inner: T,
    id: MetaId,
    version: Option<MetaVersion>,
}

pub type TableMeta = DatasourceWrapper<Arc<dyn Table>>;
pub type TableFunctionMeta = DatasourceWrapper<Arc<dyn TableFunction>>;

impl<T> DatasourceWrapper<T> {
    pub fn with_id(inner: T, id: MetaId) -> Arc<DatasourceWrapper<T>> {
        Arc::new(DatasourceWrapper {
            inner,
            id,
            version: None,
        })
    }
}

impl<T> DatasourceWrapper<T> {
    pub fn get_id(&self) -> MetaId {
        self.id
    }

    pub fn get_version(&self) -> Option<MetaVersion> {
        self.version
    }

    pub fn get_inner(&self) -> &T {
        &self.inner
    }
}

// Maintain all the databases of user.
pub struct DatabaseCatalog {
    databases: RwLock<HashMap<String, Arc<dyn Database>>>,
    table_functions: RwLock<HashMap<String, Arc<TableFunctionMeta>>>,
    remote_factory: RemoteFactory,
    meta_store_syncer: Synchronizer,
}

impl DatabaseCatalog {
    pub fn try_create() -> Result<Self> {
        let conf = Config::default();
        DatabaseCatalog::try_create_with_config(&conf)
    }

    pub fn try_create_with_config(conf: &Config) -> Result<Self> {
        let mut datasource = DatabaseCatalog {
            databases: Default::default(),
            table_functions: Default::default(),
            remote_factory: RemoteFactory::new(conf),
            meta_store_syncer: Synchronizer::new(),
        };

        datasource.register_default_database()?;
        datasource.register_local_database()?;
        datasource.register_system_database()?;
        //datasource.register_remote_database()?;
        Ok(datasource)
    }

    fn insert_databases(&mut self, databases: Vec<Arc<dyn Database>>) -> Result<()> {
        let mut db_lock = self.databases.write();
        for database in databases {
            db_lock.insert(database.name().to_lowercase(), database.clone());
            for tbl_func in database.get_table_functions()? {
                self.table_functions
                    .write()
                    .insert(tbl_func.get_inner().name().to_string(), tbl_func.clone());
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

impl DatabaseCatalog {
    pub fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        let db_lock = self.databases.read();
        db_lock
            .get(db_name)
            .map(Clone::clone)
            .ok_or_else(|| ErrorCode::UnknownDatabase(format!("Unknown database: '{}'", db_name)))
            .or_else(|_| self.meta_store_syncer.get_database(db_name))
    }

    pub fn get_databases(&self) -> Result<Vec<String>> {
        let mut results = vec![];
        for (k, _v) in self.databases.read().iter() {
            results.push(k.clone());
        }
        let local_db_names = BTreeSet::from_iter(results);
        let meta_store_dbs = self.meta_store_syncer.get_databases()?;
        let meta_store_db_names =
            BTreeSet::from_iter(meta_store_dbs.iter().map(|db| db.name().to_owned()));
        let db_names = meta_store_db_names.union(&local_db_names);
        let mut res = db_names.map(String::to_owned).collect::<Vec<_>>();
        res.sort();
        Ok(res)
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        let database = self.get_database(db_name)?;
        database
            .get_table(table_name)
            .or_else(|_e| self.meta_store_syncer.get_table(db_name, table_name))
    }

    pub fn get_table_by_id(
        &self,
        _tbl_id: MetaId,
        _tbl_ver: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        todo!()
    }

    pub fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>> {
        let mut results = vec![];
        for (k, v) in self.databases.read().iter() {
            let tables = v.get_tables()?;
            for table in tables {
                results.push((k.clone(), table.clone()));
            }
        }

        let meta_store_info = self.meta_store_syncer.get_databases()?;
        for db in meta_store_info.iter() {
            let tables = db.get_tables()?;
            for table in tables {
                results.push((db.name().to_owned(), table.clone()));
            }
        }

        Ok(results)
    }

    pub fn get_table_function(&self, name: &str) -> Result<Arc<TableFunctionMeta>> {
        let table_func_lock = self.table_functions.read();
        let table = table_func_lock.get(name).ok_or_else(|| {
            ErrorCode::UnknownTableFunction(format!("Unknown table function: '{}'", name))
        })?;

        Ok(table.clone())
    }

    pub async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
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
                let mut client = self
                    .remote_factory
                    .store_client_provider()
                    .try_get_client()
                    .await?;
                client.create_database(plan.clone()).await.map(|_| {
                    let database = RemoteDatabase::create(
                        self.remote_factory.store_client_provider(),
                        plan.db.clone(),
                    );
                    self.databases
                        .write()
                        .insert(plan.db.clone(), Arc::new(database));
                })?;
            }
        }
        Ok(())
    }

    pub async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        if self.databases.read().get(db_name).is_none() {
            return if plan.if_exists {
                Ok(())
            } else {
                Err(ErrorCode::UnknownDatabase(format!(
                    "Unknown database: '{}'",
                    plan.db
                )))
            };
        }

        let database = self.get_database(db_name)?;
        if database.is_local() {
            self.databases.write().remove(db_name);
        } else {
            let mut client = self
                .remote_factory
                .store_client_provider()
                .try_get_client()
                .await?;
            client.drop_database(plan.clone()).await.map(|_| {
                self.databases.write().remove(plan.db.as_str());
            })?;
        };

        Ok(())
    }
}
