// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
// use anyhow::Result;
use common_exception::{Result, ErrorCodes};
use common_flights::StoreClient;
use common_infallible::RwLock;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;

use crate::configs::Config;
use crate::datasources::local::LocalDatabase;
use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;
use crate::datasources::IDatabase;
use crate::datasources::ITable;
use crate::datasources::ITableFunction;

#[async_trait::async_trait]
pub trait IDataSource: Sync + Send {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn IDatabase>>;
    fn get_databases(&self) -> Result<Vec<String>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>>;
    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn ITable>)>>;
    fn get_table_function(&self, name: &str) -> Result<Arc<dyn ITableFunction>>;
    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
}

// Maintain all the databases of user.
pub struct DataSource {
    conf: Config,
    databases: RwLock<HashMap<String, Arc<dyn IDatabase>>>,
    table_functions: RwLock<HashMap<String, Arc<dyn ITableFunction>>>,
    store_client: RwLock<Option<StoreClient>>
}

impl DataSource {
    pub fn try_create() -> Result<Self> {
        let mut datasource = DataSource {
            conf: Config::default(),
            databases: Default::default(),
            table_functions: Default::default(),
            store_client: RwLock::new(None)
        };

        datasource.register_system_database()?;
        datasource.register_local_database()?;
        datasource.register_default_database()?;
        datasource.register_remote_database()?;
        Ok(datasource)
    }

    pub fn try_create_with_config(conf: Config) -> Result<Self> {
        let mut ds = Self::try_create()?;
        ds.conf = conf;
        Ok(ds)
    }

    async fn try_get_client(&self) -> Result<StoreClient> {
        if self.store_client.read().is_none() {
            let store_addr = self.conf.store_api_address.clone();
            let username = self.conf.store_api_username.clone();
            let password = self.conf.store_api_password.clone();
            let client = StoreClient::try_create(&store_addr, &username, &password).await.map_err(ErrorCodes::from_anyhow)?;
            *self.store_client.write() = Some(client);
        }
        Ok(self.store_client.read().as_ref().unwrap().clone())
    }

    fn insert_databases(&mut self, databases: Vec<Arc<dyn IDatabase>>) -> Result<()> {
        let mut db_lock = self.databases.write();
        for database in databases {
            db_lock.insert(database.name().to_lowercase(), database.clone());
            for tbl_func in database.get_table_functions().map_err(ErrorCodes::from_anyhow)? {
                self.table_functions
                    .write()
                    .insert(tbl_func.name().to_string(), tbl_func.clone());
            }
        }
        Ok(())
    }

    // Register local database with System engine.
    fn register_system_database(&mut self) -> Result<()> {
        let factory = SystemFactory::create();
        let databases = factory.load_databases().map_err(ErrorCodes::from_anyhow)?;
        self.insert_databases(databases)
    }

    // Register local database with Local engine.
    fn register_local_database(&mut self) -> Result<()> {
        let factory = LocalFactory::create();
        let databases = factory.load_databases().map_err(ErrorCodes::from_anyhow)?;
        self.insert_databases(databases)
    }

    // Register remote database with Remote engine.
    fn register_remote_database(&mut self) -> Result<()> {
        let factory = RemoteFactory::create(self.conf.clone());
        let databases = factory.load_databases().map_err(ErrorCodes::from_anyhow)?;
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
impl IDataSource for DataSource {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn IDatabase>> {
        let db_lock = self.databases.read();
        let database = db_lock
            .get(db_name)
            .ok_or_else(|| {
                ErrorCodes::UnknownDatabase(
                    format!("DataSource Error: Unknown database: '{}'", db_name)
                )
            })?;
        Ok(database.clone())
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>> {
        let db_lock = self.databases.read();
        let database = db_lock
            .get(db_name)
            .ok_or_else(|| {
                ErrorCodes::UnknownDatabase(
                    format!("DataSource Error: Unknown database: '{}'", db_name)
                )
            })?;

        let table = database.get_table(table_name).map_err(ErrorCodes::from_anyhow)?;
        Ok(table.clone())
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        let mut results = vec![];
        for (k, _v) in self.databases.read().iter() {
            results.push(k.clone());
        }
        Ok(results)
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn ITable>)>> {
        let mut results = vec![];
        for (k, v) in self.databases.read().iter() {
            let tables = v.get_tables().map_err(ErrorCodes::from_anyhow)?;
            for table in tables {
                results.push((k.clone(), table.clone()));
            }
        }
        Ok(results)
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn ITableFunction>> {
        let table_func_lock = self.table_functions.read();
        let table = table_func_lock
            .get(name)
            .ok_or_else(|| {
                ErrorCodes::UnknownTableFunction(
                    format!("DataSource Error: Unknown table function: '{}'", name)
                )
            })?;

        Ok(table.clone())
    }

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        match plan.engine {
            DatabaseEngineType::Local => {
                let database = LocalDatabase::create();
                self.databases.write().insert(plan.db, Arc::new(database));
            }
            DatabaseEngineType::Remote => {
                let mut client = self.try_get_client().await?;
                let _action_rst = client.create_database(plan.clone()).await.map_err(ErrorCodes::from_anyhow)?;
                // TODO add db id to cache

                // Add local cache.
                let database = RemoteDatabase::create(self.conf.clone(), plan.db.clone());
                self.databases
                    .write()
                    .insert(plan.db.clone(), Arc::new(database));
            }
        }
        Ok(())
    }
}
