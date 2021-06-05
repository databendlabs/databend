// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropDatabasePlan;
use common_planners::TableOptions;

use crate::configs::Config;
use crate::datasources::local::LocalDatabase;
use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::remote::RemoteTable;
use crate::datasources::system::SystemFactory;
use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

// Maintain all the databases of user.
pub struct DataSource {
    databases: RwLock<HashMap<String, Arc<dyn Database>>>,
    table_functions: RwLock<HashMap<String, Arc<dyn TableFunction>>>,
    remote_factory: RemoteFactory,
}

impl DataSource {
    pub fn try_create() -> Result<Self> {
        let conf = Config::default();
        DataSource::try_create_with_config(&conf)
    }

    pub fn try_create_with_config(conf: &Config) -> Result<Self> {
        let mut datasource = DataSource {
            databases: Default::default(),
            table_functions: Default::default(),
            remote_factory: RemoteFactory::new(conf),
        };

        datasource.register_system_database()?;
        datasource.register_local_database()?;
        datasource.register_default_database()?;
        datasource.register_remote_database()?;
        Ok(datasource)
    }

    fn insert_databases(&mut self, databases: Vec<Arc<dyn Database>>) -> Result<()> {
        let mut db_lock = self.databases.write();
        for database in databases {
            db_lock.insert(database.name().to_lowercase(), database.clone());
            for tbl_func in database.get_table_functions()? {
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
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register local database with Local engine.
    fn register_local_database(&mut self) -> Result<()> {
        let factory = LocalFactory::create();
        let databases = factory.load_databases()?;
        self.insert_databases(databases)
    }

    // Register remote database with Remote engine.
    fn register_remote_database(&mut self) -> Result<()> {
        let databases = self.remote_factory.load_databases()?;
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

impl DataSource {
    pub fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        let db_lock = self.databases.read();
        let database = db_lock.get(db_name).ok_or_else(|| {
            ErrorCodes::UnknownDatabase(format!("Unknown database: '{}'", db_name))
        })?;
        Ok(database.clone())
    }

    pub fn get_databases(&self) -> Result<Vec<String>> {
        let mut results = vec![];
        for (k, _v) in self.databases.read().iter() {
            results.push(k.clone());
        }
        Ok(results)
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        let db_lock = self.databases.read();
        let database = db_lock.get(db_name).ok_or_else(|| {
            ErrorCodes::UnknownDatabase(format!("Unknown database: '{}'", db_name))
        })?;

        let table = database.get_table(table_name)?;
        Ok(table.clone())
    }

    pub async fn get_remote_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        match self.get_table(db_name, table_name) {
            Ok(t) if t.is_local() => Err(ErrorCodes::LogicalError(format!(
                "local table {}.{} exists, which is used as remote",
                db_name, table_name
            ))),
            tbl @ Ok(_) => tbl,
            _ => {
                let cli_provider = self.remote_factory.store_client_provider();
                let mut store_cli = cli_provider.try_get_client().await?;
                let res = store_cli
                    .get_table(db_name.to_string(), table_name.to_string())
                    .await?;
                let remote_table = RemoteTable::try_create(
                    db_name.to_string(),
                    table_name.to_string(),
                    res.schema,
                    self.remote_factory.store_client_provider().clone(),
                    TableOptions::new(),
                )?;

                // Remote_table we've got here is NOT cached.
                //
                // Since we should solve the metadata synchronization problem in a more reasonable way,
                // let's postpone it until we have taken all the things into account.
                Ok(Arc::from(remote_table))
            }
        }
    }

    pub fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>> {
        let mut results = vec![];
        for (k, v) in self.databases.read().iter() {
            let tables = v.get_tables()?;
            for table in tables {
                results.push((k.clone(), table.clone()));
            }
        }
        Ok(results)
    }

    pub fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>> {
        let table_func_lock = self.table_functions.read();
        let table = table_func_lock.get(name).ok_or_else(|| {
            ErrorCodes::UnknownTableFunction(format!("Unknown table function: '{}'", name))
        })?;

        Ok(table.clone())
    }

    pub async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        if self.databases.read().get(db_name).is_some() {
            return if plan.if_not_exists {
                Ok(())
            } else {
                Err(ErrorCodes::UnknownDatabase(format!(
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
                Err(ErrorCodes::UnknownDatabase(format!(
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
