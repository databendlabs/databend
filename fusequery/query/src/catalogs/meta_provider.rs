// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_runtime::tokio;
use futures::channel::mpsc::Sender;

use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

pub trait MetaProvider {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    fn get_databases(&self) -> Result<Vec<String>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;
    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>>;
    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>>;
}

pub struct InMemoryMetaProvider {
    databases: RwLock<HashMap<String, Arc<dyn Database>>>,
    table_functions: RwLock<HashMap<String, Arc<dyn TableFunction>>>,
}

impl MetaProvider for InMemoryMetaProvider {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        let db_lock = self.databases.read();
        let database = db_lock.get(db_name).ok_or_else(|| {
            ErrorCode::UnknownDatabase(format!("Unknown database: '{}'", db_name))
        })?;
        Ok(database.clone())
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        let mut results = vec![];
        for (k, _v) in self.databases.read().iter() {
            results.push(k.clone());
        }
        Ok(results)
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        let db_lock = self.databases.read();
        let database = db_lock.get(db_name).ok_or_else(|| {
            ErrorCode::UnknownDatabase(format!("Unknown database: '{}'", db_name))
        })?;

        let table = database.get_table(table_name)?;
        Ok(table.clone())
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>> {
        let mut results = vec![];
        for (k, v) in self.databases.read().iter() {
            let tables = v.get_tables()?;
            for table in tables {
                results.push((k.clone(), table.clone()));
            }
        }
        Ok(results)
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>> {
        let table_func_lock = self.table_functions.read();
        let table = table_func_lock.get(name).ok_or_else(|| {
            ErrorCode::UnknownTableFunction(format!("Unknown table function: '{}'", name))
        })?;

        Ok(table.clone())
    }
}

pub struct OverlaidProvider<U, L> {
    upper: Arc<U>,
    lower: Arc<L>,
}

impl<U, L> MetaProvider for OverlaidProvider<U, L>
where
    U: MetaProvider,
    L: MetaProvider,
{
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        todo!()
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        todo!()
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>> {
        todo!()
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>> {
        todo!()
    }
}

pub struct Synchronizer {
    last_snapshot: Arc<RwLock<Option<Arc<InMemoryMetaProvider>>>>,
}

pub async fn get_snapshot() -> Result<Arc<InMemoryMetaProvider>> {
    todo!()
}

pub struct RemoteMetaProvider {
    //receiver: Receiver<Arc<InMemoryMetaProvider>>,
}

impl RemoteMetaProvider {
    pub fn new() -> Self {
        todo!()
    }
    pub fn get_last_snapshot(&self) -> Result<Arc<InMemoryMetaProvider>> {
        todo!()
    }
}

impl MetaProvider for RemoteMetaProvider {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        self.get_last_snapshot()?.get_database(db_name)
    }

    fn get_databases(&self) -> Result<Vec<String>> {
        self.get_last_snapshot()?.get_databases()
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        self.get_last_snapshot()?.get_table(db_name, table_name)
    }

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>> {
        self.get_last_snapshot()?.get_all_tables()
    }

    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>> {
        self.get_last_snapshot()?.get_table_function(name)
    }
}
