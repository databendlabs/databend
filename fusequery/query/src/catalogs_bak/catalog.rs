// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;

use common_datavalues::prelude::Arc;
use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::TableEngineType;

use crate::datasources::DataSource;
use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

pub struct DatabaseMeta {
    id: u64,
    version: u64,
    name: String,
    engine: DatabaseEngineType,
    tables: HashMap<String, Arc<TableMeta>>,
}

impl DatabaseMeta {
    pub fn get_table(&self, tbl_name: &str) -> Result<Arc<TableMeta>> {
        todo!()
    }
    pub fn get_tables(&self) -> Vec<Arc<TableMeta>> {
        todo!()
    }
}

pub struct TableMeta {
    id: u64,
    version: u64,
    name: String,
    engine: TableEngineType,
}

#[async_trait::async_trait]
pub trait Catalog {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    fn get_databases(&self) -> Result<Vec<String>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;
    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>>;
    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>>;
}

pub trait DatabaseFactory {
    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
}

pub trait TableFactory {
    fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
    fn drop_table(&self, plan: DropTablePlan) -> Result<()>;
}
