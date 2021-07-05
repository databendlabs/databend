// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::Arc;
use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

#[async_trait::async_trait]
pub trait Catalog {
    async fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    async fn get_databases(&self) -> Result<Vec<String>>;
    async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;
    async fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>>;
    async fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>>;

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
}
