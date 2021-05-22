// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::datasources::ITable;
use crate::datasources::ITableFunction;

#[async_trait::async_trait]
pub trait IDatabase: Sync + Send {
    /// Database name.
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn is_local(&self) -> bool;

    /// Get one table by name.
    fn get_table(&self, table_name: &str) -> Result<Arc<dyn ITable>>;

    /// Get all tables.
    fn get_tables(&self) -> Result<Vec<Arc<dyn ITable>>>;

    /// Get database table functions.
    fn get_table_functions(&self) -> Result<Vec<Arc<dyn ITableFunction>>>;

    /// DDL
    async fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
    async fn drop_table(&self, plan: DropTablePlan) -> Result<()>;
}
