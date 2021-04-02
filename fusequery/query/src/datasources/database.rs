// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_planners::CreateTablePlan;

use crate::datasources::{ITable, ITableFunction};

pub trait IDatabase: Sync + Send {
    // Database name.
    fn name(&self) -> &str;

    // Database engine.
    fn engine(&self) -> &str;

    // Get one table by name.
    fn get_table(&self, table_name: &str) -> Result<Arc<dyn ITable>>;

    // Get all tables.
    fn get_tables(&self) -> Result<Vec<Arc<dyn ITable>>>;

    // Get database table functions.
    fn get_table_functions(&self) -> Result<Vec<Arc<dyn ITableFunction>>>;

    // DDL
    fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
}
