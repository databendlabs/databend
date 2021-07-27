// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalog::utils::TableFunctionMeta;
use crate::catalog::utils::TableMeta;

#[async_trait::async_trait]
pub trait Database: Sync + Send {
    /// Database name.
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn is_local(&self) -> bool;

    /// Get one table by name.
    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>>;

    /// Get table by meta id
    fn get_table_by_id(
        &self,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>>;

    /// Get all tables.
    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>>;

    /// Get database table functions.
    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>>;

    /// DDL
    async fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
    async fn drop_table(&self, plan: DropTablePlan) -> Result<()>;
}
