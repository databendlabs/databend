// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::utils::TableFunctionMeta;
use crate::catalogs::utils::TableMeta;
use crate::datasources::Database;

#[async_trait::async_trait]
pub trait Catalog {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;

    fn get_databases(&self) -> Result<Vec<String>>;

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>>;

    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>>;

    fn get_all_tables(&self) -> Result<Vec<(String, Arc<TableMeta>)>>;

    fn get_table_function(&self, name: &str) -> Result<Arc<TableFunctionMeta>>;

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
}
