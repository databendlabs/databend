// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_datavalues::DataSchemaRef;
use common_metatypes::Database;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_metatypes::Table;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseActionResult {
    pub database_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetDatabaseActionResult {
    pub database_id: u64,
    pub db: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropDatabaseActionResult {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateTableActionResult {
    pub table_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropTableActionResult {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableActionResult {
    pub table_id: u64,
    pub db: String,
    pub name: String,
    pub schema: DataSchemaRef,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DatabaseMetaSnapshot {
    pub meta_ver: u64,
    pub db_metas: Vec<(String, Database)>,
    pub tbl_metas: Vec<(u64, Table)>,
}
pub type DatabaseMetaReply = Option<DatabaseMetaSnapshot>;

#[async_trait::async_trait]
pub trait MetaApi {
    async fn create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseActionResult>;

    async fn get_database(&mut self, db: &str)
        -> common_exception::Result<GetDatabaseActionResult>;

    async fn drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> common_exception::Result<DropDatabaseActionResult>;

    async fn create_table(
        &mut self,
        plan: CreateTablePlan,
    ) -> common_exception::Result<CreateTableActionResult>;

    async fn drop_table(
        &mut self,
        plan: DropTablePlan,
    ) -> common_exception::Result<DropTableActionResult>;

    async fn get_table(
        &mut self,
        db: String,
        table: String,
    ) -> common_exception::Result<GetTableActionResult>;

    async fn get_table_ext(
        &mut self,
        table_id: MetaId,
        db_ver: Option<MetaVersion>,
    ) -> common_exception::Result<GetTableActionResult>;

    async fn get_database_meta(
        &mut self,
        current_ver: Option<u64>,
    ) -> common_exception::Result<DatabaseMetaReply>;
}
