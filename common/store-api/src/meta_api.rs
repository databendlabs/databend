// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_datavalues::DataSchemaRef;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseActionResult {
    pub database_id: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetDatabaseActionResult {
    pub database_id: i64,
    pub db: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropDatabaseActionResult {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateTableActionResult {
    pub table_id: i64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropTableActionResult {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableActionResult {
    pub table_id: i64,
    pub db: String,
    pub name: String,
    pub schema: DataSchemaRef,
}

#[async_trait::async_trait]
pub trait MetaApi: Sync + Send {
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
}
