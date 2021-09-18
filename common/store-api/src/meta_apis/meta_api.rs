//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;

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
    pub engine: String,
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
    pub engine: String,
    pub options: HashMap<String, String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DatabaseMetaSnapshot {
    pub meta_ver: u64,
    pub db_metas: Vec<(String, Database)>,
    pub tbl_metas: Vec<(u64, Table)>,
}
pub type DatabaseMetaReply = Option<DatabaseMetaSnapshot>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum CommitTableReply {
    // done
    Success,
    // recoverable, returns the current snapshot-id, which should be merged with
    Conflict(String),
    // fatal, not recoverable, returns the current snapshot-id
    Failure(String),
}

#[async_trait::async_trait]
pub trait MetaApi: Send + Sync {
    async fn create_database(
        &self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseActionResult>;

    async fn get_database(&self, db: &str) -> common_exception::Result<GetDatabaseActionResult>;

    async fn drop_database(
        &self,
        plan: DropDatabasePlan,
    ) -> common_exception::Result<DropDatabaseActionResult>;

    async fn create_table(
        &self,
        plan: CreateTablePlan,
    ) -> common_exception::Result<CreateTableActionResult>;

    async fn drop_table(
        &self,
        plan: DropTablePlan,
    ) -> common_exception::Result<DropTableActionResult>;

    async fn get_table(
        &self,
        db: String,
        table: String,
    ) -> common_exception::Result<GetTableActionResult>;

    async fn get_table_ext(
        &self,
        table_id: MetaId,
        db_ver: Option<MetaVersion>,
    ) -> common_exception::Result<GetTableActionResult>;

    async fn get_database_meta(
        &self,
        current_ver: Option<u64>,
    ) -> common_exception::Result<DatabaseMetaReply>;
}
