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

use std::sync::Arc;

use common_exception::Result;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

#[async_trait::async_trait]
pub trait MetaApi: Send + Sync {
    // database

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply>;

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;

    async fn get_database(&self, db: &str) -> Result<Arc<DatabaseInfo>>;

    async fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>>;

    // table

    async fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply>;

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()>;

    async fn get_table(&self, db: &str, table: &str) -> Result<Arc<TableInfo>>;

    async fn get_tables(&self, db: &str) -> Result<Vec<Arc<TableInfo>>>;

    async fn get_table_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)>;

    async fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        option_key: String,
        option_value: String,
    ) -> Result<UpsertTableOptionReply>;

    fn name(&self) -> String;
}
