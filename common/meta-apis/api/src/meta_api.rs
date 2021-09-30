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

use common_exception::Result;
use common_meta_api_vo::CreateDatabaseReply;
use common_meta_api_vo::CreateTableReply;
use common_meta_api_vo::DatabaseInfo;
use common_meta_api_vo::DropDatabaseReply;
use common_meta_api_vo::DropTableReply;
use common_meta_api_vo::GetDatabasesReply;
use common_meta_api_vo::GetTablesReply;
use common_meta_api_vo::TableInfo;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

#[async_trait::async_trait]
pub trait MetaApi: Send + Sync {
    // database

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply>;

    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<DropDatabaseReply>;

    async fn get_database(&self, db: &str) -> Result<DatabaseInfo>;

    async fn get_databases(&self) -> Result<GetDatabasesReply>;

    // table

    async fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply>;

    async fn drop_table(&self, plan: DropTablePlan) -> Result<DropTableReply>;

    async fn get_table(&self, db: &str, table: &str) -> Result<TableInfo>;

    async fn get_tables(&self, db: &str) -> Result<GetTablesReply>;

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
        db_ver: Option<MetaVersion>,
    ) -> Result<TableInfo>;
}
