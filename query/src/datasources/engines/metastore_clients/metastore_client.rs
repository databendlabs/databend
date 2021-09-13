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

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::TableOptions;

#[derive(Debug)]
pub struct TableInfo {
    pub db: String,
    pub table_id: u64,
    pub name: String,
    pub schema: DataSchemaRef,
    pub engine: String,
    pub table_option: TableOptions,
}

#[derive(Clone)]
pub struct DatabaseInfo {
    pub name: String,
    pub engine: String,
}

/// TODO rename to MetaBackend after refactoring
pub trait MetaStoreClient: Send + Sync {
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>>;
    fn exist_table(&self, db_name: &str, table_name: &str) -> Result<bool>;
    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableInfo>>;

    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>>;

    fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>>;

    fn exists_database(&self, db_name: &str) -> Result<bool>;

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>>;

    fn create_table(&self, plan: CreateTablePlan) -> Result<()>;

    fn drop_table(&self, plan: DropTablePlan) -> Result<()>;

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;
    fn name(&self) -> String;
}
