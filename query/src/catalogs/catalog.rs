// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_exception::Result;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Database;
use crate::catalogs::TableFunctionMeta;
use crate::catalogs::TableMeta;

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
