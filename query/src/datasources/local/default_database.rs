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
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::Database;
use crate::catalogs::TableFunctionMeta;
use crate::catalogs::TableMeta;
use crate::datasources::local::LocalDatabase;

// Default database.
pub struct DefaultDatabase {
    local: LocalDatabase,
}

impl DefaultDatabase {
    pub fn create() -> Self {
        DefaultDatabase {
            local: LocalDatabase::create(),
        }
    }
}

#[async_trait::async_trait]
impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        "default"
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn is_local(&self) -> bool {
        true
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        self.local.get_table(table_name)
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        self.local.get_table_by_id(table_id, table_version)
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        self.local.get_tables()
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        self.local.get_table_functions()
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        self.local.create_table(plan).await
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.local.drop_table(plan).await
    }
}
