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

use crate::catalogs::meta::TableFunctionMeta;
use crate::catalogs::meta::TableMeta;
use crate::catalogs::meta_client::MetaClient;
use crate::datasources::Database;

pub struct RemoteDatabase {
    _id: MetaId,
    name: String,
    meta_client: Arc<dyn MetaClient>,
}

impl RemoteDatabase {
    pub fn create_new(id: MetaId, name: impl Into<String>, cli: Arc<dyn MetaClient>) -> Self {
        Self {
            _id: id,
            name: name.into(),
            meta_client: cli,
        }
    }
}

#[async_trait::async_trait]
impl Database for RemoteDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn engine(&self) -> &str {
        "remote"
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        self.meta_client.get_table(&self.name, table_name)
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        self.meta_client
            .get_table_by_id(&self.name, table_id, table_version)
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        self.meta_client.get_db_tables(&self.name)
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(vec![])
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        self.meta_client.create_table(plan).await
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.meta_client.drop_table(plan).await
    }
}
