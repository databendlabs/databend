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
use crate::catalogs::MetaBackend;
use crate::catalogs::TableFunctionMeta;
use crate::catalogs::TableMeta;

pub struct LocalDatabase {
    name: String,
    meta_backend: Arc<dyn MetaBackend>,
}

impl LocalDatabase {
    pub fn create(name: &str, meta_backend: Arc<dyn MetaBackend>) -> Self {
        LocalDatabase {
            name: name.to_string(),
            meta_backend,
        }
    }
}

impl Database for LocalDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn is_local(&self) -> bool {
        true
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        self.meta_backend.get_table(self.name(), table_name)
    }

    fn exists_table(&self, table_name: &str) -> Result<bool> {
        Ok(self.get_table(table_name).is_ok())
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        self.meta_backend
            .get_table_by_id(self.name(), table_id, table_version)
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        self.meta_backend.get_tables(self.name())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(vec![])
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        self.meta_backend.create_table(plan)
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.meta_backend.drop_table(plan)
    }
}
