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
use crate::datasources::engines::metastore_clients::MetaStoreClient;

pub struct ExampleDatabase {
    db_name: String,
    engine_name: String,
}

impl ExampleDatabase {
    pub fn new(
        db_name: impl Into<String>,
        engine_name: impl Into<String>,
        _meta_store_client: Arc<dyn MetaStoreClient>,
    ) -> Self {
        Self {
            db_name: db_name.into(),
            engine_name: engine_name.into(),
        }
    }
}

impl Database for ExampleDatabase {
    fn name(&self) -> &str {
        self.db_name.as_str()
    }

    fn engine(&self) -> &str {
        self.engine_name.as_str()
    }

    fn is_local(&self) -> bool {
        true
    }

    fn get_table(&self, _table_name: &str) -> Result<Arc<TableMeta>> {
        todo!()
    }

    fn exists_table(&self, _table_name: &str) -> Result<bool> {
        todo!()
    }

    fn get_table_by_id(
        &self,
        _table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        todo!()
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        todo!()
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(vec![])
    }

    fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        todo!()
    }

    fn drop_table(&self, _plan: DropTablePlan) -> Result<()> {
        todo!()
    }
}
