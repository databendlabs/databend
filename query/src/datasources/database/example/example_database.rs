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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api_vo::TableInfo;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::meta_backend::MetaBackend;
use crate::catalogs::Database;
use crate::catalogs::TableMeta;
use crate::datasources::database::example::ExampleTable;

pub struct ExampleDatabase {
    db_name: String,
    engine_name: String,
    meta_store_client: Arc<dyn MetaBackend>,
}
const EXAMPLE_TBL_ENGINE: &str = "ExampleNull";

impl ExampleDatabase {
    pub fn new(
        db_name: impl Into<String>,
        engine_name: impl Into<String>,
        meta_store_client: Arc<dyn MetaBackend>,
    ) -> Self {
        Self {
            db_name: db_name.into(),
            engine_name: engine_name.into(),
            meta_store_client,
        }
    }

    fn build_table_instance(
        &self,
        table_info: &TableInfo,
    ) -> common_exception::Result<Arc<TableMeta>> {
        let engine = &table_info.engine;
        if !engine.is_empty() && engine != EXAMPLE_TBL_ENGINE {
            return Err(ErrorCode::UnknownDatabaseEngine(format!(
                "table engine {} not supported by example database, (supported table engine: {})",
                engine, EXAMPLE_TBL_ENGINE,
            )));
        }

        let tbl = ExampleTable::try_create(
            table_info.db.clone(),
            table_info.name.clone(),
            table_info.schema.clone(),
            table_info.table_option.clone(),
            table_info.table_id,
        )?;
        let tbl_meta = TableMeta::create(tbl.into(), table_info.table_id);
        Ok(Arc::new(tbl_meta))
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

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        let db_name = self.name();
        let table_info = self.meta_store_client.get_table(db_name, table_name)?;
        self.build_table_instance(table_info.as_ref())
    }

    fn get_table_by_id(
        &self,
        _table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        todo!()
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        let table_infos = self.meta_store_client.get_tables(self.name())?;
        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.build_table_instance(item)?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        self.meta_store_client.create_table(plan)
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.meta_store_client.drop_table(plan)
    }
}
