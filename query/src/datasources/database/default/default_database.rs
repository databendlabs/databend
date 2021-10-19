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

use common_context::TableDataContext;
use common_dal::InMemoryData;
use common_exception::ErrorCode;
use common_infallible::RwLock;
use common_meta_types::TableInfo;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::backends::MetaApiSync;
use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::datasources::table_engine_registry::TableEngineRegistry;

pub struct DefaultDatabase {
    db_name: String,
    meta: Arc<dyn MetaApiSync>,
    table_factory_registry: Arc<TableEngineRegistry>,
    in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
}

impl DefaultDatabase {
    pub fn new(
        db_name: impl Into<String>,
        meta: Arc<dyn MetaApiSync>,
        table_factory_registry: Arc<TableEngineRegistry>,
        in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
    ) -> Self {
        Self {
            db_name: db_name.into(),
            meta,
            table_factory_registry,
            in_memory_data,
        }
    }

    fn build_table_instance(
        &self,
        table_info: &TableInfo,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let engine = &table_info.engine;
        let factory = self
            .table_factory_registry
            .get_table_factory(engine)
            .ok_or_else(|| {
                ErrorCode::UnknownTableEngine(format!("unknown table engine {}", engine))
            })?;

        let tbl: Arc<dyn Table> = factory
            .try_create(
                table_info.clone(),
                Arc::new(TableDataContext {
                    in_memory_data: self.in_memory_data.clone(),
                }),
            )?
            .into();

        Ok(tbl)
    }
}

impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        &self.db_name
    }

    fn get_tables(&self) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        unimplemented!();
    }

    fn create_table(&self, plan: CreateTablePlan) -> common_exception::Result<()> {
        // TODO validate table parameters by using TableFactory
        self.meta.create_table(plan)?;
        Ok(())
    }

    fn drop_table(&self, plan: DropTablePlan) -> common_exception::Result<()> {
        self.meta.drop_table(plan)
    }
}
