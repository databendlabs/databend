// Copyright 2021 Datafuse Labs.
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
use common_infallible::RwLock;
use common_meta_types::TableInfo;
use nom::lib::std::collections::HashMap;

use crate::catalogs::Table;
use crate::sessions::QueryContext;
use crate::storages::csv::CsvTable;
use crate::storages::memory::MemoryTable;
use crate::storages::null::NullTable;
use crate::storages::StorageContext;

pub trait StorageEngine: Send + Sync {
    fn try_create(&self, ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>>;
}

impl<T> StorageEngine for T
where
    T: Fn(StorageContext, TableInfo) -> Result<Box<dyn Table>>,
    T: Send + Sync,
{
    fn try_create(&self, ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        self(ctx, table_info)
    }
}

#[derive(Default)]
pub struct StorageEngineFactory {
    engines: RwLock<HashMap<String, Arc<dyn StorageEngine>>>,
}

impl StorageEngineFactory {
    pub fn create(query_ctx: QueryContext) -> Self {
        let mut engines: HashMap<String, Arc<dyn StorageEngine>> = Default::default();

        if query_ctx.get_config().query.table_engine_csv_enabled {
            engines.insert("CSV".to_string(), Arc::new(CsvTable::try_create));
        }
        if query_ctx.get_config().query.table_engine_memory_enabled {
            engines.insert("MEMORY".to_string(), Arc::new(MemoryTable::try_create));
        }
        engines.insert("NULL".to_string(), Arc::new(NullTable::try_create));

        StorageEngineFactory {
            engines: RwLock::new(engines),
        }
    }

    pub fn get_table(&self, ctx: StorageContext, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine().to_uppercase();
        let lock = self.engines.read();
        let factory = lock.get(&engine).ok_or_else(|| {
            ErrorCode::UnknownTableEngine(format!("Unknown table engine {}", engine))
        })?;

        let table: Arc<dyn Table> = factory.try_create(ctx, table_info.clone())?.into();
        Ok(table)
    }
}
