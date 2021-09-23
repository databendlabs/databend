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

use common_exception::Result;

use crate::catalogs::meta_backend::DatabaseInfo;
use crate::catalogs::meta_backend::MetaBackend;
use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::common::StoreApiProvider;
use crate::configs::Config;
use crate::datasources::database::default::default_database::DefaultDatabase;
use crate::datasources::table_engine_registry::TableEngineRegistry;

/// Default database engine, which
/// - creates tables by using TableFactory
/// - keeps metadata in the given meta_backend
pub struct DefaultDatabaseFactory {
    meta_backend: Arc<dyn MetaBackend>,
    table_factory_registry: Arc<TableEngineRegistry>,
}

impl DefaultDatabaseFactory {
    pub fn new(
        meta_backend: Arc<dyn MetaBackend>,
        table_factory_registry: Arc<TableEngineRegistry>,
    ) -> Self {
        Self {
            meta_backend,
            table_factory_registry,
        }
    }
}

impl DatabaseEngine for DefaultDatabaseFactory {
    fn create(&self, conf: &Config, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        let client_provider = StoreApiProvider::new(conf);
        let db = DefaultDatabase::new(
            &db_info.name,
            &db_info.engine,
            self.meta_backend.clone(),
            self.table_factory_registry.clone(),
            client_provider,
        );
        Ok(Arc::new(db))
    }

    fn description(&self) -> String {
        format!("default database engine, with {}", self.meta_backend.name())
    }
}
