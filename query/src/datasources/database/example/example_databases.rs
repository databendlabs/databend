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

use crate::catalogs::Database;
use crate::configs::Config;
use crate::datasources::database::example::ExampleDatabase;
use crate::datasources::engines::database_factory::DatabaseFactory;
use crate::datasources::engines::metastore_clients::DatabaseInfo;
use crate::datasources::engines::metastore_clients::EmbeddedMetaStore;
use crate::datasources::engines::metastore_clients::MetaStoreClient;

/// The collection of the local database.
pub struct ExampleDatabases {
    meta_backend: Arc<dyn MetaStoreClient>,
}

impl ExampleDatabases {
    pub fn create() -> Self {
        let meta_backend = Arc::new(EmbeddedMetaStore::new());
        ExampleDatabases { meta_backend }
    }
}

impl DatabaseFactory for ExampleDatabases {
    fn create(&self, _conf: &Config, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        let db = ExampleDatabase::new(&db_info.name, &db_info.engine, self.meta_backend.clone());
        Ok(Arc::new(db))
    }

    fn description(&self) -> String {
        "The example engine is used by example databases and tables.".to_owned()
    }
}
