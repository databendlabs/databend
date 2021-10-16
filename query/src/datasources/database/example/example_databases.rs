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
use common_meta_types::DatabaseInfo;

use crate::catalogs::backends::MetaApiSync;
use crate::catalogs::backends::MetaEmbeddedSync;
use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::configs::Config;
use crate::datasources::database::example::ExampleDatabase;

pub struct ExampleDatabaseEngine {
    meta: Arc<dyn MetaApiSync>,
}

impl ExampleDatabaseEngine {
    pub fn create() -> Self {
        let meta = Arc::new(MetaEmbeddedSync::create());
        ExampleDatabaseEngine { meta }
    }
}

impl DatabaseEngine for ExampleDatabaseEngine {
    fn create(&self, _conf: &Config, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        let db = ExampleDatabase::new(&db_info.db, &db_info.engine, self.meta.clone());
        Ok(Arc::new(db))
    }

    fn description(&self) -> String {
        "The example engine is used by example databases and tables.".to_owned()
    }
}
