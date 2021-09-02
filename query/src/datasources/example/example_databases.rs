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
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::configs::Config;
use crate::datasources::example::example_meta_backend::ExampleMetaBackend;
use crate::datasources::MetaBackend;

/// The collection of the local database.
pub struct ExampleDatabases {
    meta_backend: Arc<dyn MetaBackend>,
}

impl ExampleDatabases {
    pub fn create(_conf: Config) -> Self {
        let meta_backend = Arc::new(ExampleMetaBackend::create());
        ExampleDatabases { meta_backend }
    }
}

impl DatabaseEngine for ExampleDatabases {
    fn engine_name(&self) -> &str {
        "example"
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        self.meta_backend.get_database(db_name)
    }

    fn exists_database(&self, db_name: &str) -> Result<bool> {
        self.meta_backend.exists_database(db_name)
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        self.meta_backend.get_databases()
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        self.meta_backend.create_database(plan)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        self.meta_backend.drop_database(plan)
    }
}
