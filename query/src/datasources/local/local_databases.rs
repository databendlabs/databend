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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_infallible::RwLock;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::configs::Config;
use crate::datasources::local::LocalDatabase;

/// The collection of the local database.
pub struct LocalDatabases {
    databases: RwLock<HashMap<String, Arc<dyn Database>>>,
}

impl LocalDatabases {
    pub fn create(_conf: Config) -> Self {
        let databases: RwLock<HashMap<String, Arc<dyn Database>>> = Default::default();
        let default_database = Arc::new(LocalDatabase::create("default"));
        databases
            .write()
            .insert("default".to_string(), default_database);

        LocalDatabases { databases }
    }
}

impl DatabaseEngine for LocalDatabases {
    fn engine_name(&self) -> &str {
        "local"
    }

    fn get_database(&self, db_name: &str) -> Result<Option<Arc<dyn Database>>> {
        Ok(self.databases.read().get(db_name).cloned())
    }

    fn exists_database(&self, db_name: &str) -> Result<bool> {
        Ok(self.databases.read().get(db_name).is_some())
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        let databases = self.databases.read();
        Ok(databases.values().cloned().collect::<Vec<_>>())
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        let db_name = plan.db.as_str();
        let database = LocalDatabase::create(db_name);
        self.databases.write().insert(plan.db, Arc::new(database));
        Ok(())
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        self.databases.write().remove(plan.db.as_str());
        Ok(())
    }
}
