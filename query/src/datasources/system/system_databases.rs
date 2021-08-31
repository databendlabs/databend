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
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::configs::Config;
use crate::datasources::system::SystemDatabase;

pub struct SystemDatabases {
    database: Arc<dyn Database>,
}

impl SystemDatabases {
    pub fn create(_conf: Config) -> Self {
        let database = Arc::new(SystemDatabase::create());
        SystemDatabases { database }
    }
}

impl DatabaseEngine for SystemDatabases {
    fn engine_name(&self) -> &str {
        "system"
    }

    fn get_database(&self, _db_name: &str) -> Result<Option<Arc<dyn Database>>> {
        Ok(Some(self.database.clone()))
    }

    fn exists_database(&self, db_name: &str) -> Result<bool> {
        Ok(self.database.name() == db_name)
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![self.database.clone()])
    }

    fn create_database(&self, _plan: CreateDatabasePlan) -> Result<()> {
        Err(ErrorCode::UnImplement("Cannot create system database"))
    }

    fn drop_database(&self, _plan: DropDatabasePlan) -> Result<()> {
        Err(ErrorCode::UnImplement("Cannot drop system database"))
    }
}
