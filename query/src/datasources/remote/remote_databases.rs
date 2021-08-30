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

use crate::catalogs::CatalogBackend;
use crate::catalogs::Database;

pub struct RemoteDatabases {}

impl RemoteDatabases {
    pub fn create() -> Self {
        RemoteDatabases {}
    }
}

impl CatalogBackend for RemoteDatabases {
    fn get_database(&self, _db_name: &str) -> Result<Option<Arc<dyn Database>>> {
        todo!()
    }

    fn exists_database(&self, _db_name: &str) -> Result<bool> {
        todo!()
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        todo!()
    }

    fn create_database(&self, _plan: CreateDatabasePlan) -> Result<()> {
        todo!()
    }

    fn drop_database(&self, _plan: DropDatabasePlan) -> Result<()> {
        todo!()
    }
}
