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
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::Database;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::Table;

pub struct SystemDatabase {
    name: String,
    tables: Arc<InMemoryMetas>,
}

impl SystemDatabase {
    pub fn create(name: impl Into<String>, tables: Arc<InMemoryMetas>) -> Self {
        let name = name.into();
        SystemDatabase { name, tables }
    }
}

impl Database for SystemDatabase {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table(&self, _table_name: &str) -> Result<Arc<dyn Table>> {
        unimplemented!();
    }

    fn get_table_by_id(
        &self,
        _table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<dyn Table>> {
        unimplemented!();
    }

    fn get_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        Ok(self.tables.name_to_table.values().cloned().collect())
    }

    fn create_table(&self, _plan: CreateTablePlan) -> Result<()> {
        Result::Err(ErrorCode::UnImplement(
            "Cannot create table for system database",
        ))
    }

    fn drop_table(&self, _plan: DropTablePlan) -> Result<()> {
        Result::Err(ErrorCode::UnImplement(
            "Cannot drop table for system database",
        ))
    }
}
