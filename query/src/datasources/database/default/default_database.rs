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

use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

use crate::catalogs::backends::MetaApiSync;
use crate::catalogs::Database;

pub struct DefaultDatabase {
    db_name: String,
    meta: Arc<dyn MetaApiSync>,
}

impl DefaultDatabase {
    pub fn new(db_name: impl Into<String>, meta: Arc<dyn MetaApiSync>) -> Self {
        Self {
            db_name: db_name.into(),
            meta,
        }
    }
}

impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        &self.db_name
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
