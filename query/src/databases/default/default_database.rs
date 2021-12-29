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

use common_exception::Result;
use common_meta_types::DatabaseInfo;

use crate::databases::Database;
use crate::databases::DatabaseContext;

#[derive(Clone)]
pub struct DefaultDatabase {
    db_info: DatabaseInfo,
}

impl DefaultDatabase {
    pub fn try_create(_ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        Ok(Box::new(Self { db_info }))
    }
}

impl Database for DefaultDatabase {
    fn name(&self) -> &str {
        &self.db_info.db
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }
}
