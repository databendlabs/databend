// Copyright 2021 Datafuse Labs
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

use databend_common_meta_app::schema::DatabaseInfo;

pub const HIVE_DATABASE_ENGINE: &str = "hive";
use databend_common_catalog::database::Database;

#[derive(Clone)]
pub struct HiveDatabase {
    pub database_info: DatabaseInfo,
}

#[async_trait::async_trait]
impl Database for HiveDatabase {
    fn name(&self) -> &str {
        self.database_info.name_ident.database_name()
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.database_info
    }
}
