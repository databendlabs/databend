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

use std::collections::HashMap;

use common_exception::Result;
use common_meta_types::DatabaseInfo;
use dyn_clone::DynClone;

#[async_trait::async_trait]
pub trait Database: DynClone + Sync + Send {
    /// Database name.
    fn name(&self) -> &str;

    fn engine(&self) -> &str {
        self.get_db_info().engine()
    }

    fn engine_options(&self) -> &HashMap<String, String> {
        &self.get_db_info().meta.engine_options
    }

    fn options(&self) -> &HashMap<String, String> {
        &self.get_db_info().meta.options
    }

    fn get_db_info(&self) -> &DatabaseInfo;

    // Initial a database.
    async fn init_database(&self, _tenant: &str) -> Result<()> {
        Ok(())
    }
}
