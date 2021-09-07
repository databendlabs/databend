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
//

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Database;

/// Database engine type, for maintaining databases, like create/drop or others lookup.
pub trait DatabaseEngine: Send + Sync {
    // Engine name of the database.
    fn engine_name(&self) -> &str;
    // Get the database by db_name.
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    // Check the database is exists or not.
    fn exists_database(&self, db_name: &str) -> Result<bool>;
    // Get all the databases of this backend.
    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>>;

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;

    // A description for this engine.
    fn engine_desc(&self) -> &str;
}
