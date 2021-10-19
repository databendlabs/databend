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

use common_exception::Result;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;

pub trait Database: Sync + Send {
    /// Database name.
    fn name(&self) -> &str;

    /// DDL
    fn create_table(&self, plan: CreateTablePlan) -> Result<()>;
    fn drop_table(&self, plan: DropTablePlan) -> Result<()>;
}
