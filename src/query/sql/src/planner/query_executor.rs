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

use async_trait::async_trait;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::executor::PhysicalPlan;

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute_query_with_physical_plan(&self, plan: &PhysicalPlan)
        -> Result<Vec<DataBlock>>;

    async fn execute_query_with_sql_string(&self, sql: &str) -> Result<Vec<DataBlock>>;
}
