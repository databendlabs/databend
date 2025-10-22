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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_sql::plans::AlterDatabasePlan;
use log::debug;
use databend_common_catalog::table_context::TableContext;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct AlterDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterDatabasePlan,
}

impl AlterDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterDatabasePlan) -> Result<Self> {
        Ok(AlterDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterDatabaseInterpreter {
    fn name(&self) -> &str {
        "AlterDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "alter_database_execute");

        // Get the catalog and database
        let _catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        // For now, we need to implement the actual database metadata update
        // This would typically involve:
        // 1. Getting the current database metadata
        // 2. Merging the new options with existing ones
        // 3. Updating the database metadata in the meta service

        // TODO: Implement actual database options update through catalog API
        // This requires extending the catalog interface to support database metadata updates

        Ok(PipelineBuildResult::create())
    }
}