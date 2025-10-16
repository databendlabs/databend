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
use databend_common_sql::plans::RefreshDatabaseCachePlan;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct RefreshDatabaseCacheInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshDatabaseCachePlan,
}

impl RefreshDatabaseCacheInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshDatabaseCachePlan) -> Result<Self> {
        Ok(RefreshDatabaseCacheInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshDatabaseCacheInterpreter {
    fn name(&self) -> &str {
        "RefreshDatabaseCacheInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let catalog = self.ctx.get_catalog(&plan.catalog).await?;

        let _ = catalog
            .get_database(&plan.tenant, &plan.database)
            .await?
            .refresh_database()
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
