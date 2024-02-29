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
use databend_common_sql::plans::UndropDatabasePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct UndropDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: UndropDatabasePlan,
}

impl UndropDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UndropDatabasePlan) -> Result<Self> {
        Ok(UndropDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UndropDatabaseInterpreter {
    fn name(&self) -> &str {
        "UndropDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        catalog.undrop_database(self.plan.clone().into()).await?;
        Ok(PipelineBuildResult::create())
    }
}
