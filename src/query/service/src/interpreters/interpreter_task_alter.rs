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
use databend_common_sql::plans::AlterTaskPlan;

use crate::interpreters::task::TaskInterpreter;
use crate::interpreters::task::TaskInterpreterManager;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct AlterTaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterTaskPlan,
}

impl AlterTaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterTaskPlan) -> Result<Self> {
        Ok(AlterTaskInterpreter { ctx, plan })
    }
}

impl AlterTaskInterpreter {}

#[async_trait::async_trait]
impl Interpreter for AlterTaskInterpreter {
    fn name(&self) -> &str {
        "AlterTaskInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        TaskInterpreterManager::build(&self.ctx)?
            .alter_task(&self.ctx, &self.plan)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
