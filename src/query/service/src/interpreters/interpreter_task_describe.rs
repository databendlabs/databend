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
use databend_common_sql::plans::DescribeTaskPlan;
use databend_common_storages_system::parse_tasks_to_datablock;

use crate::interpreters::task::TaskInterpreter;
use crate::interpreters::task::TaskInterpreterManager;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct DescribeTaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescribeTaskPlan,
}

impl DescribeTaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescribeTaskPlan) -> Result<Self> {
        Ok(DescribeTaskInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeTaskInterpreter {
    fn name(&self) -> &str {
        "DescribeTaskInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        let Some(task) = TaskInterpreterManager::build(&self.ctx)?
            .describe_task(&self.ctx, &self.plan)
            .await?
        else {
            return Ok(PipelineBuildResult::create());
        };
        PipelineBuildResult::from_blocks(vec![parse_tasks_to_datablock(vec![task])?])
    }
}
