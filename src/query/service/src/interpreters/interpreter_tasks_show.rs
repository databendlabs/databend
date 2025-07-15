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
use databend_common_sql::plans::ShowTasksPlan;
use databend_common_storages_system::parse_tasks_to_datablock;

use crate::interpreters::task::TaskInterpreter;
use crate::interpreters::task::TaskInterpreterManager;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ShowTasksInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowTasksPlan,
}

impl ShowTasksInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowTasksPlan) -> Result<Self> {
        Ok(ShowTasksInterpreter { ctx, plan })
    }
}

impl ShowTasksInterpreter {}

#[async_trait::async_trait]
impl Interpreter for ShowTasksInterpreter {
    fn name(&self) -> &str {
        "ShowTasksInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tasks = TaskInterpreterManager::build(&self.ctx)?
            .show_tasks(&self.ctx, &self.plan)
            .await?;

        let result = parse_tasks_to_datablock(tasks)?;
        PipelineBuildResult::from_blocks(vec![result])
    }
}
