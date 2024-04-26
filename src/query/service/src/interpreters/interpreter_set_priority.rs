// Copyright 2024 Datafuse Labs.
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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::SetPriorityPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct SetPriorityInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetPriorityPlan,
}

impl SetPriorityInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetPriorityPlan) -> Result<Self> {
        Ok(SetPriorityInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetPriorityInterpreter {
    fn name(&self) -> &str {
        "AdjustPriorityInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let id = &self.plan.id;
        match self.ctx.get_session_by_id(id) {
            Some(adjust_session) => {
                adjust_session.set_query_priority(self.plan.priority);
                Ok(PipelineBuildResult::create())
            }
            None => Err(ErrorCode::UnknownSession(format!(
                "Not found session id {}",
                id
            ))),
        }
    }
}
