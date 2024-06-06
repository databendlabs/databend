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
use databend_common_sql::plans::SetBacktracePlan;
use databend_common_tracing::change_backtrace;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct SetBacktraceInterpreter {
    plan: SetBacktracePlan,
}

impl SetBacktraceInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: SetBacktracePlan) -> Result<Self> {
        Ok(SetBacktraceInterpreter { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetBacktraceInterpreter {
    fn name(&self) -> &str {
        "SetBacktraceInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        change_backtrace(self.plan.switch);
        Ok(PipelineBuildResult::create())
    }
}
