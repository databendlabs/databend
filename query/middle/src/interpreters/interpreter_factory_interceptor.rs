// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_planners::PlanNode;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::access::ManagementModeAccess;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::InterpreterQueryLog;
use crate::sessions::QueryContext;

pub struct InterceptorInterpreter {
    ctx: Arc<QueryContext>,
    plan: PlanNode,
    inner: InterpreterPtr,
    query_log: InterpreterQueryLog,
    management_mode_access: ManagementModeAccess,
}

impl InterceptorInterpreter {
    pub fn create(ctx: Arc<QueryContext>, inner: InterpreterPtr, plan: PlanNode) -> Self {
        InterceptorInterpreter {
            ctx: ctx.clone(),
            plan: plan.clone(),
            inner,
            query_log: InterpreterQueryLog::create(ctx.clone(), plan),
            management_mode_access: ManagementModeAccess::create(ctx),
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for InterceptorInterpreter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        // Management mode access check.
        self.management_mode_access.check(&self.plan)?;

        let result_stream = self.inner.execute(input_stream).await?;
        let metric_stream =
            ProgressStream::try_create(result_stream, self.ctx.get_result_progress())?;
        Ok(Box::pin(metric_stream))
    }

    async fn start(&self) -> Result<()> {
        self.query_log.log_start().await
    }

    async fn finish(&self) -> Result<()> {
        self.query_log.log_finish().await
    }
}
