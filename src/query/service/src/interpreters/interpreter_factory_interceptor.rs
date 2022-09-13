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
use std::time::SystemTime;

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::PlanNode;
use common_streams::ErrorStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use parking_lot::Mutex;

use crate::interpreters::access::ManagementModeAccess;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::{PipelineBuildResult, SourcePipeBuilder};
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::TableContext;
use crate::sql::plans::Plan;

pub struct InterceptorInterpreter {
    ctx: Arc<QueryContext>,
    plan: PlanNode,
    new_plan: Option<Plan>,
    inner: InterpreterPtr,
    query_log: InterpreterQueryLog,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
    management_mode_access: ManagementModeAccess,
}

impl InterceptorInterpreter {
    pub fn create(
        ctx: Arc<QueryContext>,
        inner: InterpreterPtr,
        plan: PlanNode,
        new_plan: Option<Plan>,
        query_kind: String,
    ) -> Self {
        InterceptorInterpreter {
            ctx: ctx.clone(),
            plan,
            new_plan,
            inner,
            query_log: InterpreterQueryLog::create(ctx.clone(), query_kind),
            source_pipe_builder: Mutex::new(None),
            management_mode_access: ManagementModeAccess::create(ctx),
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for InterceptorInterpreter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema(&self) -> DataSchemaRef {
        self.inner.schema()
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        // Management mode access check.
        match &self.new_plan {
            Some(p) => self.management_mode_access.check_new(p)?,
            _ => self.management_mode_access.check(&self.plan)?,
        }

        let _ = self
            .inner
            .set_source_pipe_builder((*self.source_pipe_builder.lock()).clone());
        let result_stream = match self.inner.execute().await {
            Ok(s) => s,
            Err(e) => {
                self.ctx.set_error(e.clone());
                return Err(e);
            }
        };

        let error_stream = ErrorStream::create(result_stream, self.ctx.get_error());
        let metric_stream =
            ProgressStream::try_create(Box::pin(error_stream), self.ctx.get_result_progress())?;
        Ok(Box::pin(metric_stream))
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        unimplemented!()
    }

    async fn start(&self) -> Result<()> {
        let session = self.ctx.get_current_session();
        let now = SystemTime::now();
        if session.get_type().is_user_session() {
            SessionManager::instance().status.write().query_start(now);
        }
        self.query_log.log_start(now, None).await
    }

    async fn finish(&self) -> Result<()> {
        let session = self.ctx.get_current_session();
        let now = SystemTime::now();
        session.get_status().write().query_finish();
        if session.get_type().is_user_session() {
            SessionManager::instance().status.write().query_finish(now)
        }
        let error = self.ctx.get_error_value();
        self.query_log.log_finish(now, error).await
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}
