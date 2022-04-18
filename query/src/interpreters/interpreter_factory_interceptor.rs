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

use std::any::Any;
use std::sync::Arc;
use std::time::SystemTime;

use common_exception::Result;
use common_planners::PlanNode;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;

#[derive(Clone)]
pub struct InterceptorInterpreter {
    ctx: Arc<QueryContext>,
    inner: InterpreterPtr,
    query_log: InterpreterQueryLog,
}

impl InterceptorInterpreter {
    pub fn create(ctx: Arc<QueryContext>, inner: InterpreterPtr, plan: PlanNode) -> Self {
        InterceptorInterpreter {
            ctx: ctx.clone(),
            inner,
            query_log: InterpreterQueryLog::create(ctx, plan),
        }
    }

    pub fn get_inner(&self) -> &InterpreterPtr {
        &self.inner
    }

    pub fn set_insert_inner(&mut self, interpreter: Box<dyn Interpreter>) {
        self.inner = InterpreterPtr::from(interpreter);
    }
    pub fn get_box(self) -> Box<InterceptorInterpreter> {
        Box::new(self)
    }
}

#[async_trait::async_trait]
impl Interpreter for InterceptorInterpreter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let result_stream = self.inner.execute(input_stream).await?;
        let metric_stream =
            ProgressStream::try_create(result_stream, self.ctx.get_result_progress())?;
        Ok(Box::pin(metric_stream))
    }

    async fn start(&self) -> Result<()> {
        let session = self.ctx.get_current_session();
        let now = SystemTime::now();
        if session.get_type().is_user_session() {
            session
                .get_session_manager()
                .status
                .write()
                .query_start(now);
        }
        self.query_log.log_start(now).await
    }

    async fn finish(&self) -> Result<()> {
        let session = self.ctx.get_current_session();
        let now = SystemTime::now();
        session.get_status().write().query_finish();
        if session.get_type().is_user_session() {
            session
                .get_session_manager()
                .status
                .write()
                .query_finish(now)
        }
        self.query_log.log_finish(now).await
    }
}
