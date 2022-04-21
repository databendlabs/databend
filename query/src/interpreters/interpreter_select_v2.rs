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
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::sessions::QueryContext;
use crate::sql::Planner;

/// Interpret SQL query with new SQL planner
pub struct SelectInterpreterV2 {
    ctx: Arc<QueryContext>,
    query: String,
}

impl SelectInterpreterV2 {
    #[allow(dead_code)]
    pub fn try_create(ctx: Arc<QueryContext>, query: String) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreterV2 { ctx, query }))
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreterV2 {
    fn name(&self) -> &str {
        "SelectInterpreterV2"
    }

    #[tracing::instrument(level = "debug", name = "select_interpreter_v2_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let mut planner = Planner::new(self.ctx.clone());
        let pipeline = planner.plan_sql(self.query.as_str()).await?;
        let async_runtime = self.ctx.get_storage_runtime();
        let executor = PipelinePullingExecutor::try_create(async_runtime, pipeline)?;
        let executor_stream = Box::pin(ProcessorExecutorStream::create(executor)?);
        Ok(Box::pin(self.ctx.try_create_abortable(executor_stream)?))
    }
}
