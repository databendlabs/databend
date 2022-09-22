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

use common_catalog::table_context::TableContext;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::InterpreterQueryLog;
use crate::interpreters::PullingExecutorStream;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;

#[async_trait::async_trait]
/// Interpreter is a trait for different PlanNode
/// Each type of planNode has its own corresponding interpreter
pub trait Interpreter: Sync + Send {
    /// Return the name of Interpreter, such as "CreateDatabaseInterpreter"
    fn name(&self) -> &str;

    /// Return the schema of Interpreter
    fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }

    /// The core of the databend processor which will execute the logical plan and get the DataBlock
    async fn execute(&self, ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
        log_query_start(&ctx);

        let mut build_res = self.execute2().await?;

        if build_res.main_pipeline.pipes.is_empty() {
            log_query_finished(&ctx, None);

            return Ok(Box::pin(DataBlockStream::create(
                self.schema(),
                None,
                vec![],
            )));
        }

        let query_ctx = ctx.clone();
        build_res.main_pipeline.set_on_finished(move |may_error| {
            log_query_finished(&query_ctx, may_error.clone());

            match may_error {
                None => Ok(()),
                Some(error) => Err(error.clone()),
            }
        });

        let settings = ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(&settings)?;

        if build_res.main_pipeline.is_complete_pipeline()? {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

            ctx.set_executor(Arc::downgrade(&complete_executor.get_inner()));
            complete_executor.execute()?;
            return Ok(Box::pin(DataBlockStream::create(
                Arc::new(DataSchema::new(vec![])),
                None,
                vec![],
            )));
        }

        // WTF: We need to implement different logic for the HTTP handler
        if let Some(handle) = ctx.get_http_query() {
            return handle.execute(ctx.clone(), build_res, self.schema()).await;
        }

        let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;

        ctx.set_executor(Arc::downgrade(&pulling_executor.get_inner()));
        Ok(Box::pin(ProgressStream::try_create(
            Box::pin(PullingExecutorStream::create(pulling_executor)?),
            ctx.get_result_progress(),
        )?))
    }

    /// The core of the databend processor which will execute the logical plan and build the pipeline
    async fn execute2(&self) -> Result<PipelineBuildResult>;

    /// Do some start work for the interpreter
    /// Such as query counts, query start time and etc
    async fn start(&self) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement start method for {:?}",
            self.name()
        )))
    }

    /// Do some finish work for the interpreter.
    /// Such as get the metrics and write to query log etc.
    async fn finish(&self) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement finish method for {:?}",
            self.name()
        )))
    }

    fn set_source_pipe_builder(&self, _builder: Option<SourcePipeBuilder>) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "UnImplement set_source_pipe_builder method for {:?}",
            self.name()
        )))
    }
}

pub type InterpreterPtr = Arc<dyn Interpreter>;

fn log_query_start(ctx: &QueryContext) {
    let now = SystemTime::now();
    let session = ctx.get_current_session();

    if session.get_type().is_user_session() {
        SessionManager::instance().status.write().query_start(now);
    }

    if let Err(error) = InterpreterQueryLog::log_start(ctx, now, None) {
        tracing::error!("interpreter.start.error: {:?}", error)
    }
}

fn log_query_finished(ctx: &QueryContext, error: Option<ErrorCode>) {
    let now = SystemTime::now();
    let session = ctx.get_current_session();

    session.get_status().write().query_finish();
    if session.get_type().is_user_session() {
        SessionManager::instance().status.write().query_finish(now)
    }

    if let Err(error) = InterpreterQueryLog::log_finish(ctx, now, error) {
        tracing::error!("interpreter.finish.error: {:?}", error)
    }
}
