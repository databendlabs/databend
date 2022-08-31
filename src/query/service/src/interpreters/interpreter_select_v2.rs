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

use common_base::base::GlobalIORuntime;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

use super::plan_schedulers::schedule_query_v2;
use crate::clusters::ClusterHelper;
use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::executor::PipelineBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

/// Interpret SQL query with ne&w SQL planner
pub struct SelectInterpreterV2 {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    bind_context: BindContext,
    metadata: MetadataRef,
}

impl SelectInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        bind_context: BindContext,
        s_expr: SExpr,
        metadata: MetadataRef,
    ) -> Result<Self> {
        Ok(SelectInterpreterV2 {
            ctx,
            s_expr,
            bind_context,
            metadata,
        })
    }

    pub async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        let builder = PhysicalPlanBuilder::new(self.metadata.clone(), self.ctx.clone());
        let physical_plan = builder.build(&self.s_expr).await?;

        if self.ctx.get_cluster().is_empty() {
            let last_schema = physical_plan.output_schema()?;
            let pb = PipelineBuilder::create(self.ctx.clone());
            let mut build_res = pb.finalize(&physical_plan)?;

            // Render result set with given output schema
            PipelineBuilder::render_result_set(
                last_schema,
                &self.bind_context.columns,
                &mut build_res.main_pipeline,
            )?;

            build_res.set_max_threads(self.ctx.get_settings().get_max_threads()? as usize);
            Ok(build_res)
        } else {
            schedule_query_v2(self.ctx.clone(), &self.bind_context.columns, &physical_plan).await
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreterV2 {
    fn name(&self) -> &str {
        "SelectInterpreterV2"
    }

    fn schema(&self) -> DataSchemaRef {
        self.bind_context.output_schema()
    }

    #[tracing::instrument(level = "debug", name = "select_interpreter_v2_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let build_res = self.build_pipeline().await?;

        // WTF: We need to implement different logic for the HTTP handler
        if let Some(handle) = self.ctx.get_http_query() {
            return handle
                .execute(self.ctx.clone(), build_res, &self.bind_context.columns)
                .await;
        }

        let executor_settings = ExecutorSettings::try_create(&self.ctx.get_settings())?;

        Ok(Box::pin(Box::pin(ProcessorExecutorStream::create(
            PipelinePullingExecutor::from_pipelines(
                GlobalIORuntime::instance(),
                self.ctx.query_need_abort(),
                build_res,
                executor_settings,
            )?,
        )?)))
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a Pipeline
    async fn create_new_pipeline(&self) -> Result<PipelineBuildResult> {
        let build_res = self.build_pipeline().await?;
        Ok(build_res)
    }

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        Ok(())
    }
}
