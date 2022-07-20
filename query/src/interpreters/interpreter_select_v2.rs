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

use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::Pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::exec::PhysicalPlanBuilder;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

/// Interpret SQL query with new SQL planner
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
        let builder = PhysicalPlanBuilder::new(self.metadata.clone());
        let physical_plan = builder.build(&self.s_expr)?;

        if let Some(handle) = self.ctx.get_http_query() {
            return handle
                .execute(self.ctx.clone(), &physical_plan, &self.bind_context.columns)
                .await;
        }

        let last_schema = physical_plan.output_schema()?;
        let mut pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut build_res = pipeline_builder.finalize(&physical_plan)?;

        // Render result set with given output schema
        PipelineBuilder::render_result_set(
            last_schema,
            &self.bind_context.columns,
            &mut build_res.main_pipeline,
        )?;

        let async_runtime = self.ctx.get_storage_runtime();
        let query_need_abort = self.ctx.query_need_abort();
        build_res.set_max_threads(self.ctx.get_settings().get_max_threads()? as usize);

        Ok(Box::pin(ProcessorExecutorStream::create(
            PipelinePullingExecutor::from_pipelines(
                async_runtime, query_need_abort, build_res,
            )?)?
        ))
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a Pipeline
    async fn create_new_pipeline(&self) -> Result<Pipeline> {
        let builder = PhysicalPlanBuilder::new(self.metadata.clone());
        let physical_plan = builder.build(&self.s_expr)?;
        let last_schema = physical_plan.output_schema()?;

        let mut pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut build_res = pipeline_builder.finalize(&physical_plan)?;

        PipelineBuilder::render_result_set(
            last_schema,
            &self.bind_context.columns,
            &mut build_res.main_pipeline,
        )?;

        build_res.set_max_threads(self.ctx.get_settings().get_max_threads()? as usize);

        if !build_res.sources_pipelines.is_empty() {
            return Err(ErrorCode::UnImplement(
                "Unsupported run query with sub-pipeline".to_string(),
            ));
        }

        Ok(build_res.main_pipeline)
    }

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        Ok(())
    }
}
