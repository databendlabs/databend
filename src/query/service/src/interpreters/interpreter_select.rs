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
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_streams::SendableDataBlockStream;

use crate::clusters::ClusterHelper;
use crate::interpreters::plan_schedulers;
use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::optimizers::Optimizers;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::QueryPipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

/// SelectInterpreter struct which interprets SelectPlan
pub struct SelectInterpreter {
    ctx: Arc<QueryContext>,
    select: SelectPlan,
}

impl SelectInterpreter {
    /// Create the SelectInterpreter from SelectPlan
    pub fn try_create(ctx: Arc<QueryContext>, select: SelectPlan) -> Result<Self> {
        Ok(SelectInterpreter { ctx, select })
    }

    /// Call this method to optimize the logical plan before executing
    fn rewrite_plan(&self) -> Result<PlanNode> {
        plan_schedulers::apply_plan_rewrite(
            Optimizers::create(self.ctx.clone()),
            &self.select.input,
        )
    }

    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        match self.ctx.get_cluster().is_empty() {
            true => {
                let settings = self.ctx.get_settings();
                let builder = QueryPipelineBuilder::create(self.ctx.clone());
                let mut query_pipeline = builder.finalize(&self.rewrite_plan()?)?;
                query_pipeline.set_max_threads(settings.get_max_threads()? as usize);
                Ok(query_pipeline)
            }
            false => {
                let ctx = self.ctx.clone();
                let optimized_plan = self.rewrite_plan()?;
                plan_schedulers::schedule_query_new(ctx, &optimized_plan).await
            }
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    /// Get the schema of SelectPlan
    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }

    #[tracing::instrument(level = "debug", name = "select_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    /// Currently, the method has two sets of logic, if `get_enable_new_processor_framework` is turned on in the settings,
    /// the execution will use the new processor, otherwise the old processing logic will be executed.
    /// Note: there is an issue to track the progress of the new processor:  https://github.com/datafuselabs/databend/issues/3379
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let build_res = self.build_pipeline().await?;
        let async_runtime = GlobalIORuntime::instance();
        let query_need_abort = self.ctx.query_need_abort();
        let executor_settings = ExecutorSettings::try_create(&self.ctx.get_settings())?;
        Ok(Box::pin(ProcessorExecutorStream::create(
            PipelinePullingExecutor::from_pipelines(
                async_runtime,
                query_need_abort,
                build_res,
                executor_settings,
            )?,
        )?))
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a Pipeline
    async fn create_new_pipeline(&self) -> Result<PipelineBuildResult> {
        self.build_pipeline().await
    }
}
