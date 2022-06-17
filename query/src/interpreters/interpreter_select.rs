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

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::plan_schedulers;
use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::QueryPipelineBuilder;
use crate::sessions::QueryContext;

/// SelectInterpreter struct which interprets SelectPlan
pub struct SelectInterpreter {
    ctx: Arc<QueryContext>,
    select: SelectPlan,
}

impl SelectInterpreter {
    /// Create the SelectInterpreter from SelectPlan
    pub fn try_create(ctx: Arc<QueryContext>, select: SelectPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }

    /// Call this method to optimize the logical plan before executing
    fn rewrite_plan(&self) -> Result<PlanNode> {
        plan_schedulers::apply_plan_rewrite(
            Optimizers::create(self.ctx.clone()),
            &self.select.input,
        )
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

    #[tracing::instrument(level = "debug", name = "select_interpreter_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    /// Currently, the method has two sets of logic, if `get_enable_new_processor_framework` is turned on in the settings,
    /// the execution will use the new processor, otherwise the old processing logic will be executed.
    /// Note: there is an issue to track the progress of the new processor:  https://github.com/datafuselabs/databend/issues/3379
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let settings = self.ctx.get_settings();

        if settings.get_enable_new_processor_framework()? != 0 {
            let mut query_pipeline = match self.ctx.get_cluster().is_empty() {
                true => self.create_new_pipeline()?,
                false => {
                    let optimized_plan = self.rewrite_plan()?;
                    plan_schedulers::schedule_query_new(self.ctx.clone(), &optimized_plan).await?
                }
            };

            let settings = self.ctx.get_settings();
            let async_runtime = self.ctx.get_storage_runtime();
            query_pipeline.set_max_threads(settings.get_max_threads()? as usize);
            let executor = PipelinePullingExecutor::try_create(async_runtime, query_pipeline)?;
            let (handler, stream) = ProcessorExecutorStream::create(executor)?;
            self.ctx.add_source_abort_handle(handler);
            return Ok(Box::pin(stream));
        } else {
            let optimized_plan = self.rewrite_plan()?;
            plan_schedulers::schedule_query(&self.ctx, &optimized_plan).await
        }
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a NewPipeline
    fn create_new_pipeline(&self) -> Result<NewPipeline> {
        let builder = QueryPipelineBuilder::create(self.ctx.clone());
        builder.finalize(&self.rewrite_plan()?)
    }
}
