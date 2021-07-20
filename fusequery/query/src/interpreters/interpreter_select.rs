// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::SelectPlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;
use crate::interpreters::plan_scheduler::PlanScheduler;

pub struct SelectInterpreter {
    ctx: FuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, select: SelectPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = Optimizers::create(self.ctx.clone()).optimize(&self.select.input)?;

        let scheduler = PlanScheduler::try_create(self.ctx.clone())?;
        let scheduled_tasks = scheduler.reschedule(&plan)?;
        let remote_actions = scheduled_tasks.get_tasks()?;

        let remote_actions_ref = &remote_actions;
        let prepare_error_handler = move |error: ErrorCode, end: usize| {
            let mut killed_set = HashSet::new();
            for (node, _) in remote_actions_ref.iter().take(end) {
                if killed_set.get(&node.name).is_none() {
                    // TODO: ISSUE-204 kill prepared query stage
                    killed_set.insert(node.name.clone());
                }
            }

            Result::Err(error)
        };

        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        for (index, (node, action)) in remote_actions.iter().enumerate() {
            let mut flight_client = node.get_flight_client().await?;
            let prepare_query_stage = flight_client.execute_action(action.clone(), timeout);
            if let Err(error) = prepare_query_stage.await {
                return prepare_error_handler(error, index);
            }
        }

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
        in_local_pipeline.execute().await
    }

    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }
}
