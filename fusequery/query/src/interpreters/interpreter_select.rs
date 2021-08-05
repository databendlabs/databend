// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::{HashSet, HashMap};
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::SelectPlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;
use crate::clusters::Node;
use crate::api::{FlightAction, CancelAction};
use common_runtime::tokio::macros::support::Future;

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
        let optimized_plan = Optimizers::create(self.ctx.clone()).optimize(&self.select.input)?;

        let scheduler = PlanScheduler::try_create(self.ctx.clone())?;
        let scheduled_tasks = scheduler.reschedule(&optimized_plan)?;
        let remote_stage_actions = scheduled_tasks.get_tasks()?;

        // TODO: maybe panic?
        let mut scheduled = HashMap::new();
        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        for (node, action) in remote_stage_actions {
            let mut flight_client = node.get_flight_client().await?;
            let executing_action = flight_client.execute_action(action.clone(), timeout);

            match executing_action.await {
                Ok(_) => { scheduled.insert(node.name.clone(), node.clone()); },
                Err(error) => {
                    let scheduled = scheduled.values().cloned().collect();
                    self.error_handler(error, scheduled).await?;
                }
            };
        }

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut in_local_pipeline = pipeline_builder.build(&scheduled_tasks.get_local_task())?;
        in_local_pipeline.execute().await
    }

    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }
}

impl SelectInterpreter {
    async fn error_handler(&self, error: ErrorCode, scheduled: Vec<Arc<Node>>) -> Result<()> {
        let settings = self.ctx.get_settings();
        let flight_timeout = settings.get_flight_client_timeout()?;

        for scheduled_node in scheduled {
            let mut flight_client = scheduled_node.get_flight_client().await?;
            let cancel_action = self.cancel_flight_action();
            let executing_action = flight_client.execute_action(cancel_action, flight_timeout);
            executing_action.await?;
        }

        Err(error)
    }

    fn cancel_flight_action(&self) -> FlightAction {
        FlightAction::CancelAction(
            CancelAction {
                query_id: self.ctx.get_id()
            }
        )
    }
}
