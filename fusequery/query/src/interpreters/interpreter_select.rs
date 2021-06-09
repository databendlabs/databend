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

use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::interpreters::IInterpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizer;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;

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
impl IInterpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = Optimizer::create(self.ctx.clone()).optimize(&self.select.input)?;

        let scheduled_actions = PlanScheduler::reschedule(self.ctx.clone(), &plan)?;

        let remote_actions_ref = &scheduled_actions.remote_actions;
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

        let timeout = self.ctx.get_flight_client_timeout()?;
        for (index, (node, action)) in scheduled_actions.remote_actions.iter().enumerate() {
            let mut flight_client = node.get_flight_client().await?;
            if let Err(error) = flight_client
                .prepare_query_stage(action.clone(), timeout)
                .await
            {
                return prepare_error_handler(error, index);
            }
        }

        PipelineBuilder::create(self.ctx.clone(), scheduled_actions.local_plan.clone())
            .build()?
            .execute()
            .await
    }
}
