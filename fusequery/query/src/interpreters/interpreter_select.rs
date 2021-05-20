// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::{Result, ErrorCodes};
use common_planners::{SelectPlan, PlanNode, StagePlan};
use common_streams::SendableDataBlockStream;

use crate::interpreters::IInterpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizer;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContextRef;
use crate::api::FlightClient;
use crate::api::ExecutePlanWithShuffleAction;
use crate::interpreters::plan_scheduler::PlanScheduler;
use std::collections::HashSet;

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

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = Optimizer::create(self.ctx.clone()).optimize(&self.select.input)?;

        let (local_plan, remote_actions) = PlanScheduler::reschedule(self.ctx.clone(), &plan)?;

        let remote_actions_ref = &remote_actions;
        let prepare_error_handler = move |error: ErrorCodes, end: usize| {
            let mut killed_set = HashSet::new();
            for index in 0..end {
                let (node, _) = &remote_actions_ref[index];
                if let None = killed_set.get(&node.name) {
                    // TODO: kill prepared query stage
                    killed_set.insert(node.name.clone());
                }
            }

            Result::Err(error)
        };

        let timeout = self.ctx.get_flight_client_timeout()?;
        for index in 0..remote_actions.len() {
            let (node, action) = &remote_actions[index];
            if let Err(error) = node.prepare_query_stage(action.clone(), timeout).await {
                return prepare_error_handler(error, index);
            }
        }

        PipelineBuilder::create(self.ctx.clone(), local_plan)
            .build()?
            .execute()
            .await
    }
}

