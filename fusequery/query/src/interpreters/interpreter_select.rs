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
use common_flights::ExecutePlanWithShuffleAction;
use crate::interpreters::plan_scheduler::PlanScheduler;

pub struct SelectInterpreter {
    ctx: FuseQueryContextRef,
    select: SelectPlan,
}

impl SelectInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, select: SelectPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreter { ctx, select }))
    }

    fn extract_plan_node_with_stage(plan: PlanNode) -> Vec<StagePlan> {
        let mut execute_plan = vec![];
        plan.walk_preorder(|node| -> Result<bool> {
            if let PlanNode::Stage(stage_plan) = node {
                execute_plan.push(stage_plan.clone());
            }

            Ok(true)
        });

        execute_plan
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

        fn prepare_error_handler(error: ErrorCodes, end: usize) -> Result<SendableDataBlockStream> {
            Result::Err(error)
        }

        let timeout = self.ctx.get_flight_client_timeout()?;
        for index in 0..remote_actions.len() {
            let (node, action) = &remote_actions[index];
            match node.try_get_client() {
                Err(error) => return prepare_error_handler(error, index),
                Ok(mut flight_client) => {
                    if let Err(error) = flight_client.prepare_query_stage(action.clone(), timeout).await {
                        return prepare_error_handler(error, index);
                    }
                }
            }
        }

        PipelineBuilder::create(self.ctx.clone(), local_plan)
            .build()?
            .execute()
            .await
    }
}

