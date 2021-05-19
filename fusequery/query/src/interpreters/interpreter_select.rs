// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
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

        // let query_id = self.ctx.get_id()?;
        // let execute_plan = Self::extract_plan_node_with_stage(plan);
        //
        // for index in 1..execute_plan.len() {
        //     let stage_id = uuid::Uuid::new_v4().to_string();
        //     let nodes = self.ctx.try_get_cluster()?.get_nodes()?;
        //
        //     for node in nodes {
        //         if let Ok(mut client) = node.try_get_client() {
        //             if let Result::Err(error) = client.prepare_query_stage(
        //                 ExecutePlanWithShuffleAction {
        //                     query_id: query_id.clone(),
        //                     stage_id: stage_id.clone(),
        //                     plan: *execute_plan[index].input.clone(),
        //                     scatters: vec![],
        //                     scatters_action: execute_plan[index].scatters_expr.clone(),
        //                 }, 60,
        //             ).await {
        //                 // TODO: kill executed plan
        //
        //                 return Result::Err(error);
        //             }
        //             break;
        //         }
        //     }
        // }

        PipelineBuilder::create(self.ctx.clone(), local_plan)
            .build()?
            .execute()
            .await
    }
}

