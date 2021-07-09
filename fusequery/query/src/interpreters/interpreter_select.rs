// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::TryStreamExt;

use crate::interpreters::plan_scheduler::PlanScheduler;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::processors::{PipelineBuilder};
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

fn get_filter_plan(plan: PlanNode) -> Result<FilterPlan> {
    let mut res = Err(ErrorCode::Ok("Not filter plan found"));
    plan.walk_preorder(|node| -> Result<bool> {
        match node {
            PlanNode::Filter(ref filter_plan) => {
                res = Ok(filter_plan.clone());
                Ok(false)
            }
            _ => Ok(true),
        }
    })?;
    res
}

async fn execute_one_select(
    ctx: FuseQueryContextRef,
    plan: PlanNode,
    subquery_res_map: HashMap<String, bool>,
) -> Result<SendableDataBlockStream> {
    let scheduled_actions = PlanScheduler::reschedule(ctx.clone(), &plan)?;

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

    let timeout = ctx.get_settings().get_flight_client_timeout()?;
    for (index, (node, action)) in scheduled_actions.remote_actions.iter().enumerate() {
        let mut flight_client = node.get_flight_client().await?;
        if let Err(error) = flight_client
            .prepare_query_stage(action.clone(), timeout)
            .await
        {
            return prepare_error_handler(error, index);
        }
    }

    let pipeline_builder = PipelineBuilder::create(ctx.clone());
    let mut in_local_pipeline = pipeline_builder.build(&scheduled_actions.local_plan)?;
    in_local_pipeline.execute().await
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = Optimizers::create(self.ctx.clone()).optimize(&self.select.input)?;

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

        let timeout = self.ctx.get_settings().get_flight_client_timeout()?;
        for (index, (node, action)) in scheduled_actions.remote_actions.iter().enumerate() {
            let mut flight_client = node.get_flight_client().await?;
            let prepare_query_stage = flight_client.prepare_query_stage(action.clone(), timeout);
            if let Err(error) = prepare_query_stage.await {
                return prepare_error_handler(error, index);
            }
        }

        let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        let mut in_local_pipeline = pipeline_builder.build(&scheduled_actions.local_plan)?;
        in_local_pipeline.execute().await
    }

    fn schema(&self) -> DataSchemaRef {
        self.select.schema()
    }
}
