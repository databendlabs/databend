// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::PlanNode;
use common_planners::StageKind;
use common_planners::StagePlan;
use common_tracing::tracing;

use crate::api::ExecutePlanWithShuffleAction;
use crate::sessions::FuseQueryContextRef;
use crate::shuffle::DefaultExecutorPlan;
use crate::shuffle::EmptyExecutorPlan;
use crate::shuffle::ExecutorPlan;
use crate::shuffle::ReadSourceExecutorPlan;
use crate::shuffle::RemoteExecutorPlan;

pub struct PlanScheduler;

pub struct ScheduledActions {
    pub local_plan: PlanNode,
    pub remote_actions: Vec<(Arc<ClusterExecutor>, ExecutePlanWithShuffleAction)>,
}

impl PlanScheduler {
    /// Schedule the plan to Local or Remote mode.
    #[tracing::instrument(level = "info", skip(ctx, plan))]
    pub async fn reschedule(
        ctx: FuseQueryContextRef,
        subquery_res_map: HashMap<String, bool>,
        plan: &PlanNode,
    ) -> Result<ScheduledActions> {
        let executors = ctx.try_get_executors().await?;

        if executors.is_empty() {
            return Ok(ScheduledActions {
                local_plan: plan.clone(),
                remote_actions: vec![],
            });
        }

        let mut last_stage = None;
        let mut builders = vec![];
        let mut get_node_plan: Arc<Box<dyn ExecutorPlan>> = Arc::new(Box::new(EmptyExecutorPlan));

        plan.walk_postorder(|node: &PlanNode| -> Result<bool> {
            match node {
                PlanNode::Stage(plan) => {
                    let stage_id = uuid::Uuid::new_v4().to_string();

                    last_stage = Some(plan.clone());
                    builders.push(ExecutionPlanBuilder::create(
                        ctx.get_id(),
                        stage_id.clone(),
                        plan,
                        &get_node_plan,
                    ));
                    get_node_plan = RemoteExecutorPlan::create(ctx.get_id(), stage_id, plan);
                }
                PlanNode::ReadSource(plan) => {
                    get_node_plan =
                        ReadSourceExecutorPlan::create(&ctx, plan, &get_node_plan, &executors)?;
                }
                _ => {
                    get_node_plan = Arc::new(Box::new(DefaultExecutorPlan(
                        node.clone(),
                        get_node_plan.clone(),
                    )))
                }
            };

            Ok(true)
        })?;

        if let Some(stage_plan) = last_stage {
            if stage_plan.kind != StageKind::Convergent {
                return Result::Err(ErrorCode::PlanScheduleError(
                    "The final stage plan must be convergent",
                ));
            }
        }

        let local_executor = (&executors).iter().find(|executor| executor.local);

        if local_executor.is_none() {
            return Result::Err(ErrorCode::NotFoundLocalNode(
                "The PlanScheduler must be in the query cluster",
            ));
        }

        let local_plan = get_node_plan.get_plan(&local_executor.unwrap().name, &executors)?;
        let mut remote_actions = vec![];
        for executor in &executors {
            for builder in &builders {
                if let Some(action) =
                    builder.build(&executor.name, &executors, subquery_res_map.clone())?
                {
                    remote_actions.push((executor.clone(), action));
                }
            }
        }

        Ok(ScheduledActions {
            local_plan,
            remote_actions,
        })
    }
}

struct ExecutionPlanBuilder(String, String, StagePlan, Arc<Box<dyn ExecutorPlan>>);

impl ExecutionPlanBuilder {
    pub fn create(
        query_id: String,
        stage_id: String,
        plan: &StagePlan,
        node_plan_getter: &Arc<Box<dyn ExecutorPlan>>,
    ) -> Arc<Box<ExecutionPlanBuilder>> {
        Arc::new(Box::new(ExecutionPlanBuilder(
            query_id,
            stage_id,
            plan.clone(),
            node_plan_getter.clone(),
        )))
    }

    pub fn build(
        &self,
        executor_name: &str,
        executors: &[Arc<ClusterExecutor>],
        subquery_res_map: HashMap<String, bool>,
    ) -> Result<Option<ExecutePlanWithShuffleAction>> {
        match self.2.kind {
            StageKind::Expansive => {
                let all_nodes_name = executors
                    .iter()
                    .map(|node| node.name.clone())
                    .collect::<Vec<_>>();
                for executor in executors {
                    if executor.name == *executor_name && executor.local {
                        return Ok(Some(ExecutePlanWithShuffleAction {
                            query_id: self.0.clone(),
                            stage_id: self.1.clone(),
                            plan: self.3.get_plan(executor_name, executors)?,
                            scatters: all_nodes_name,
                            scatters_action: self.2.scatters_expr.clone(),
                            subquery_res_map,
                        }));
                    }
                }
                Ok(None)
            }
            StageKind::Convergent => {
                for executor in executors {
                    if executor.local {
                        return Ok(Some(ExecutePlanWithShuffleAction {
                            query_id: self.0.clone(),
                            stage_id: self.1.clone(),
                            plan: self.3.get_plan(executor_name, executors)?,
                            scatters: vec![executor.name.clone()],
                            scatters_action: self.2.scatters_expr.clone(),
                            subquery_res_map,
                        }));
                    }
                }

                Result::Err(ErrorCode::NotFoundLocalNode(
                    "The PlanScheduler must be in the query cluster",
                ))
            }
            StageKind::Normal => {
                let executor_names = executors
                    .iter()
                    .map(|executor| executor.name.clone())
                    .collect::<Vec<_>>();
                Ok(Some(ExecutePlanWithShuffleAction {
                    query_id: self.0.clone(),
                    stage_id: self.1.clone(),
                    plan: self.3.get_plan(executor_name, executors)?,
                    scatters: executor_names,
                    scatters_action: self.2.scatters_expr.clone(),
                    subquery_res_map,
                }))
            }
        }
    }
}
