// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::min;
use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::{Result, ErrorCodes};
use common_planners::{EmptyPlan, StagePlan, RemotePlan, StageKind};
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use log::info;

use crate::sessions::FuseQueryContextRef;
use common_flights::ExecutePlanWithShuffleAction;
use crate::clusters::{ClusterRef, Node};

pub struct PlanScheduler;

impl PlanScheduler {
    /// Schedule the plan to Local or Remote mode.
    pub fn reschedule(ctx: FuseQueryContextRef, plan: &PlanNode) -> Result<(PlanNode, Vec<(Node, ExecutePlanWithShuffleAction)>)> {
        let cluster = ctx.try_get_cluster()?;

        if cluster.is_empty()? {
            return Ok((plan.clone(), vec![]));
        }

        let mut last_stage = None;
        let cluster_nodes = cluster.get_nodes()?;
        let mut builders = vec![];
        let mut get_node_plan: Arc<Box<dyn GetNodePlan>> = Arc::new(Box::new(EmptyGetNodePlan));

        plan.walk_postorder(|node: &PlanNode| -> Result<bool> {
            match node {
                PlanNode::Stage(plan) => {
                    let stage_id = uuid::Uuid::new_v4().to_string();

                    last_stage = Some(plan.clone());
                    builders.push(ExecutionPlanBuilder::create(ctx.get_id()?, stage_id.clone(), plan, &get_node_plan));
                    get_node_plan = RemoteGetNodePlan::create(ctx.get_id()?, stage_id.clone(), plan);
                },
                _ => get_node_plan = Arc::new(Box::new(DefaultGetNodePlan(node.clone(), get_node_plan.clone()))),
            };

            Ok(true)
        })?;

        if let Some(stage_plan) = last_stage {
            if stage_plan.kind != StageKind::Convergent {
                return Result::Err(ErrorCodes::PlanScheduleError("The final stage plan must be convergent"));
            }
        }

        let local_node = (&cluster_nodes).iter().find(|node| node.local);

        if local_node.is_none() {
            return Result::Err(ErrorCodes::NotFountLocalNode("The PlanScheduler must be in the query cluster"));
        }

        let local_plan = get_node_plan.get_plan(&local_node.unwrap().name, &cluster_nodes)?;
        let mut remote_plans = vec![];
        for node in &cluster_nodes {
            for builder in &builders {
                if let Some(action) = builder.build(&node.name, &cluster_nodes) {
                    remote_plans.push((node.clone(), action));
                }
            }
        }

        Ok((local_plan, remote_plans))
    }
}

trait GetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Node>) -> Result<PlanNode>;
}

struct EmptyGetNodePlan;

struct RemoteGetNodePlan(String, String, StagePlan);

struct DefaultGetNodePlan(PlanNode, Arc<Box<dyn GetNodePlan>>);

impl GetNodePlan for DefaultGetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Node>) -> Result<PlanNode> {
        let mut clone_node = self.0.clone();
        if let Ok(input) = self.1.get_plan(node_name, cluster_nodes) {
            clone_node.set_inputs(vec![&input]);
        }

        Ok(clone_node)
    }
}

impl GetNodePlan for EmptyGetNodePlan {
    fn get_plan(&self, _node_name: &String, _cluster_nodes: &Vec<Node>) -> Result<PlanNode> {
        Ok(PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        }))
    }
}

impl RemoteGetNodePlan {
    pub fn create(query_id: String, stage_id: String, plan: &StagePlan) -> Arc<Box<dyn GetNodePlan>> {
        Arc::new(Box::new(RemoteGetNodePlan(
            query_id, stage_id, plan.clone(),
        )))
    }
}

impl GetNodePlan for RemoteGetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Node>) -> Result<PlanNode> {
        match self.2.kind {
            StageKind::Expansive => {
                for cluster_node in cluster_nodes {
                    if cluster_node.local {
                        return Ok(PlanNode::Remote(RemotePlan {
                            schema: self.2.schema(),
                            fetch_name: format!("{}/{}/{}", self.0, self.1, node_name),
                            fetch_nodes: vec![cluster_node.name.clone()],
                        }))
                    }
                }

                Err(ErrorCodes::NotFountLocalNode("The PlanScheduler must be in the query cluster"))
            }
            _ => {
                let all_nodes_name = cluster_nodes.iter().map(|node| node.name.clone()).collect::<Vec<_>>();
                Ok(PlanNode::Remote(RemotePlan {
                    schema: self.2.schema(),
                    fetch_name: format!("{}/{}/{}", self.0, self.1, node_name),
                    fetch_nodes: all_nodes_name.clone(),
                }))
            }
        }
    }
}

struct ExecutionPlanBuilder(String, String, StagePlan, Arc<Box<dyn GetNodePlan>>);

impl ExecutionPlanBuilder {
    pub fn create(query_id: String, stage_id: String, plan: &StagePlan, node_plan_getter: &Arc<Box<dyn GetNodePlan>>) -> Arc<Box<ExecutionPlanBuilder>> {
        Arc::new(Box::new(ExecutionPlanBuilder(
            query_id, stage_id, plan.clone(), node_plan_getter.clone(),
        )))
    }

    pub fn build(&self, node_name: &String, cluster_nodes: &Vec<Node>) -> Option<ExecutePlanWithShuffleAction> {
        match self.2.kind {
            StageKind::Expansive => {
                let all_nodes_name = cluster_nodes.iter().map(|node| node.name.clone()).collect::<Vec<_>>();
                for cluster_node in cluster_nodes {
                    if cluster_node.name == *node_name && cluster_node.local {
                        return Some(ExecutePlanWithShuffleAction {
                            query_id: self.0.clone(),
                            stage_id: self.1.clone(),
                            plan: self.3.get_plan(node_name, cluster_nodes).unwrap(),
                            scatters: all_nodes_name.clone(),
                            scatters_action: self.2.scatters_expr.clone(),
                        });
                    }
                }
                None
            },
            StageKind::Convergent => {
                for cluster_node in cluster_nodes {
                    if cluster_node.local {
                        return Some(ExecutePlanWithShuffleAction {
                            query_id: self.0.clone(),
                            stage_id: self.1.clone(),
                            plan: self.3.get_plan(node_name, cluster_nodes).unwrap(),
                            scatters: vec![cluster_node.name.clone()],
                            scatters_action: self.2.scatters_expr.clone(),
                        })
                    }
                }

                None
            }
            StageKind::Normal => {
                let all_nodes_name = cluster_nodes.iter().map(|node| node.name.clone()).collect::<Vec<_>>();
                Some(ExecutePlanWithShuffleAction {
                    query_id: self.0.clone(),
                    stage_id: self.1.clone(),
                    plan: self.3.get_plan(node_name, cluster_nodes).unwrap(),
                    scatters: all_nodes_name.clone(),
                    scatters_action: self.2.scatters_expr.clone(),
                })
            }
        }
    }
}
