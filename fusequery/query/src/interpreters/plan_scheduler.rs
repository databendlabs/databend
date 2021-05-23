// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::min;
use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::{Result, ErrorCodes};
use common_planners::{EmptyPlan, StagePlan, RemotePlan, StageKind, Partitions};
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use log::info;

use crate::sessions::FuseQueryContextRef;
use crate::api::ExecutePlanWithShuffleAction;
use crate::clusters::{ClusterRef, Node};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Vacant, Occupied};

pub struct PlanScheduler;

impl PlanScheduler {
    /// Schedule the plan to Local or Remote mode.
    pub fn reschedule(ctx: FuseQueryContextRef, plan: &PlanNode) -> Result<(PlanNode, Vec<(Arc<Node>, ExecutePlanWithShuffleAction)>)> {
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
                }
                PlanNode::ReadSource(plan) => {
                    get_node_plan = ReadSourceGetNodePlan::create(&ctx, &plan, &get_node_plan, &cluster_nodes)?;
                }
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
            return Result::Err(ErrorCodes::NotFoundLocalNode("The PlanScheduler must be in the query cluster"));
        }

        let local_plan = get_node_plan.get_plan(&local_node.unwrap().name, &cluster_nodes)?;
        let mut remote_plans = vec![];
        for node in &cluster_nodes {
            for builder in &builders {
                if let Some(action) = builder.build(&node.name, &cluster_nodes)? {
                    remote_plans.push((node.clone(), action));
                }
            }
        }

        Ok((local_plan, remote_plans))
    }
}

trait GetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode>;
}

struct EmptyGetNodePlan;

struct RemoteGetNodePlan(String, String, StagePlan);

struct DefaultGetNodePlan(PlanNode, Arc<Box<dyn GetNodePlan>>);

struct LocalReadSourceGetNodePlan(ReadDataSourcePlan, Arc<Box<dyn GetNodePlan>>);

struct RemoteReadSourceGetNodePlan(ReadDataSourcePlan, Arc<HashMap<String, Partitions>>, Arc<Box<dyn GetNodePlan>>);

struct ReadSourceGetNodePlan(Arc<Box<dyn GetNodePlan>>);

impl GetNodePlan for DefaultGetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode> {
        let mut clone_node = self.0.clone();
        clone_node.set_inputs(vec![&self.1.get_plan(node_name, cluster_nodes)?]);
        Ok(clone_node)
    }
}

impl GetNodePlan for EmptyGetNodePlan {
    fn get_plan(&self, _node_name: &String, _cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode> {
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
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode> {
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

                Err(ErrorCodes::NotFoundLocalNode("The PlanScheduler must be in the query cluster"))
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

impl GetNodePlan for LocalReadSourceGetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode> {
        match cluster_nodes.iter().filter(|node| &node.name == node_name && node.local).count() {
            0 => Result::Err(ErrorCodes::NotFoundLocalNode("The PlanScheduler must be in the query cluster")),
            _ => Ok(PlanNode::ReadSource(self.0.clone())),
        }
    }
}

impl GetNodePlan for RemoteReadSourceGetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode> {
        let partitions = self.1.get(node_name).map(Clone::clone).unwrap_or(vec![]);
        Ok(PlanNode::ReadSource(ReadDataSourcePlan {
            db: self.0.db.clone(),
            table: self.0.table.clone(),
            schema: self.0.schema.clone(),
            partitions: partitions.clone(),
            statistics: self.0.statistics.clone(),
            description: self.0.description.clone(),
            scan_plan: self.0.scan_plan.clone(),
        }))
    }
}

impl ReadSourceGetNodePlan {
    pub fn create(
        ctx: &FuseQueryContextRef,
        plan: &ReadDataSourcePlan,
        nest_getter: &Arc<Box<dyn GetNodePlan>>,
        cluster_nodes: &Vec<Arc<Node>>,
    ) -> Result<Arc<Box<dyn GetNodePlan>>> {
        let table = ctx.get_table(&plan.db, &plan.table)?;

        if !table.is_local() {
            let new_partitions_size = ctx.get_max_threads()? as usize * cluster_nodes.len();
            let new_read_source_plan = table.read_plan(ctx.clone(), &*plan.scan_plan, new_partitions_size)?;

            // We always put adjacent partitions in the same node
            let new_partitions = &new_read_source_plan.partitions;
            let mut nodes_partitions = HashMap::new();
            let partitions_pre_node = new_partitions.len() / cluster_nodes.len();

            for node_index in 0..cluster_nodes.len() {
                let mut node_partitions = vec![];
                let node_partitions_offset = partitions_pre_node * node_index;

                for partition_index in 0..partitions_pre_node {
                    node_partitions.push((new_partitions[node_partitions_offset + partition_index]).clone());
                }

                if !node_partitions.is_empty() {
                    nodes_partitions.insert(cluster_nodes[node_index].name.clone(), node_partitions);
                }
            }

            // For some irregular partitions, we assign them to the head nodes
            let offset = partitions_pre_node * cluster_nodes.len();
            for index in 0..(new_partitions.len() % cluster_nodes.len()) {
                let node_name = &cluster_nodes[index].name;
                match nodes_partitions.entry(node_name.clone()) {
                    Vacant(entry) => {
                        let mut node_partitions = vec![];
                        node_partitions.push(new_partitions[offset + index].clone());
                        entry.insert(node_partitions);
                    },
                    Occupied(mut entry) => {
                        entry.get_mut().push(new_partitions[offset + index].clone());
                    },
                }
            }

            println!("Reschedule partitions: {:?}", nodes_partitions);

            let nested_getter = RemoteReadSourceGetNodePlan(new_read_source_plan, Arc::new(nodes_partitions), nest_getter.clone());
            return Ok(Arc::new(Box::new(ReadSourceGetNodePlan(Arc::new(Box::new(nested_getter))))))
        }


        let nested_getter = LocalReadSourceGetNodePlan(plan.clone(), nest_getter.clone());
        Ok((Arc::new(Box::new(ReadSourceGetNodePlan(Arc::new(Box::new(nested_getter)))))))
    }
}

impl GetNodePlan for ReadSourceGetNodePlan {
    fn get_plan(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<PlanNode> {
        self.0.get_plan(node_name, cluster_nodes)
    }
}

struct ExecutionPlanBuilder(String, String, StagePlan, Arc<Box<dyn GetNodePlan>>);

impl ExecutionPlanBuilder {
    pub fn create(query_id: String, stage_id: String, plan: &StagePlan, node_plan_getter: &Arc<Box<dyn GetNodePlan>>) -> Arc<Box<ExecutionPlanBuilder>> {
        Arc::new(Box::new(ExecutionPlanBuilder(
            query_id, stage_id, plan.clone(), node_plan_getter.clone(),
        )))
    }

    pub fn build(&self, node_name: &String, cluster_nodes: &Vec<Arc<Node>>) -> Result<Option<ExecutePlanWithShuffleAction>> {
        match self.2.kind {
            StageKind::Expansive => {
                let all_nodes_name = cluster_nodes.iter().map(|node| node.name.clone()).collect::<Vec<_>>();
                for cluster_node in cluster_nodes {
                    if cluster_node.name == *node_name && cluster_node.local {
                        return Ok(Some(ExecutePlanWithShuffleAction {
                            query_id: self.0.clone(),
                            stage_id: self.1.clone(),
                            plan: self.3.get_plan(node_name, cluster_nodes)?,
                            scatters: all_nodes_name.clone(),
                            scatters_action: self.2.scatters_expr.clone(),
                        }));
                    }
                }
                Ok(None)
            },
            StageKind::Convergent => {
                for cluster_node in cluster_nodes {
                    if cluster_node.local {
                        return Ok(Some(ExecutePlanWithShuffleAction {
                            query_id: self.0.clone(),
                            stage_id: self.1.clone(),
                            plan: self.3.get_plan(node_name, cluster_nodes)?,
                            scatters: vec![cluster_node.name.clone()],
                            scatters_action: self.2.scatters_expr.clone(),
                        }))
                    }
                }

                Result::Err(ErrorCodes::NotFoundLocalNode("The PlanScheduler must be in the query cluster"))
            }
            StageKind::Normal => {
                let all_nodes_name = cluster_nodes.iter().map(|node| node.name.clone()).collect::<Vec<_>>();
                Ok(Some(ExecutePlanWithShuffleAction {
                    query_id: self.0.clone(),
                    stage_id: self.1.clone(),
                    plan: self.3.get_plan(node_name, cluster_nodes)?,
                    scatters: all_nodes_name.clone(),
                    scatters_action: self.2.scatters_expr.clone(),
                }))
            }
        }
    }
}
