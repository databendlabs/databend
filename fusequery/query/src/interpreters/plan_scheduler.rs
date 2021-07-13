// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::{EmptyPlan, AggregatorPartialPlan, AggregatorFinalPlan, BroadcastPlan, ProjectionPlan, ExpressionPlan, SubQueriesSetsPlan, FilterPlan, HavingPlan, SortPlan, LimitPlan, LimitByPlan, ScanPlan, SelectPlan, Part};
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::PlanVisitor;
use common_planners::ReadDataSourcePlan;
use common_planners::RemotePlan;
use common_planners::StageKind;
use common_planners::StagePlan;
use common_tracing::tracing;

use crate::api::FlightAction;
use crate::api::ShuffleAction;
use crate::clusters::Node;
use crate::sessions::FuseQueryContextRef;
use crate::datasources::{Table, TablePtr};

pub struct PlanScheduler;

pub struct ScheduledActions {
    pub local_plan: PlanNode,
    pub remote_actions: Vec<(Arc<Node>, FlightAction)>,
}

#[derive(Clone)]
enum ScheduleTask {
    Scheduling(PlanNode),
    Scheduled(FlightAction),
}

struct PlanSchedulerImpl {
    stage_id: String,
    local_node: String,
    cluster_nodes: Vec<String>,
    query_context: FuseQueryContextRef,

    plan_cluster_tasks: HashMap<String, Vec<ScheduleTask>>,
}

impl PlanSchedulerImpl {
    fn local_node_name(&self) -> String {
        self.local_node.clone()
    }

    fn all_cluster_names(&self) -> Vec<String> {
        self.cluster_nodes.clone()
    }

    fn normal_action(&self, stage: &StagePlan, task: ScheduleTask) -> Result<ScheduleTask> {
        match task {
            ScheduleTask::Scheduled(_) => Err(ErrorCode::LogicalError("")),
            ScheduleTask::Scheduling(schedule_plan) => Ok(ScheduleTask::Scheduled(
                FlightAction::PrepareShuffleAction(ShuffleAction {
                    stage_id: self.stage_id.clone(),
                    query_id: self.query_context.get_id(),
                    plan: schedule_plan,
                    sinks: self.all_cluster_names(),
                    scatters_expression: stage.scatters_expr.clone(),
                }),
            )),
        }
    }

    fn normal_remote_plan(&self, node_name: &str, task: &ScheduleTask) -> Result<PlanNode> {
        match task {
            ScheduleTask::Scheduled(FlightAction::PrepareShuffleAction(action)) => {
                Ok(PlanNode::Remote(RemotePlan {
                    schema: action.plan.schema(),
                    query_id: action.query_id.clone(),
                    stage_id: action.stage_id.clone(),
                    stream_id: node_name.to_string(),
                    fetch_nodes: self.all_cluster_names(),
                }))
            }
            _ => Err(ErrorCode::LogicalError("")),
        }
    }

    fn schedule_normal_tasks(&mut self, stage: &StagePlan) -> Result<()> {
        for node_name in &self.cluster_nodes {
            let scheduling_task = self.plan_cluster_tasks
                .get_mut(node_name)
                .and_then(|tasks| tasks.pop());

            assert!(scheduling_task.is_some());
            if let Some(scheduling_task) = scheduling_task {
                let scheduled_task = self.normal_action(stage, scheduling_task)?;
                let remote_plan = self.normal_remote_plan(node_name, &scheduled_task)?;
                if let Some(tasks) = self.plan_cluster_tasks.get_mut(node_name) {
                    tasks.push(scheduled_task);
                    tasks.push(ScheduleTask::Scheduling(remote_plan));
                }
            }
        }

        Ok(())
    }

    fn expansive_action(&self, stage: &StagePlan, task: ScheduleTask) -> Result<ScheduleTask> {
        match task {
            ScheduleTask::Scheduled(_) => Err(ErrorCode::LogicalError("")),
            ScheduleTask::Scheduling(schedule_plan) => Ok(ScheduleTask::Scheduled(
                FlightAction::PrepareShuffleAction(ShuffleAction {
                    stage_id: self.stage_id.clone(),
                    query_id: self.query_context.get_id(),
                    plan: schedule_plan,
                    sinks: self.all_cluster_names(),
                    scatters_expression: stage.scatters_expr.clone(),
                }),
            )),
        }
    }

    fn expansive_remote_plan(&self, node_name: &str, task: &ScheduleTask) -> Result<PlanNode> {
        match task {
            ScheduleTask::Scheduled(FlightAction::PrepareShuffleAction(action)) => {
                Ok(PlanNode::Remote(RemotePlan {
                    schema: action.plan.schema(),
                    query_id: action.query_id.clone(),
                    stage_id: action.stage_id.clone(),
                    stream_id: node_name.to_string(),
                    fetch_nodes: vec![self.local_node_name()],
                }))
            }
            _ => Err(ErrorCode::LogicalError("")),
        }
    }

    fn schedule_expansive_tasks(&mut self, stage: &StagePlan) -> Result<()> {
        let local_node_name = self.local_node_name();
        let local_scheduling_task = self.plan_cluster_tasks
            .get_mut(&local_node_name)
            .and_then(|tasks| tasks.pop());

        assert!(local_scheduling_task.is_some());
        if let Some(local_scheduling_task) = local_scheduling_task {
            let local_scheduled_task = self.expansive_action(stage, local_scheduling_task)?;
            if let Some(tasks) = self.plan_cluster_tasks.get_mut(&local_node_name) {
                tasks.push(local_scheduled_task.clone());
            }

            for node_name in &self.cluster_nodes {
                let remote_plan = self.expansive_remote_plan(node_name, &local_scheduled_task)?;
                if let Some(tasks) = self.plan_cluster_tasks.get_mut(node_name) {
                    tasks.push(ScheduleTask::Scheduling(remote_plan))
                }
            }
        }

        Ok(())
    }

    fn converge_action(&self, stage: &StagePlan, task: ScheduleTask) -> Result<ScheduleTask> {
        match task {
            ScheduleTask::Scheduled(_) => Err(ErrorCode::LogicalError("")),
            ScheduleTask::Scheduling(schedule_plan) => Ok(ScheduleTask::Scheduled(
                FlightAction::PrepareShuffleAction(ShuffleAction {
                    stage_id: self.stage_id.clone(),
                    query_id: self.query_context.get_id(),
                    plan: schedule_plan,
                    sinks: vec![self.local_node_name()],
                    scatters_expression: stage.scatters_expr.clone(),
                }),
            )),
        }
    }

    fn converge_remote_plan(&self, node_name: &str, task: &ScheduleTask) -> Result<PlanNode> {
        match task {
            ScheduleTask::Scheduled(FlightAction::PrepareShuffleAction(action)) => {
                Ok(PlanNode::Remote(RemotePlan {
                    schema: action.plan.schema(),
                    query_id: action.query_id.clone(),
                    stage_id: action.stage_id.clone(),
                    stream_id: node_name.to_string(),
                    fetch_nodes: self.all_cluster_names(),
                }))
            }
            _ => Err(ErrorCode::LogicalError("")),
        }
    }

    fn schedule_converge_tasks(&mut self, stage: &StagePlan) -> Result<()> {
        for node_name in &self.cluster_nodes {
            let scheduling_task = self.plan_cluster_tasks
                .get_mut(node_name)
                .and_then(|tasks| tasks.pop());

            assert!(scheduling_task.is_some());
            if let Some(scheduling_task) = scheduling_task {
                let scheduled_task = self.converge_action(stage, scheduling_task)?;
                if let Some(tasks) = self.plan_cluster_tasks.get_mut(node_name) {
                    tasks.push(scheduled_task);
                }
            }
        }

        let local_node_name = self.local_node_name();
        let local_last_task = self.plan_cluster_tasks
            .get_mut(&local_node_name)
            .and_then(|tasks| tasks.last())
            .map(Clone::clone);

        assert!(local_last_task.is_some());
        let remote_plan = self.converge_remote_plan(&local_node_name, &local_last_task.unwrap())?;
        if let Some(local_tasks) = self.plan_cluster_tasks.get_mut(&local_node_name) {
            local_tasks.push(ScheduleTask::Scheduling(remote_plan))
        }

        Ok(())
    }

    fn schedule_plan_node<F: Fn(PlanNode) -> PlanNode>(&mut self, f: F) -> Result<()> {
        for node_name in &self.cluster_nodes {
            let schedule_task = self.plan_cluster_tasks
                .get_mut(node_name)
                .and_then(|tasks| tasks.pop());

            assert!(schedule_task.is_some());
            if let Some(schedule_task) = schedule_task {
                let schedule_task = match schedule_task {
                    ScheduleTask::Scheduled(action) => ScheduleTask::Scheduled(action),
                    ScheduleTask::Scheduling(input) => ScheduleTask::Scheduling(f(input)),
                };

                if let Some(tasks) = self.plan_cluster_tasks.get_mut(node_name) {
                    tasks.push(schedule_task);
                }
            }
        }

        Ok(())
    }
}

impl PlanVisitor for PlanSchedulerImpl {
    fn visit_subquery_plan(&mut self, subquery_plan: &PlanNode) -> Result<()> {
        // TODO: subquery and scalar subquery
        self.visit_plan_node(subquery_plan)
    }

    fn visit_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::AggregatorPartial(
            AggregatorPartialPlan {
                schema: plan.schema(),
                aggr_expr: plan.aggr_expr.clone(),
                group_expr: plan.group_expr.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::AggregatorFinal(
            AggregatorFinalPlan {
                schema: plan.schema.clone(),
                aggr_expr: plan.aggr_expr.clone(),
                group_expr: plan.group_expr.clone(),
                schema_before_group_by: plan.schema_before_group_by.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_empty(&mut self, plan: &EmptyPlan) -> Result<()> {
        match plan.is_cluster {
            true => self.cluster_empty_plan(plan),
            false => self.local_empty_plan(plan),
        }
    }

    fn visit_stage(&mut self, stage: &StagePlan) -> Result<()> {
        self.visit_plan_node(stage.input.as_ref())?;

        // Entering new stage
        self.stage_id = uuid::Uuid::new_v4().to_string();

        match stage.kind {
            StageKind::Normal => self.schedule_normal_tasks(stage),
            StageKind::Expansive => self.schedule_expansive_tasks(stage),
            StageKind::Convergent => self.schedule_converge_tasks(stage),
        }
    }

    fn visit_broadcast(&mut self, plan: &BroadcastPlan) -> Result<()> {
        // TODO: broadcast action
        self.visit_plan_node(plan.input.as_ref())
    }

    fn visit_projection(&mut self, plan: &ProjectionPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Projection(
            ProjectionPlan {
                expr: plan.expr.clone(),
                schema: plan.schema.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Expression(
            ExpressionPlan {
                desc: plan.desc.clone(),
                exprs: plan.exprs.clone(),
                schema: plan.schema.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_sub_queries_sets(&mut self, plan: &SubQueriesSetsPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.visit_exprs(&plan.expressions)?;

        // TODO: merge multiple expression and input into sub_queries_sets
        self.schedule_plan_node(|input| PlanNode::SubQueryExpression(
            SubQueriesSetsPlan {
                expressions: plan.expressions.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_filter(&mut self, plan: &FilterPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Filter(
            FilterPlan {
                schema: plan.schema.clone(),
                predicate: plan.predicate.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_having(&mut self, plan: &HavingPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Having(
            HavingPlan {
                schema: plan.schema.clone(),
                predicate: plan.predicate.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_sort(&mut self, plan: &SortPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Sort(
            SortPlan {
                schema: plan.schema.clone(),
                order_by: plan.order_by.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_limit(&mut self, plan: &LimitPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Limit(
            LimitPlan {
                n: plan.n.clone(),
                offset: plan.offset.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_limit_by(&mut self, plan: &LimitByPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::LimitBy(
            LimitByPlan {
                limit: plan.limit.clone(),
                limit_by: plan.limit_by.clone(),
                input: Arc::new(input),
            }
        ))
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        let table = self.query_context.get_table(&plan.db, &plan.table)?;

        match table.is_local() {
            true => self.local_read_data_source(plan),
            false => {
                let cluster_source = self.cluster_source(&plan.scan_plan, table)?;
                self.cluster_read_data_source(&cluster_source)
            }
        }
    }

    fn visit_select(&mut self, plan: &SelectPlan) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref())?;
        self.schedule_plan_node(|input| PlanNode::Select(
            SelectPlan {
                input: Arc::new(input),
            }
        ))
    }
}


impl PlanScheduler {
    /// Schedule the plan to Local or Remote mode.
    #[tracing::instrument(level = "info", skip(ctx, plan))]
    pub fn reschedule(ctx: FuseQueryContextRef, plan: &PlanNode) -> Result<ScheduledActions> {
        let cluster = ctx.try_get_cluster()?;

        if cluster.is_empty()? {
            return Ok(ScheduledActions {
                local_plan: plan.clone(),
                remote_actions: vec![],
            });
        }

        let mut scheduler = PlanSchedulerImpl::create(ctx)?;
        scheduler.visit_plan_node(plan)?;

        let local_execute_plan = scheduler.plan_cluster_tasks
            .get_mut(&scheduler.local_node)
            .and_then(|tasks| tasks.pop());

        match local_execute_plan {
            Some(ScheduleTask::Scheduling(local_plan)) => {
                let mut remote_actions = vec![];
                for node_name in scheduler.cluster_nodes {
                    let node = cluster.get_node_by_name(node_name.clone())?;
                    if let Some(node_tasks) = scheduler.plan_cluster_tasks.remove(&node_name) {
                        for node_task in node_tasks {
                            match node_task {
                                ScheduleTask::Scheduling(_) => return Err(ErrorCode::LogicalError("")),
                                ScheduleTask::Scheduled(action) => remote_actions.push((node.clone(), action)),
                            };
                        }
                    }
                }

                Ok(ScheduledActions {
                    local_plan,
                    remote_actions,
                })
            }
            _ => Err(ErrorCode::LogicalError(""))
        }
    }
}

impl PlanSchedulerImpl {
    fn create(context: FuseQueryContextRef) -> Result<PlanSchedulerImpl> {
        let cluster = context.try_get_cluster()?;
        let cluster_nodes = cluster.get_nodes()?;

        let mut local_node = String::from("");
        let mut cluster_tasks = HashMap::new();
        let mut cluster_nodes_name = Vec::with_capacity(cluster_nodes.len());

        for cluster_node in cluster_nodes {
            cluster_nodes_name.push(cluster_node.name.clone());
            cluster_tasks.insert(cluster_node.name.clone(), vec![]);

            if cluster_node.is_local() {
                local_node = cluster_node.name.clone();
            }
        }

        Ok(PlanSchedulerImpl {
            local_node,
            stage_id: String::from(""),
            cluster_nodes: cluster_nodes_name,
            query_context: context,
            plan_cluster_tasks: cluster_tasks,
        })
    }

    fn cluster_source(&mut self, node: &ScanPlan, table: TablePtr) -> Result<ReadDataSourcePlan> {
        let nodes = self.cluster_nodes.clone();
        let context = self.query_context.clone();
        let settings = context.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        table.read_plan(context, node, max_threads * nodes.len())
    }

    fn repartition(&mut self, cluster_source: &ReadDataSourcePlan) -> Vec<Partitions> {
        // We always put adjacent partitions in the same node
        let nodes = self.cluster_nodes.clone();
        let cluster_parts = &cluster_source.parts;
        let parts_pre_node = cluster_parts.len() / nodes.len();

        let mut nodes_parts = Vec::with_capacity(nodes.len());
        for index in 0..nodes.len() {
            let begin = parts_pre_node * index;
            let end = parts_pre_node * (index + 1);
            let node_parts = cluster_parts[begin..end].to_vec();

            if !node_parts.is_empty() {
                nodes_parts.push(node_parts);
            }
        }

        // For some irregular partitions, we assign them to the head nodes
        let mut begin = parts_pre_node * nodes.len();
        let remain_cluster_parts = &cluster_parts[begin..];
        for index in 0..remain_cluster_parts.len() {
            nodes_parts[index].push(remain_cluster_parts[index].clone());
        }

        nodes_parts
    }

    fn cluster_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        let nodes = self.cluster_nodes.clone();
        let nodes_parts = self.repartition(&plan);

        for index in 0..nodes.len() {
            assert!(self.plan_cluster_tasks.contains_key(&nodes[index]));
            if let Some(node_tasks) = self.plan_cluster_tasks.get_mut(&nodes[index]) {
                let mut read_plan = plan.clone();

                read_plan.parts = nodes_parts[index].clone();
                node_tasks.push(ScheduleTask::Scheduling(PlanNode::ReadSource(read_plan)));
            }
        }

        Ok(())
    }

    fn local_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        let local_node_name = self.local_node_name();
        if let Some(tasks) = self.plan_cluster_tasks.get_mut(&local_node_name) {
            tasks.push(ScheduleTask::Scheduling(PlanNode::ReadSource(plan.clone())));
        }

        Ok(())
    }

    fn cluster_empty_plan(&mut self, plan: &EmptyPlan) -> Result<()> {
        let nodes = self.cluster_nodes.clone();
        for index in 0..nodes.len() {
            assert!(self.plan_cluster_tasks.contains_key(&nodes[index]));
            if let Some(node_tasks) = self.plan_cluster_tasks.get_mut(&nodes[index]) {
                node_tasks.push(ScheduleTask::Scheduling(PlanNode::Empty(plan.clone())));
            }
        }

        Ok(())
    }

    fn local_empty_plan(&mut self, plan: &EmptyPlan) -> Result<()> {
        let local_node_name = self.local_node_name();
        if let Some(tasks) = self.plan_cluster_tasks.get_mut(&local_node_name) {
            tasks.push(ScheduleTask::Scheduling(PlanNode::Empty(plan.clone())));
        }

        Ok(())
    }
}
