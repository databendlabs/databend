// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_context::IOContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::NodeInfo;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::BroadcastPlan;
use common_planners::EmptyPlan;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::Expressions;
use common_planners::Extras;
use common_planners::FilterPlan;
use common_planners::HavingPlan;
use common_planners::LimitByPlan;
use common_planners::LimitPlan;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RemotePlan;
use common_planners::SelectPlan;
use common_planners::SortPlan;
use common_planners::StageKind;
use common_planners::StagePlan;
use common_planners::SubQueriesSetPlan;
use common_tracing::tracing;

use crate::api::BroadcastAction;
use crate::api::FlightAction;
use crate::api::ShuffleAction;
use crate::catalogs::TablePtr;
use crate::catalogs::ToReadDataSourcePlan;
use crate::sessions::DatabendQueryContext;
use crate::sessions::DatabendQueryContextRef;

enum RunningMode {
    Cluster,
    Standalone,
}

pub struct Tasks {
    plan: PlanNode,
    context: DatabendQueryContextRef,
    actions: HashMap<String, VecDeque<FlightAction>>,
}

pub struct PlanScheduler {
    stage_id: String,
    cluster_nodes: Vec<String>,

    local_pos: usize,
    nodes_plan: Vec<PlanNode>,
    running_mode: RunningMode,
    query_context: DatabendQueryContextRef,
    subqueries_expressions: Vec<Expressions>,
}

impl PlanScheduler {
    pub fn try_create(context: DatabendQueryContextRef) -> Result<PlanScheduler> {
        let cluster = context.get_cluster();
        let cluster_nodes = cluster.get_nodes();

        let mut local_pos = 0;
        let mut nodes_plan = Vec::new();
        let mut cluster_nodes_name = Vec::with_capacity(cluster_nodes.len());
        for index in 0..cluster_nodes.len() {
            if cluster.is_local(cluster_nodes[index].as_ref()) {
                local_pos = index;
            }

            nodes_plan.push(PlanNode::Empty(EmptyPlan::create()));
            cluster_nodes_name.push(cluster_nodes[index].id.clone());
        }

        Ok(PlanScheduler {
            local_pos,
            nodes_plan,
            stage_id: uuid::Uuid::new_v4().to_string(),
            query_context: context,
            subqueries_expressions: vec![],
            cluster_nodes: cluster_nodes_name,
            running_mode: RunningMode::Standalone,
        })
    }

    /// Schedule the plan to Local or Remote mode.
    #[tracing::instrument(level = "info", skip(self, plan))]
    pub fn reschedule(mut self, plan: &PlanNode) -> Result<Tasks> {
        let context = self.query_context.clone();
        let cluster = context.get_cluster();
        let mut tasks = Tasks::create(context);

        match cluster.is_empty() {
            true => tasks.finalize(plan),
            false => {
                self.visit_plan_node(plan, &mut tasks)?;
                tasks.finalize(&self.nodes_plan[self.local_pos])
            }
        }
    }
}

impl Tasks {
    pub fn create(context: DatabendQueryContextRef) -> Tasks {
        Tasks {
            context,
            actions: HashMap::new(),
            plan: PlanNode::Empty(EmptyPlan::create()),
        }
    }

    pub fn get_local_task(&self) -> PlanNode {
        self.plan.clone()
    }

    pub fn finalize(mut self, plan: &PlanNode) -> Result<Self> {
        self.plan = plan.clone();
        Ok(self)
    }

    pub fn get_tasks(&self) -> Result<Vec<(Arc<NodeInfo>, FlightAction)>> {
        let cluster = self.context.get_cluster();

        let mut tasks = Vec::new();
        for cluster_node in &cluster.get_nodes() {
            if let Some(actions) = self.actions.get(&cluster_node.id) {
                for action in actions {
                    tasks.push((cluster_node.clone(), action.clone()));
                }
            }
        }

        Ok(tasks)
    }

    #[allow(clippy::ptr_arg)]
    pub fn add_task(&mut self, node_name: &String, action: FlightAction) {
        match self.actions.entry(node_name.to_string()) {
            Entry::Occupied(mut entry) => entry.get_mut().push_back(action),
            Entry::Vacant(entry) => {
                let mut node_tasks = VecDeque::new();
                node_tasks.push_back(action);
                entry.insert(node_tasks);
            }
        };
    }
}

impl PlanScheduler {
    fn normal_action(&self, stage: &StagePlan, input: &PlanNode) -> ShuffleAction {
        ShuffleAction {
            stage_id: self.stage_id.clone(),
            query_id: self.query_context.get_id(),
            plan: input.clone(),
            sinks: self.cluster_nodes.clone(),
            scatters_expression: stage.scatters_expr.clone(),
        }
    }

    fn normal_remote_plan(&self, node_name: &str, action: &ShuffleAction) -> RemotePlan {
        RemotePlan {
            schema: action.plan.schema(),
            query_id: action.query_id.clone(),
            stage_id: action.stage_id.clone(),
            stream_id: node_name.to_string(),
            fetch_nodes: self.cluster_nodes.clone(),
        }
    }

    fn schedule_normal_tasks(&mut self, stage: &StagePlan, tasks: &mut Tasks) -> Result<()> {
        if let RunningMode::Standalone = self.running_mode {
            return Err(ErrorCode::LogicalError(
                "Normal stage cannot work on standalone mode",
            ));
        }

        for index in 0..self.nodes_plan.len() {
            let node_name = &self.cluster_nodes[index];
            let shuffle_action = self.normal_action(stage, &self.nodes_plan[index]);
            let remote_plan_node = self.normal_remote_plan(node_name, &shuffle_action);
            let shuffle_flight_action = FlightAction::PrepareShuffleAction(shuffle_action);

            tasks.add_task(node_name, shuffle_flight_action);
            self.nodes_plan[index] = PlanNode::Remote(remote_plan_node);
        }

        Ok(())
    }

    fn expansive_action(&self, stage: &StagePlan, input: &PlanNode) -> ShuffleAction {
        ShuffleAction {
            stage_id: self.stage_id.clone(),
            query_id: self.query_context.get_id(),
            plan: input.clone(),
            sinks: self.cluster_nodes.clone(),
            scatters_expression: stage.scatters_expr.clone(),
        }
    }

    fn expansive_remote_plan(&self, node_name: &str, action: &ShuffleAction) -> PlanNode {
        PlanNode::Remote(RemotePlan {
            schema: action.plan.schema(),
            query_id: action.query_id.clone(),
            stage_id: action.stage_id.clone(),
            stream_id: node_name.to_string(),
            fetch_nodes: vec![self.cluster_nodes[self.local_pos].clone()],
        })
    }

    fn schedule_expansive_tasks(&mut self, stage: &StagePlan, tasks: &mut Tasks) -> Result<()> {
        if let RunningMode::Cluster = self.running_mode {
            return Err(ErrorCode::LogicalError(
                "Expansive stage cannot work on Cluster mode",
            ));
        }

        self.running_mode = RunningMode::Cluster;
        let node_name = &self.cluster_nodes[self.local_pos];
        let shuffle_action = self.expansive_action(stage, &self.nodes_plan[self.local_pos]);
        tasks.add_task(
            node_name,
            FlightAction::PrepareShuffleAction(shuffle_action.clone()),
        );

        for index in 0..self.nodes_plan.len() {
            let node_name = &self.cluster_nodes[index];
            self.nodes_plan[index] = self.expansive_remote_plan(node_name, &shuffle_action);
        }

        Ok(())
    }

    fn converge_action(&self, stage: &StagePlan, input: &PlanNode) -> ShuffleAction {
        ShuffleAction {
            stage_id: self.stage_id.clone(),
            query_id: self.query_context.get_id(),
            plan: input.clone(),
            sinks: vec![self.cluster_nodes[self.local_pos].clone()],
            scatters_expression: stage.scatters_expr.clone(),
        }
    }

    fn converge_remote_plan(&self, node_name: &str, stage: &StagePlan) -> RemotePlan {
        RemotePlan {
            schema: stage.schema(),
            stage_id: self.stage_id.clone(),
            query_id: self.query_context.get_id(),
            stream_id: node_name.to_string(),
            fetch_nodes: self.cluster_nodes.clone(),
        }
    }

    fn schedule_converge_tasks(&mut self, stage: &StagePlan, tasks: &mut Tasks) -> Result<()> {
        if let RunningMode::Standalone = self.running_mode {
            return Err(ErrorCode::LogicalError(
                "Converge stage cannot work on standalone mode",
            ));
        }

        for index in 0..self.nodes_plan.len() {
            let node_name = &self.cluster_nodes[index];
            let shuffle_action = self.converge_action(stage, &self.nodes_plan[index]);
            let shuffle_flight_action = FlightAction::PrepareShuffleAction(shuffle_action);

            tasks.add_task(node_name, shuffle_flight_action);
        }

        self.running_mode = RunningMode::Standalone;
        let node_name = &self.cluster_nodes[self.local_pos];
        let remote_plan_node = self.converge_remote_plan(node_name, stage);
        self.nodes_plan[self.local_pos] = PlanNode::Remote(remote_plan_node);

        Ok(())
    }
}

impl PlanScheduler {
    fn visit_plan_node(&mut self, node: &PlanNode, tasks: &mut Tasks) -> Result<()> {
        match node {
            PlanNode::AggregatorPartial(plan) => self.visit_aggr_part(plan, tasks),
            PlanNode::AggregatorFinal(plan) => self.visit_aggr_final(plan, tasks),
            PlanNode::Empty(plan) => self.visit_empty(plan, tasks),
            PlanNode::Projection(plan) => self.visit_projection(plan, tasks),
            PlanNode::Filter(plan) => self.visit_filter(plan, tasks),
            PlanNode::Sort(plan) => self.visit_sort(plan, tasks),
            PlanNode::Limit(plan) => self.visit_limit(plan, tasks),
            PlanNode::LimitBy(plan) => self.visit_limit_by(plan, tasks),
            PlanNode::ReadSource(plan) => self.visit_data_source(plan, tasks),
            PlanNode::Select(plan) => self.visit_select(plan, tasks),
            PlanNode::Stage(plan) => self.visit_stage(plan, tasks),
            PlanNode::Broadcast(plan) => self.visit_broadcast(plan, tasks),
            PlanNode::Having(plan) => self.visit_having(plan, tasks),
            PlanNode::Expression(plan) => self.visit_expression(plan, tasks),
            PlanNode::SubQueryExpression(plan) => self.visit_subqueries_set(plan, tasks),
            _ => Err(ErrorCode::UnImplement("")),
        }
    }

    fn visit_aggr_part(&mut self, plan: &AggregatorPartialPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_aggr_part(plan),
            RunningMode::Standalone => self.visit_local_aggr_part(plan),
        }
        Ok(())
    }

    fn visit_local_aggr_part(&mut self, plan: &AggregatorPartialPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::AggregatorPartial(AggregatorPartialPlan {
            schema: plan.schema(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_aggr_part(&mut self, plan: &AggregatorPartialPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::AggregatorPartial(AggregatorPartialPlan {
                schema: plan.schema(),
                aggr_expr: plan.aggr_expr.clone(),
                group_expr: plan.group_expr.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_aggr_final(&mut self, plan: &AggregatorFinalPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;

        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_aggr_final(plan),
            RunningMode::Standalone => self.visit_local_aggr_final(plan),
        };
        Ok(())
    }

    fn visit_local_aggr_final(&mut self, plan: &AggregatorFinalPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::AggregatorFinal(AggregatorFinalPlan {
            schema: plan.schema.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            schema_before_group_by: plan.schema_before_group_by.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        })
    }

    fn visit_cluster_aggr_final(&mut self, plan: &AggregatorFinalPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::AggregatorFinal(AggregatorFinalPlan {
                schema: plan.schema.clone(),
                aggr_expr: plan.aggr_expr.clone(),
                group_expr: plan.group_expr.clone(),
                schema_before_group_by: plan.schema_before_group_by.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            })
        }
    }

    fn visit_empty(&mut self, plan: &EmptyPlan, _: &mut Tasks) -> Result<()> {
        match plan {
            EmptyPlan {
                is_cluster: true, ..
            } => self.visit_cluster_empty(plan),
            EmptyPlan {
                is_cluster: false, ..
            } => self.visit_local_empty(plan),
        };
        Ok(())
    }

    fn visit_local_empty(&mut self, origin: &EmptyPlan) {
        self.running_mode = RunningMode::Standalone;
        self.nodes_plan[self.local_pos] = PlanNode::Empty(origin.clone());
    }

    fn visit_cluster_empty(&mut self, origin: &EmptyPlan) {
        self.running_mode = RunningMode::Cluster;
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Empty(EmptyPlan {
                schema: origin.schema.clone(),
                is_cluster: origin.is_cluster,
            })
        }
    }

    fn visit_stage(&mut self, stage: &StagePlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(stage.input.as_ref(), tasks)?;

        // Entering new stage
        self.stage_id = uuid::Uuid::new_v4().to_string();

        match stage.kind {
            StageKind::Normal => self.schedule_normal_tasks(stage, tasks),
            StageKind::Expansive => self.schedule_expansive_tasks(stage, tasks),
            StageKind::Convergent => self.schedule_converge_tasks(stage, tasks),
        }
    }

    fn visit_broadcast(&mut self, plan: &BroadcastPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;

        // Entering new stage
        self.stage_id = uuid::Uuid::new_v4().to_string();

        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_broadcast(tasks),
            RunningMode::Standalone => self.visit_local_broadcast(tasks),
        };

        Ok(())
    }

    fn broadcast_action(&self, input: &PlanNode) -> BroadcastAction {
        BroadcastAction {
            stage_id: self.stage_id.clone(),
            query_id: self.query_context.get_id(),
            plan: input.clone(),
            sinks: self.cluster_nodes.clone(),
        }
    }

    fn broadcast_remote(&self, node_name: &str, action: &BroadcastAction) -> RemotePlan {
        RemotePlan {
            schema: action.plan.schema(),
            query_id: action.query_id.clone(),
            stage_id: action.stage_id.clone(),
            stream_id: node_name.to_string(),
            fetch_nodes: self.cluster_nodes.clone(),
        }
    }

    fn visit_local_broadcast(&mut self, tasks: &mut Tasks) {
        self.running_mode = RunningMode::Cluster;
        let node_name = &self.cluster_nodes[self.local_pos];
        let action = self.broadcast_action(&self.nodes_plan[self.local_pos]);
        tasks.add_task(node_name, FlightAction::BroadcastAction(action.clone()));

        for index in 0..self.nodes_plan.len() {
            let node_name = &self.cluster_nodes[index];
            self.nodes_plan[index] = PlanNode::Remote(RemotePlan {
                schema: action.plan.schema(),
                query_id: action.query_id.clone(),
                stage_id: action.stage_id.clone(),
                stream_id: node_name.to_string(),
                fetch_nodes: vec![self.cluster_nodes[self.local_pos].clone()],
            });
        }
    }

    fn visit_cluster_broadcast(&mut self, tasks: &mut Tasks) {
        self.running_mode = RunningMode::Cluster;
        for index in 0..self.nodes_plan.len() {
            let node_name = &self.cluster_nodes[index];
            let action = self.broadcast_action(&self.nodes_plan[index]);
            let remote_plan_node = self.broadcast_remote(node_name, &action);

            tasks.add_task(node_name, FlightAction::BroadcastAction(action));
            self.nodes_plan[index] = PlanNode::Remote(remote_plan_node);
        }
    }

    fn visit_projection(&mut self, plan: &ProjectionPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_projection(plan),
            RunningMode::Standalone => self.visit_local_projection(plan),
        };
        Ok(())
    }

    fn visit_local_projection(&mut self, plan: &ProjectionPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Projection(ProjectionPlan {
            schema: plan.schema.clone(),
            expr: plan.expr.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        })
    }

    fn visit_cluster_projection(&mut self, plan: &ProjectionPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Projection(ProjectionPlan {
                schema: plan.schema.clone(),
                expr: plan.expr.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            })
        }
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_expression(plan),
            RunningMode::Standalone => self.visit_local_expression(plan),
        };
        Ok(())
    }

    fn visit_local_expression(&mut self, plan: &ExpressionPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Expression(ExpressionPlan {
            desc: plan.desc.clone(),
            exprs: plan.exprs.clone(),
            schema: plan.schema.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_expression(&mut self, plan: &ExpressionPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Expression(ExpressionPlan {
                desc: plan.desc.clone(),
                exprs: plan.exprs.clone(),
                schema: plan.schema.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_subqueries_set(&mut self, plan: &SubQueriesSetPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;

        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_subqueries(&plan.expressions, tasks),
            RunningMode::Standalone => self.visit_local_subqueries(&plan.expressions, tasks),
        }
    }

    fn visit_local_subqueries(&mut self, exprs: &[Expression], tasks: &mut Tasks) -> Result<()> {
        self.visit_subqueries(exprs, tasks)?;

        if self.subqueries_expressions.len() != self.nodes_plan.len() {
            return Err(ErrorCode::LogicalError(
                "New subqueries size miss match nodes plan",
            ));
        }

        let new_expressions = self.subqueries_expressions[self.local_pos].clone();

        if new_expressions.len() != exprs.len() {
            return Err(ErrorCode::LogicalError(
                "New expression size miss match exprs",
            ));
        }

        self.nodes_plan[self.local_pos] = PlanNode::SubQueryExpression(SubQueriesSetPlan {
            expressions: new_expressions,
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });

        Ok(())
    }

    fn visit_cluster_subqueries(&mut self, exprs: &[Expression], tasks: &mut Tasks) -> Result<()> {
        self.visit_subqueries(exprs, tasks)?;

        if self.subqueries_expressions.len() != self.nodes_plan.len() {
            return Err(ErrorCode::LogicalError(
                "New subqueries size miss match nodes plan",
            ));
        }

        for index in 0..self.nodes_plan.len() {
            let new_expressions = self.subqueries_expressions[index].clone();

            if new_expressions.len() != exprs.len() {
                return Err(ErrorCode::LogicalError(
                    "New expression size miss match exprs",
                ));
            }

            self.nodes_plan[index] = PlanNode::SubQueryExpression(SubQueriesSetPlan {
                expressions: new_expressions,
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }

        Ok(())
    }

    fn visit_subqueries(&mut self, exprs: &[Expression], tasks: &mut Tasks) -> Result<()> {
        self.subqueries_expressions.clear();
        for expression in exprs {
            match expression {
                Expression::Subquery { name, query_plan } => {
                    let subquery = query_plan.as_ref();
                    let subquery_nodes_plan = self.visit_subquery(subquery, tasks)?;

                    for index in 0..subquery_nodes_plan.len() {
                        let new_expression = Expression::Subquery {
                            name: name.clone(),
                            query_plan: Arc::new(subquery_nodes_plan[index].clone()),
                        };

                        match index < self.subqueries_expressions.len() {
                            true => self.subqueries_expressions[index].push(new_expression),
                            false => self.subqueries_expressions.push(vec![new_expression]),
                        };
                    }
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery = query_plan.as_ref();
                    let subquery_nodes_plan = self.visit_subquery(subquery, tasks)?;

                    for index in 0..subquery_nodes_plan.len() {
                        let new_expression = Expression::ScalarSubquery {
                            name: name.clone(),
                            query_plan: Arc::new(subquery_nodes_plan[index].clone()),
                        };

                        match index < self.subqueries_expressions.len() {
                            true => self.subqueries_expressions.push(vec![new_expression]),
                            false => self.subqueries_expressions[index].push(new_expression),
                        };
                    }
                }
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    fn visit_subquery(&mut self, plan: &PlanNode, tasks: &mut Tasks) -> Result<Vec<PlanNode>> {
        let subquery_context = DatabendQueryContext::new(self.query_context.clone());
        let mut subquery_scheduler = PlanScheduler::try_create(subquery_context)?;
        subquery_scheduler.visit_plan_node(plan, tasks)?;
        Ok(subquery_scheduler.nodes_plan)
    }

    fn visit_filter(&mut self, plan: &FilterPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_filter(plan),
            RunningMode::Standalone => self.visit_local_filter(plan),
        };
        Ok(())
    }

    fn visit_local_filter(&mut self, plan: &FilterPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Filter(FilterPlan {
            schema: plan.schema.clone(),
            predicate: plan.predicate.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_filter(&mut self, plan: &FilterPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Filter(FilterPlan {
                schema: plan.schema.clone(),
                predicate: plan.predicate.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_having(&mut self, plan: &HavingPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_having(plan),
            RunningMode::Standalone => self.visit_local_having(plan),
        };
        Ok(())
    }

    fn visit_local_having(&mut self, plan: &HavingPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Having(HavingPlan {
            schema: plan.schema.clone(),
            predicate: plan.predicate.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_having(&mut self, plan: &HavingPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Having(HavingPlan {
                schema: plan.schema.clone(),
                predicate: plan.predicate.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_sort(&mut self, plan: &SortPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_sort(plan),
            RunningMode::Standalone => self.visit_local_sort(plan),
        };
        Ok(())
    }

    fn visit_local_sort(&mut self, plan: &SortPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Sort(SortPlan {
            schema: plan.schema.clone(),
            order_by: plan.order_by.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_sort(&mut self, plan: &SortPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Sort(SortPlan {
                schema: plan.schema.clone(),
                order_by: plan.order_by.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_limit(&mut self, plan: &LimitPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_limit(plan),
            RunningMode::Standalone => self.visit_local_limit(plan),
        };
        Ok(())
    }

    fn visit_local_limit(&mut self, plan: &LimitPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Limit(LimitPlan {
            n: plan.n,
            offset: plan.offset,
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_limit(&mut self, plan: &LimitPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Limit(LimitPlan {
                n: plan.n,
                offset: plan.offset,
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_limit_by(&mut self, plan: &LimitByPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_limit_by(plan),
            RunningMode::Standalone => self.visit_local_limit_by(plan),
        };
        Ok(())
    }

    fn visit_local_limit_by(&mut self, plan: &LimitByPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::LimitBy(LimitByPlan {
            limit: plan.limit,
            limit_by: plan.limit_by.clone(),
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_limit_by(&mut self, plan: &LimitByPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::LimitBy(LimitByPlan {
                limit: plan.limit,
                limit_by: plan.limit_by.clone(),
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }

    fn visit_data_source(&mut self, plan: &ReadDataSourcePlan, _: &mut Tasks) -> Result<()> {
        let table = self.query_context.build_table_from_source_plan(plan)?;

        match table.is_local() {
            true => self.visit_local_data_source(plan),
            false => {
                let cluster_source = self.cluster_source(&plan.push_downs, table.clone())?;
                self.visit_cluster_data_source(&cluster_source)
            }
        }
    }

    fn visit_local_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        self.running_mode = RunningMode::Standalone;
        self.nodes_plan[self.local_pos] = PlanNode::ReadSource(plan.clone());
        Ok(())
    }

    fn visit_cluster_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        self.running_mode = RunningMode::Cluster;
        let nodes_parts = self.repartition(plan);

        for index in 0..self.nodes_plan.len() {
            let mut read_plan = plan.clone();
            read_plan.parts = nodes_parts[index].clone();
            self.nodes_plan[index] = PlanNode::ReadSource(read_plan);
        }

        Ok(())
    }

    fn visit_select(&mut self, plan: &SelectPlan, tasks: &mut Tasks) -> Result<()> {
        self.visit_plan_node(plan.input.as_ref(), tasks)?;
        match self.running_mode {
            RunningMode::Cluster => self.visit_cluster_select(plan),
            RunningMode::Standalone => self.visit_local_select(plan),
        };
        Ok(())
    }

    fn visit_local_select(&mut self, _: &SelectPlan) {
        self.nodes_plan[self.local_pos] = PlanNode::Select(SelectPlan {
            input: Arc::new(self.nodes_plan[self.local_pos].clone()),
        });
    }

    fn visit_cluster_select(&mut self, _: &SelectPlan) {
        for index in 0..self.nodes_plan.len() {
            self.nodes_plan[index] = PlanNode::Select(SelectPlan {
                input: Arc::new(self.nodes_plan[index].clone()),
            });
        }
    }
}

impl PlanScheduler {
    fn cluster_source(
        &mut self,
        push_downs: &Option<Extras>,
        table: TablePtr,
    ) -> Result<ReadDataSourcePlan> {
        let io_ctx = self.query_context.get_cluster_table_io_context()?;
        let io_ctx = Arc::new(io_ctx);

        table.read_plan(
            io_ctx.clone(),
            push_downs.clone(),
            Some(io_ctx.get_max_threads() * io_ctx.get_query_node_ids().len()),
        )
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

            nodes_parts.push(node_parts);
        }

        // For some irregular partitions, we assign them to the head nodes
        let begin = parts_pre_node * nodes.len();
        let remain_cluster_parts = &cluster_parts[begin..];
        for index in 0..remain_cluster_parts.len() {
            nodes_parts[index].push(remain_cluster_parts[index].clone());
        }

        nodes_parts
    }
}
