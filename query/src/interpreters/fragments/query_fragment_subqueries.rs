use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_datavalues::DataSchemaRef;
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, AlterTableClusterKeyPlan, AlterUserPlan, AlterUserUDFPlan, AlterViewPlan, BroadcastPlan, CallPlan, CopyPlan, CreateDatabasePlan, CreateRolePlan, CreateTablePlan, CreateUserPlan, CreateUserStagePlan, CreateUserUDFPlan, CreateViewPlan, DeletePlan, DescribeTablePlan, DescribeUserStagePlan, DropDatabasePlan, DropRolePlan, DropTableClusterKeyPlan, DropTablePlan, DropUserPlan, DropUserStagePlan, DropUserUDFPlan, DropViewPlan, EmptyPlan, ExistsTablePlan, ExplainPlan, Expression, ExpressionPlan, Expressions, FilterPlan, GrantPrivilegePlan, GrantRolePlan, HavingPlan, InsertPlan, KillPlan, LimitByPlan, LimitPlan, ListPlan, OptimizeTablePlan, PlanBuilder, PlanNode, PlanRewriter, ProjectionPlan, ReadDataSourcePlan, RemotePlan, RemoveUserStagePlan, RenameDatabasePlan, RenameTablePlan, RevokePrivilegePlan, RevokeRolePlan, SelectPlan, SettingPlan, ShowCreateDatabasePlan, ShowCreateTablePlan, ShowPlan, SinkPlan, SortPlan, SubQueriesSetPlan, TruncateTablePlan, UndropDatabasePlan, UndropTablePlan, UseDatabasePlan, WindowFuncPlan};
use crate::interpreters::fragments::QueryFragment;
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment::BuilderVisitor;
use crate::interpreters::{QueryFragmentAction, QueryFragmentActions, QueryFragmentsActions};

pub struct SubQueriesFragment {
    node: SubQueriesSetPlan,
    ctx: Arc<QueryContext>,
    input: Box<dyn QueryFragment>,
    subqueries_fragment: HashMap<String, Box<dyn QueryFragment>>,
}

impl SubQueriesFragment {
    pub fn create(
        ctx: Arc<QueryContext>,
        node: &SubQueriesSetPlan,
        input: Box<dyn QueryFragment>,
        subqueries_fragment: HashMap<String, Box<dyn QueryFragment>>,
    ) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(SubQueriesFragment {
            ctx,
            input,
            subqueries_fragment,
            node: node.clone(),
        }))
    }

    fn subquery_fragment(&self, query_plan: &Arc<PlanNode>) -> Result<Box<dyn QueryFragment>> {
        BuilderVisitor::create(self.ctx.clone()).visit(query_plan)
    }

    fn finalize_expressions(&self, actions: &mut QueryFragmentsActions) -> Result<HashMap<String, HashMap<String, PlanNode>>> {
        let mut executor_subqueries = HashMap::new();

        for expression in &self.node.expressions {
            match expression {
                Expression::Subquery { name, query_plan } => {
                    let subquery_fragment = self.subquery_fragment(query_plan)?;

                    let ctx = self.ctx.clone();
                    let mut fragments_actions = QueryFragmentsActions::create(ctx);
                    subquery_fragment.finalize(&mut fragments_actions)?;

                    while !fragments_actions.fragments_actions.is_empty() {
                        let fragment_actions = fragments_actions.fragments_actions.remove(0);

                        if subquery_fragment.get_out_partition()? == PartitionState::Broadcast {
                            let fragment_action = &fragment_actions.fragment_actions[0];
                            for executor in &actions.get_executors() {
                                let action_node = subquery_fragment.rewrite_remote_plan(query_plan, &fragment_action.node)?;

                                if !executor_subqueries.contains_key(executor) {
                                    executor_subqueries.insert(executor.clone(), HashMap::new());
                                }

                                executor_subqueries
                                    .get_mut(executor)
                                    .unwrap().insert(
                                    name.clone(),
                                    action_node,
                                );
                            }
                        } else {
                            for fragment_action in &fragment_actions.fragment_actions {
                                let action_node = subquery_fragment.rewrite_remote_plan(query_plan, &fragment_action.node)?;

                                if !executor_subqueries.contains_key(&fragment_action.executor) {
                                    executor_subqueries.insert(
                                        fragment_action.executor.clone(),
                                        HashMap::new(),
                                    );
                                }

                                executor_subqueries
                                    .get_mut(&fragment_action.executor)
                                    .unwrap().insert(
                                    name.clone(),
                                    action_node,
                                );
                            }
                        }

                        actions.add_fragment_actions(fragment_actions);
                    }
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery_fragment = self.subquery_fragment(query_plan)?;

                    let ctx = self.ctx.clone();
                    let mut fragments_actions = QueryFragmentsActions::create(ctx);
                    subquery_fragment.finalize(&mut fragments_actions)?;

                    while !fragments_actions.fragments_actions.is_empty() {
                        let fragment_actions = fragments_actions.fragments_actions.remove(0);

                        if subquery_fragment.get_out_partition()? == PartitionState::Broadcast {
                            let fragment_action = &fragment_actions.fragment_actions[0];
                            for executor in &actions.get_executors() {
                                let action_node = subquery_fragment.rewrite_remote_plan(query_plan, &fragment_action.node)?;

                                if !executor_subqueries.contains_key(executor) {
                                    executor_subqueries.insert(executor.clone(), HashMap::new());
                                }

                                executor_subqueries
                                    .get_mut(executor)
                                    .unwrap().insert(
                                    name.clone(),
                                    action_node,
                                );
                            }
                        } else {
                            for fragment_action in &fragment_actions.fragment_actions {
                                let action_node = subquery_fragment.rewrite_remote_plan(query_plan, &fragment_action.node)?;

                                if !executor_subqueries.contains_key(&fragment_action.executor) {
                                    executor_subqueries.insert(
                                        fragment_action.executor.clone(),
                                        HashMap::new(),
                                    );
                                }

                                executor_subqueries
                                    .get_mut(&fragment_action.executor)
                                    .unwrap().insert(
                                    name.clone(),
                                    action_node,
                                );
                            }
                        }

                        actions.add_fragment_actions(fragment_actions);
                    }
                }
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery")
            };
        }

        Ok(executor_subqueries)
    }

    fn new_expressions(&self, queries: &mut HashMap<String, PlanNode>) -> Result<Vec<Expression>> {
        let mut new_expressions = Vec::with_capacity(self.node.expressions.len());
        for expression in &self.node.expressions {
            new_expressions.push(match expression {
                Expression::Subquery { name, .. } => Expression::Subquery {
                    name: name.clone(),
                    query_plan: Arc::new(queries.remove(name).unwrap()),
                },
                Expression::ScalarSubquery { name, .. } => Expression::ScalarSubquery {
                    name: name.clone(),
                    query_plan: Arc::new(queries.remove(name).unwrap()),
                },
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery")
            });
        }

        Ok(new_expressions)
    }
}

impl QueryFragment for SubQueriesFragment {
    fn distribute_query(&self) -> Result<bool> {
        if self.input.distribute_query()? {
            return Ok(true);
        }

        for (_name, fragment) in &self.subqueries_fragment {
            if fragment.distribute_query()? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_out_partition(&self) -> Result<PartitionState> {
        self.input.get_out_partition()
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        // TODO: random shuffle if input is not partitions(impossible?)
        let input_state = self.input.get_out_partition()?;
        assert!(!matches!(input_state, PartitionState::NotPartition | PartitionState::Broadcast));

        self.input.finalize(actions)?;
        let root_actions = actions.pop_root_actions().unwrap();
        let mut executors_subqueries = self.finalize_expressions(actions)?;

        let mut new_root_fragment_actions = QueryFragmentActions::create(
            root_actions.exchange_actions,
            root_actions.fragment_id,
        );

        println!("executors_subqueries : {:?}", executors_subqueries);
        for fragment_action in &root_actions.fragment_actions {
            println!("fragment_action executor : {:?}", fragment_action.executor);
            if let Some(mut subqueries) = executors_subqueries.remove(&fragment_action.executor) {
                let new_expressions = self.new_expressions(&mut subqueries)?;

                new_root_fragment_actions.add_action(QueryFragmentAction::create(
                    fragment_action.executor.clone(),
                    PlanNode::SubQueryExpression(SubQueriesSetPlan {
                        expressions: new_expressions,
                        input: Arc::new(fragment_action.node.clone()),
                    }),
                ));
            }
        }

        assert!(executors_subqueries.is_empty());
        actions.add_fragment_actions(new_root_fragment_actions)?;
        Ok(())
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, new_node: &PlanNode) -> Result<PlanNode> {
        if !matches!(new_node, PlanNode::SubQueryExpression(_)) {
            return Err(ErrorCode::UnknownPlan(
                format!(
                    "Unknown plan type while in rewrite_remote_plan, {:?}",
                    new_node,
                )
            ));
        }

        // use new node replace node children.
        SubqueriesRewrite { new_node }.rewrite_plan_node(node)
    }
}


struct SubqueriesRewrite<'a> {
    new_node: &'a PlanNode,
}

impl<'a> SubqueriesRewrite<'a> {
    pub fn create(new_node: &'a PlanNode) -> SubqueriesRewrite {
        SubqueriesRewrite { new_node }
    }
}

impl<'a> PlanRewriter for SubqueriesRewrite<'a> {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        PlanBuilder::from(&self.rewrite_plan_node(&plan.input)?)
            .aggregate_partial(&plan.aggr_expr, &plan.group_expr)?
            .build()
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let schema = plan.schema_before_group_by.clone();
        let new_input = self.rewrite_plan_node(&plan.input)?;

        PlanBuilder::from(&new_input)
            .aggregate_final(schema, &plan.aggr_expr, &plan.group_expr)?
            .build()
    }

    fn rewrite_sub_queries_sets(&mut self, plan: &SubQueriesSetPlan) -> Result<PlanNode> {
        Ok(self.new_node.clone())
    }
}

impl Debug for SubQueriesFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
