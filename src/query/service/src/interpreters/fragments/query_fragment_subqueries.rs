// Copyright 2022 Datafuse Labs.
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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::AggregatorFinalPlan;
use common_legacy_planners::AggregatorPartialPlan;
use common_legacy_planners::Expression;
use common_legacy_planners::Expressions;
use common_legacy_planners::PlanBuilder;
use common_legacy_planners::PlanNode;
use common_legacy_planners::PlanRewriter;
use common_legacy_planners::SubQueriesSetPlan;

use crate::api::FragmentPayload;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment::BuilderVisitor;
use crate::interpreters::fragments::QueryFragment;
use crate::interpreters::QueryFragmentAction;
use crate::interpreters::QueryFragmentActions;
use crate::interpreters::QueryFragmentsActions;
use crate::sessions::QueryContext;

pub struct SubQueriesFragment {
    ctx: Arc<QueryContext>,
    node: SubQueriesSetPlan,
    input: Box<dyn QueryFragment>,
}

impl SubQueriesFragment {
    pub fn create(
        ctx: Arc<QueryContext>,
        node: &SubQueriesSetPlan,
        input: Box<dyn QueryFragment>,
    ) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(SubQueriesFragment {
            ctx,
            input,
            node: node.clone(),
        }))
    }

    fn subquery_fragment(&self, query_plan: &Arc<PlanNode>) -> Result<Box<dyn QueryFragment>> {
        BuilderVisitor::create(self.ctx.clone()).visit(query_plan)
    }

    fn finalize_expressions(&self, actions: &mut QueryFragmentsActions) -> Result<Expressions> {
        let mut expressions = Vec::with_capacity(self.node.expressions.len());

        for expression in &self.node.expressions {
            match expression {
                Expression::Subquery { name, query_plan } => {
                    let ctx = self.ctx.clone();
                    let subquery_fragment = self.subquery_fragment(query_plan)?;
                    let mut fragments_actions = QueryFragmentsActions::create(ctx);
                    subquery_fragment.finalize(&mut fragments_actions)?;

                    let root_actions = fragments_actions.get_root_actions()?;

                    if !root_actions.fragment_actions.is_empty() {
                        let plan = match &root_actions.fragment_actions[0].payload {
                            FragmentPayload::PlanV1(node) => node,
                            FragmentPayload::PlanV2(_) => unreachable!(),
                        };
                        expressions.push(Expression::Subquery {
                            name: name.clone(),
                            query_plan: Arc::new(
                                subquery_fragment.rewrite_remote_plan(query_plan, plan)?,
                            ),
                        });
                    }

                    actions.add_fragments_actions(fragments_actions)?;
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let ctx = self.ctx.clone();
                    let subquery_fragment = self.subquery_fragment(query_plan)?;
                    let mut fragments_actions = QueryFragmentsActions::create(ctx);
                    subquery_fragment.finalize(&mut fragments_actions)?;

                    let root_actions = fragments_actions.get_root_actions()?;

                    if !root_actions.fragment_actions.is_empty() {
                        let plan = match &root_actions.fragment_actions[0].payload {
                            FragmentPayload::PlanV1(node) => node,
                            FragmentPayload::PlanV2(_) => unreachable!(),
                        };
                        expressions.push(Expression::ScalarSubquery {
                            name: name.clone(),
                            query_plan: Arc::new(
                                subquery_fragment.rewrite_remote_plan(query_plan, plan)?,
                            ),
                        });
                    }

                    actions.add_fragments_actions(fragments_actions)?;
                }
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery"),
            };
        }

        Ok(expressions)
    }
}

impl QueryFragment for SubQueriesFragment {
    fn is_distributed_query(&self) -> Result<bool> {
        if self.input.is_distributed_query()? {
            return Ok(true);
        }

        for expression in &self.node.expressions {
            match expression {
                Expression::Subquery { query_plan, .. } => {
                    if self.subquery_fragment(query_plan)?.is_distributed_query()? {
                        return Ok(true);
                    }
                }
                Expression::ScalarSubquery { query_plan, .. } => {
                    if self.subquery_fragment(query_plan)?.is_distributed_query()? {
                        return Ok(true);
                    }
                }
                _ => panic!("Logical error, expressions must be Subquery or ScalarSubquery"),
            };
        }

        Ok(false)
    }

    fn get_out_partition(&self) -> Result<PartitionState> {
        self.input.get_out_partition()
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(actions)?;
        let root_actions = actions.pop_root_actions().unwrap();
        let new_expressions = self.finalize_expressions(actions)?;

        let mut new_root_fragment_actions =
            QueryFragmentActions::create(root_actions.exchange_actions, root_actions.fragment_id);

        for fragment_action in &root_actions.fragment_actions {
            let plan = match &fragment_action.payload {
                FragmentPayload::PlanV1(node) => node,
                FragmentPayload::PlanV2(_) => unreachable!(),
            };
            new_root_fragment_actions.add_action(QueryFragmentAction::create(
                fragment_action.executor.clone(),
                PlanNode::SubQueryExpression(SubQueriesSetPlan {
                    expressions: new_expressions.clone(),
                    input: Arc::new(plan.clone()),
                }),
            ));
        }

        actions.add_fragment_actions(new_root_fragment_actions)?;
        Ok(())
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, new_node: &PlanNode) -> Result<PlanNode> {
        match new_node {
            PlanNode::SubQueryExpression(v) => QueriesRewrite::create(v).rewrite_plan_node(node),
            _ => Err(ErrorCode::UnknownPlan(format!(
                "Unknown plan type while in rewrite_remote_plan, {:?}",
                new_node,
            ))),
        }
    }
}

struct QueriesRewrite<'a> {
    new_node: &'a SubQueriesSetPlan,
    queries: HashMap<String, PlanNode>,
}

impl<'a> QueriesRewrite<'a> {
    pub fn create(new_node: &'a SubQueriesSetPlan) -> QueriesRewrite {
        QueriesRewrite {
            new_node,
            queries: Default::default(),
        }
    }
}

impl<'a> PlanRewriter for QueriesRewrite<'a> {
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

    fn rewrite_subquery_plan(&mut self, subquery_plan: &PlanNode) -> Result<PlanNode> {
        let key = format!("{:?}", subquery_plan);
        if !self.queries.contains_key(&key) {
            return Err(ErrorCode::LogicalError(""));
        }

        Ok(self.queries.get(&key).unwrap().clone())
    }

    fn rewrite_sub_queries_sets(&mut self, plan: &SubQueriesSetPlan) -> Result<PlanNode> {
        assert_eq!(self.new_node.expressions.len(), plan.expressions.len());

        for (left, right) in plan
            .expressions
            .iter()
            .zip(self.new_node.expressions.iter())
        {
            match (left, right) {
                (
                    Expression::Subquery {
                        query_plan: left, ..
                    },
                    Expression::Subquery {
                        query_plan: right, ..
                    },
                ) => {
                    self.queries
                        .insert(format!("{:?}", left), right.as_ref().clone());
                }
                (
                    Expression::ScalarSubquery {
                        query_plan: left, ..
                    },
                    Expression::ScalarSubquery {
                        query_plan: right, ..
                    },
                ) => {
                    self.queries
                        .insert(format!("{:?}", left), right.as_ref().clone());
                }
                _ => panic!(""),
            }
        }

        Ok(self.new_node.input.as_ref().clone())
    }
}

impl Debug for SubQueriesFragment {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
