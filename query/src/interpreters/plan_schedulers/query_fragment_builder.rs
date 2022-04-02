use std::sync::Arc;
use common_datavalues::DataSchemaRef;
use common_exception::{ErrorCode, Result};
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, BroadcastPlan, PlanBuilder, PlanNode, PlanRewriter, StagePlan};
use crate::interpreters::plan_schedulers::query_fragment::{BroadcastQueryFragment, QueryFragment, ShuffleQueryFragment};

pub struct QueryFragmentsBuilder;

impl QueryFragmentsBuilder {
    pub fn build(plan: &PlanNode) -> Result<Vec<Box<dyn QueryFragment>>> {
        let mut visitor = BuilderVisitor::create();
        visitor.rewrite_plan_node(plan)?;
        Ok(visitor.query_fragments)
    }
}

struct BuilderVisitor {
    query_fragments: Vec<Box<dyn QueryFragment>>,
    before_group_by_schema: Option<DataSchemaRef>,
}

impl BuilderVisitor {
    pub fn create() -> BuilderVisitor {
        BuilderVisitor {
            query_fragments: vec![],
            before_group_by_schema: None,
        }
    }
}

impl PlanRewriter for BuilderVisitor {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        match self.before_group_by_schema {
            Some(_) => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be None",
            )),
            None => {
                self.before_group_by_schema = Some(new_input.schema());
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema.take() {
            None => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be Some",
            )),
            Some(schema_before_group_by) => {
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_final(schema_before_group_by, &new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_stage(&mut self, plan: &StagePlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        let stage_plan = StagePlan {
            kind: plan.kind.clone(),
            input: Arc::new(new_input),
            scatters_expr: plan.scatters_expr.clone(),
        };
        let fragment = ShuffleQueryFragment::create(&stage_plan);
        self.query_fragments.push(fragment);
        Ok(PlanNode::Stage(stage_plan))
    }

    fn rewrite_broadcast(&mut self, plan: &BroadcastPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        let broadcast_plan = BroadcastPlan { input: Arc::new(new_input) };
        let fragment = BroadcastQueryFragment::create(&broadcast_plan);
        self.query_fragments.push(fragment);
        Ok(PlanNode::Broadcast(broadcast_plan))
    }
}
