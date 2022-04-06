use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_datavalues::DataSchemaRef;
use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::{ErrorCode, Result};
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, PlanBuilder, PlanNode, PlanRewriter, RemotePlan, StageKind, StagePlan};
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment_actions::{QueryFragmentAction, QueryFragmentActions, QueryFragmentsActions};

pub struct StageQueryFragment {
    id: String,
    stage: StagePlan,
    input: Box<dyn QueryFragment>,
}

impl StageQueryFragment {
    pub fn create(node: &StagePlan, input: Box<dyn QueryFragment>) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(StageQueryFragment { id: "".to_string(), stage: node.clone(), input }))
    }
}

impl QueryFragment for StageQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        match self.stage.kind {
            StageKind::Normal => Ok(PartitionState::HashPartition),
            StageKind::Expansive => Ok(PartitionState::HashPartition),
            StageKind::Merge => Ok(PartitionState::NotPartition)
        }
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(actions)?;
        let input_actions = actions.get_root_actions()?;
        let mut fragment_actions = QueryFragmentActions::create(true);

        // We run exchange data on the current hosts
        for action in input_actions.get_actions() {
            let fragment_action = QueryFragmentAction::create(
                action.executor.clone(),
                PlanNode::Stage(StagePlan {
                    kind: self.stage.kind.clone(),
                    scatters_expr: self.stage.scatters_expr.clone(),
                    input: Arc::new(self.input.rewrite_remote_plan(&self.stage.input, &action.node)?),
                }),
            );
            fragment_actions.add_action(fragment_action);
        }

        match input_actions.exchange_actions {
            true => actions.add_fragment_actions(fragment_actions),
            false => actions.update_root_fragment_actions(fragment_actions),
        }
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, _: &PlanNode) -> Result<PlanNode> {
        let mut stage_rewrite = StageRewrite::create(self.id.clone());
        stage_rewrite.rewrite_plan_node(node)
    }
}

struct StageRewrite {
    fragment_id: String,
    before_group_by_schema: Option<DataSchemaRef>,
}

impl StageRewrite {
    pub fn create(fragment_id: String) -> StageRewrite {
        StageRewrite {
            fragment_id,
            before_group_by_schema: None,
        }
    }
}

impl PlanRewriter for StageRewrite {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        match self.before_group_by_schema {
            Some(_) => Err(ErrorCode::LogicalError("Logical error: before group by schema must be None")),
            None => {
                self.before_group_by_schema = Some(new_input.schema());
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&plan.aggr_expr, &plan.group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema.take() {
            None => Err(ErrorCode::LogicalError("Logical error: before group by schema must be Some")),
            Some(schema_before_group_by) => PlanBuilder::from(&new_input)
                .aggregate_final(schema_before_group_by, &plan.aggr_expr, &plan.group_expr)?
                .build(),
        }
    }

    fn rewrite_stage(&mut self, _: &StagePlan) -> Result<PlanNode> {
        Ok(PlanNode::Remote(RemotePlan::create(self.fragment_id.clone())))
    }
}

impl Debug for StageQueryFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StageQueryFragment")
            .finish()
    }
}
