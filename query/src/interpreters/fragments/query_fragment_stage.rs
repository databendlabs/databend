use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_base::base::GlobalUniqName;
use common_datavalues::DataSchemaRef;
use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::{ErrorCode, Result};
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, RemotePlan, PlanBuilder, PlanNode, PlanRewriter, V1RemotePlan, StageKind, StagePlan};
use crate::api::{DataExchange, HashDataExchange, MergeExchange};
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment_actions::{QueryFragmentAction, QueryFragmentActions, QueryFragmentsActions};
use crate::sessions::QueryContext;

pub struct StageQueryFragment {
    id: String,
    stage: StagePlan,
    ctx: Arc<QueryContext>,
    input: Box<dyn QueryFragment>,
}

impl StageQueryFragment {
    pub fn create(ctx: Arc<QueryContext>, node: &StagePlan, input: Box<dyn QueryFragment>) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(StageQueryFragment { id: GlobalUniqName::unique(), stage: node.clone(), ctx, input }))
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

        if self.input.get_out_partition()? == PartitionState::NotPartition {
            if input_actions.get_actions().is_empty() {
                return Err(ErrorCode::LogicalError("Logical error, input actions is empty."));
            }

            let action = &input_actions.get_actions()[0];
            let fragment_action = QueryFragmentAction::create(
                actions.get_local_executor(),
                self.input.rewrite_remote_plan(&self.stage.input, &action.node)?,
            );

            fragment_actions.add_action(fragment_action);
        } else {
            // We run exchange data on the current hosts
            for action in input_actions.get_actions() {
                let fragment_action = QueryFragmentAction::create(
                    action.executor.clone(),
                    self.input.rewrite_remote_plan(&self.stage.input, &action.node)?,
                );

                fragment_actions.add_action(fragment_action);
            }
        }


        fragment_actions.set_exchange(match self.stage.kind {
            StageKind::Expansive => unimplemented!(),
            StageKind::Merge => MergeExchange::create(actions.get_local_executor()),
            StageKind::Normal => HashDataExchange::create(actions.get_executors(), self.stage.scatters_expr.clone()),
        });

        match input_actions.exchange_actions {
            true => actions.add_fragment_actions(fragment_actions),
            false => actions.update_root_fragment_actions(fragment_actions),
        }
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, _: &PlanNode) -> Result<PlanNode> {
        let query_id = self.ctx.get_id();
        let fragment_id = self.id.clone();
        let mut stage_rewrite = StageRewrite::create(query_id, fragment_id);
        stage_rewrite.rewrite_plan_node(node)
    }
}

struct StageRewrite {
    query_id: String,
    fragment_id: String,
}

impl StageRewrite {
    pub fn create(query_id: String, fragment_id: String) -> StageRewrite {
        StageRewrite { query_id, fragment_id }
    }
}

impl PlanRewriter for StageRewrite {
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

    fn rewrite_stage(&mut self, stage: &StagePlan) -> Result<PlanNode> {
        Ok(PlanNode::Remote(RemotePlan::create_v2(
            stage.schema(),
            self.query_id.to_owned(),
            self.fragment_id.to_owned(),
        )))
    }
}

impl Debug for StageQueryFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StageQueryFragment")
            .field("input", &self.input)
            .finish()
    }
}
