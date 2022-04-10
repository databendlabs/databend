use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::Result;
use common_planners::{PlanNode, V1RemotePlan, StageKind, StagePlan};
use crate::api::{DataExchange, MergeExchange};
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment_actions::{QueryFragmentAction, QueryFragmentActions, QueryFragmentsActions};

#[derive(Debug)]
pub struct RootQueryFragment {
    node: PlanNode,
    input: Box<dyn QueryFragment>,
}

impl RootQueryFragment {
    pub fn create(input: Box<dyn QueryFragment>, node: &PlanNode) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(RootQueryFragment { input, node: node.clone() }))
    }
}

impl QueryFragment for RootQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        Ok(PartitionState::NotPartition)
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(actions)?;
        let input_actions = actions.get_root_actions()?;
        let mut fragment_actions = QueryFragmentActions::create(false);

        // This is an implicit stage. We run remaining plans on the current hosts
        for action in input_actions.get_actions() {
            fragment_actions.add_action(QueryFragmentAction::create(
                action.executor.clone(),
                self.input.rewrite_remote_plan(&self.node, &action.node)?,
            ));
        }

        fragment_actions.set_exchange(MergeExchange::create(actions.get_local_executor()));
        match input_actions.exchange_actions {
            true => actions.add_fragment_actions(fragment_actions),
            false => actions.update_root_fragment_actions(fragment_actions),
        }
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, _: &PlanNode) -> Result<PlanNode> {
        // Do nothing, We will not call this method on RootQueryFragment
        Ok(node.clone())
    }
}