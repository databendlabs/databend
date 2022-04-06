use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::Result;
use common_planners::PlanNode;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment_actions::{QueryFragmentActions, QueryFragmentsActions};

#[derive(Debug)]
pub struct RootQueryFragment {
    input: Box<dyn QueryFragment>,
}

impl RootQueryFragment {
    pub fn create(input: Box<dyn QueryFragment>) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(RootQueryFragment { input }))
    }
}

impl QueryFragment for RootQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        Ok(PartitionState::NotPartition)
    }

    fn finalize(&self, nodes: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(nodes)?;

        if self.input.get_out_partition()? == PartitionState::NotPartition {
            return Ok(());
        }

        let input_actions = actions.get_root_actions()?;
        let mut fragment_actions = QueryFragmentActions::create(true);


        todo!()

    }

    fn rewrite_remote_plan(&self, node: &PlanNode, new: &PlanNode) -> Result<PlanNode> {
        todo!()
    }
}