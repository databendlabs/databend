use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::Result;
use common_planners::PlanNode;
use crate::api::FlightAction;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentsActions;

#[derive(Debug)]
pub struct RootQueryFragment {
    input: Box<dyn QueryFragment>,
}

impl RootQueryFragment {
    pub fn create(input: Box<dyn QueryFragment>) -> Result<Box<dyn QueryFragment>> {
        // let input_partition = input.get_out_partition()?;
        Ok(Box::new(RootQueryFragment { input }))
    }
}

impl QueryFragment for RootQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        Ok(PartitionState::NotPartition)
    }

    fn finalize(&self, nodes: &mut QueryFragmentsActions) -> Result<()> {
        todo!()
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, new: &PlanNode) -> Result<PlanNode> {
        todo!()
    }
}