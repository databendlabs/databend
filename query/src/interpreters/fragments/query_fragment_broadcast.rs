use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;

use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment::QueryFragment;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentsActions;

#[derive(Debug)]
pub struct BroadcastQueryFragment {
    input: Box<dyn QueryFragment>,
}

impl BroadcastQueryFragment {
    pub fn create(input: Box<dyn QueryFragment>) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(BroadcastQueryFragment { input }))
    }
}

impl QueryFragment for BroadcastQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        if self.input.get_out_partition()? != PartitionState::NotPartition {
            return Err(ErrorCode::UnImplement("broadcast distributed subquery."));
        }

        Ok(PartitionState::Broadcast)
    }

    fn finalize(&self, _nodes: &mut QueryFragmentsActions) -> Result<()> {
        todo!()
    }

    fn rewrite_remote_plan(&self, _node: &PlanNode, _new: &PlanNode) -> Result<PlanNode> {
        todo!()
    }
}
