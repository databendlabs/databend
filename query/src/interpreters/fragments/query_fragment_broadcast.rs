use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::{ErrorCode, Result};
use crate::api::FlightAction;
use crate::interpreters::fragments::partition_state::PartitionState;

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

    fn finalize(&self, nodes: Vec<String>) -> Result<Vec<FlightAction>> {
        todo!()
    }
}
