use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::Result;
use crate::api::FlightAction;
use crate::interpreters::fragments::partition_state::PartitionState;

#[derive(Debug)]
pub struct StageQueryFragment {
    input: Box<dyn QueryFragment>,
}

impl StageQueryFragment {
    pub fn create(input: Box<dyn QueryFragment>) -> Result<Box<dyn QueryFragment>> {
        // let input_partition = input.get_out_partition()?;
        Ok(Box::new(StageQueryFragment { input }))
    }
}

impl QueryFragment for StageQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        todo!()
    }

    fn finalize(&self, nodes: Vec<String>) -> Result<Vec<FlightAction>> {
        todo!()
    }
}
