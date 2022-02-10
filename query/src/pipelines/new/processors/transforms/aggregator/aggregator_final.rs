use std::sync::Arc;
use common_datablocks::{DataBlock, HashMethod};
use common_datavalues2::Series;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::transforms::group_by::{PolymorphicKeysHelper};
use common_exception::Result;

pub struct FinalAggregator<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send> {
    is_generated: bool,

    method: Method,
    state: Method::State,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator for FinalAggregator<true, Method> {
    const NAME: &'static str = "";

    fn consume(&mut self, data: DataBlock) -> Result<()> {
        todo!()
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        todo!()
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator for FinalAggregator<false, Method> {
    const NAME: &'static str = "";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let keys_column = block.column(self.params.aggregate_functions.len());

        // let key_array: $key_column_type = Series::check_get(keys_column)?;

        // let key_array = block.column(self.params.aggregate_functions.len()).to_array()?;
        // let key_array = block.column(aggr_funcs_len).to_array()?;

        // let key_array: $key_array_type = key_array.$downcast_fn()?;
        //
        // let states_series = (0..aggr_funcs_len)
        //     .map(|i| block.column(i).to_array())
        //     .collect::<Result<Vec<_>>>()?;
        // let mut states_binary_arrays = Vec::with_capacity(states_series.len());
        todo!()
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        todo!()
    }
}
