use std::sync::Arc;
use common_datablocks::{DataBlock, HashMethod};
use common_datavalues2::{ColumnRef, MutableColumn, Series};
use common_exception::Result;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::transforms::group_by::StateEntity;
use crate::pipelines::new::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::transforms::group_by::{AggregatorState, GroupColumnsBuilder, KeysColumnIter, PolymorphicKeysHelper};

pub struct FinalAggregator<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send> {
    is_generated: bool,

    method: Method,
    state: Method::State,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator for FinalAggregator<true, Method> {
    const NAME: &'static str = "";

    fn consume(&mut self, data: DataBlock) -> Result<()> {
        unimplemented!()
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        unimplemented!()
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator for FinalAggregator<false, Method> {
    const NAME: &'static str = "";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let key_array = block.column(0);
        let keys_iter = self.method.keys_iter_from_column(&key_array)?;

        let mut inserted = true;
        for keys_ref in keys_iter.get_slice() {
            self.state.entity_by_key(keys_ref, &mut inserted);
        }

        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.state.len() == 0 || self.is_generated {
            true => Ok(None),
            false => {
                self.is_generated = true;
                let mut columns_builder = self.method.group_columns_builder(self.state.len(), &self.params);
                for group_entity in self.state.iter() {
                    columns_builder.append_value(group_entity.get_state_key());
                }

                let columns = columns_builder.finish()?;
                Ok(Some(DataBlock::create(self.params.before_schema.clone(), columns)))
            }
        }
    }
}
