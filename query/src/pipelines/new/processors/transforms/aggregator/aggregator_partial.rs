use std::sync::Arc;
use bytes::BytesMut;
use common_datablocks::{DataBlock, HashMethod, HashMethodKeysU16, HashMethodKeysU32, HashMethodKeysU64, HashMethodKeysU8, HashMethodSerializer};
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::{IntoSeries, Series, StringArrayBuilder};
use common_exception::Result;
use common_functions::aggregates::{StateAddr, StateAddrs};
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::transforms::group_by::StateEntity;
use crate::pipelines::transforms::group_by::KeysArrayBuilder;
use crate::pipelines::new::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::transforms::group_by::{AggregatorState, PolymorphicKeysHelper};

pub type KeysU8PartialAggregator<const HAS_AGG: bool> = PartialAggregator<HAS_AGG, HashMethodKeysU8>;
pub type KeysU16PartialAggregator<const HAS_AGG: bool> = PartialAggregator<HAS_AGG, HashMethodKeysU16>;
pub type KeysU32PartialAggregator<const HAS_AGG: bool> = PartialAggregator<HAS_AGG, HashMethodKeysU32>;
pub type KeysU64PartialAggregator<const HAS_AGG: bool> = PartialAggregator<HAS_AGG, HashMethodKeysU64>;
pub type SerializerPartialAggregator<const HAS_AGG: bool> = PartialAggregator<HAS_AGG, HashMethodSerializer>;

pub struct PartialAggregator<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>> {
    is_generated: bool,

    method: Method,
    state: Method::State,
    params: Arc<AggregatorParams>,
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send> PartialAggregator<HAS_AGG, Method> {
    pub fn create(method: Method, params: Arc<AggregatorParams>) -> Self {
        let state = method.aggregate_state();
        Self { is_generated: false, state, method, params }
    }

    #[inline(always)]
    fn lookup_key(keys: Vec<Method::HashKey>, state: &mut Method::State) {
        let mut inserted = true;
        for key in keys.iter() {
            state.entity(key, &mut inserted);
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(params: &Arc<AggregatorParams>, keys: Vec<Method::HashKey>, state: &mut Method::State) -> StateAddrs {
        let mut places = Vec::with_capacity(keys.len());

        let mut inserted = true;
        for key in keys.iter() {
            let entity = state.entity(key, &mut inserted);

            // match inserted {
            //     true => {
            //         let place = state.alloc_layout(params);
            //         places.push(place);
            //         entity.set_state_value(place.addr());
            //     }
            //     false => {
            //         let place: StateAddr = (*entity.get_state_value()).into();
            //         places.push(place);
            //     }
            // }
        }
        places
    }

    #[inline(always)]
    fn aggregate_arguments(block: &DataBlock, params: &Arc<AggregatorParams>) -> Result<Vec<Vec<Series>>> {
        let aggregate_functions_arguments = &params.aggregate_functions_arguments_name;
        let mut aggregate_arguments_columns = Vec::with_capacity(aggregate_functions_arguments.len());
        for function_arguments in aggregate_functions_arguments {
            let mut function_arguments_column = Vec::with_capacity(function_arguments.len());

            for argument_name in function_arguments {
                let argument_column = block.try_column_by_name(argument_name)?;
                function_arguments_column.push(argument_column.to_array()?);
            }

            aggregate_arguments_columns.push(function_arguments_column);
        }

        Ok(aggregate_arguments_columns)
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute(params: &Arc<AggregatorParams>, block: &DataBlock, places: &StateAddrs) -> Result<()> {
        let aggregate_functions = &params.aggregate_functions;
        let offsets_aggregate_states = &params.offsets_aggregate_states;
        let aggregate_arguments_columns = Self::aggregate_arguments(block, params)?;

        // This can benificial for the case of dereferencing
        // This will help improve the performance ~hundreds of megabits per second
        let aggr_arg_columns_slice = &aggregate_arguments_columns;

        for index in 0..aggregate_functions.len() {
            let rows = block.num_rows();
            let function = &aggregate_functions[index];
            let state_offset = offsets_aggregate_states[index];
            let function_arguments = &aggr_arg_columns_slice[index];
            function.accumulate_keys(places, state_offset, function_arguments, rows)?;
        }

        Ok(())
    }

    #[inline(always)]
    pub fn group_columns<'a>(names: &[String], block: &'a DataBlock) -> Result<Vec<&'a DataColumn>> {
        names
            .iter()
            .map(|column_name| block.try_column_by_name(column_name))
            .collect::<Result<Vec<&DataColumn>>>()
    }

    #[inline(always)]
    fn generate_data(&mut self) -> Result<Option<DataBlock>> {
        if self.state.len() == 0 || self.is_generated {
            return Ok(None);
        }

        self.is_generated = true;
        let state_groups_len = self.state.len();
        let aggregator_params = self.params.as_ref();
        let funcs = &aggregator_params.aggregate_functions;
        let aggr_len = funcs.len();
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        // Builders.
        let mut state_builders: Vec<StringArrayBuilder> = (0..aggr_len)
            .map(|_| StringArrayBuilder::with_capacity(state_groups_len * 4))
            .collect();

        let mut group_key_builder = self.method.state_array_builder(state_groups_len);

        let mut bytes = BytesMut::new();
        for group_entity in self.state.iter() {
            let place: StateAddr = (*group_entity.get_state_value()).into();

            for (idx, func) in funcs.iter().enumerate() {
                let arg_place = place.next(offsets_aggregate_states[idx]);
                func.serialize(arg_place, &mut bytes)?;
                state_builders[idx].append_value(&bytes[..]);
                bytes.clear();
            }

            group_key_builder.append_value(group_entity.get_state_key());
        }

        let schema = &self.params.schema;
        let mut columns: Vec<Series> = Vec::with_capacity(schema.fields().len());
        for mut builder in state_builders {
            columns.push(builder.finish().into_series());
        }

        columns.push(group_key_builder.finish());
        Ok(Some(DataBlock::create_by_array(schema.clone(), columns)))
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator for PartialAggregator<true, Method> {
    const NAME: &'static str = "";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&self.params.group_columns_name, &block)?;
        let group_keys = self.method.build_keys(&group_columns, block.num_rows())?;

        let places = Self::lookup_state(&self.params, group_keys, &mut self.state);
        Self::execute(&self.params, &block, &places)
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        self.generate_data()
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator for PartialAggregator<false, Method> {
    const NAME: &'static str = "";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&self.params.group_columns_name, &block)?;
        let group_keys = self.method.build_keys(&group_columns, block.num_rows())?;
        Self::lookup_key(group_keys, &mut self.state);
        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        self.generate_data()
    }
}
