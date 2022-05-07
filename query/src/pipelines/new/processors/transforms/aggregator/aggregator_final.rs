// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKeysU16;
use common_datablocks::HashMethodKeysU32;
use common_datablocks::HashMethodKeysU64;
use common_datablocks::HashMethodKeysU8;
use common_datablocks::HashMethodSerializer;
use common_datablocks::HashMethodSingleString;
use common_datavalues::DataType;
use common_datavalues::MutableColumn;
use common_datavalues::ScalarColumn;
use common_datavalues::Series;
use common_datavalues::StringColumn;
use common_exception::Result;
use common_functions::aggregates::StateAddr;
use common_functions::aggregates::StateAddrs;

use crate::pipelines::new::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::transforms::group_by::AggregatorState;
use crate::pipelines::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::transforms::group_by::KeysColumnIter;
use crate::pipelines::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::transforms::group_by::StateEntity;
use crate::sessions::QueryContext;

pub type KeysU8FinalAggregator<const HAS_AGG: bool> = FinalAggregator<HAS_AGG, HashMethodKeysU8>;
pub type KeysU16FinalAggregator<const HAS_AGG: bool> = FinalAggregator<HAS_AGG, HashMethodKeysU16>;
pub type KeysU32FinalAggregator<const HAS_AGG: bool> = FinalAggregator<HAS_AGG, HashMethodKeysU32>;
pub type KeysU64FinalAggregator<const HAS_AGG: bool> = FinalAggregator<HAS_AGG, HashMethodKeysU64>;
pub type SingleStringFinalAggregator<const HAS_AGG: bool> =
    FinalAggregator<HAS_AGG, HashMethodSingleString>;

pub type SerializerFinalAggregator<const HAS_AGG: bool> =
    FinalAggregator<HAS_AGG, HashMethodSerializer>;

pub struct FinalAggregator<
    const HAS_AGG: bool,
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
> {
    is_generated: bool,
    states_dropped: bool,

    method: Method,
    state: Method::State,
    params: Arc<AggregatorParams>,
    // used for deserialization only, so we can reuse it during the loop
    temp_place: StateAddr,
    ctx: Arc<QueryContext>,
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send>
    FinalAggregator<HAS_AGG, Method>
{
    pub fn create(method: Method, params: Arc<AggregatorParams>, ctx: Arc<QueryContext>) -> Self {
        let state = method.aggregate_state();
        let temp_place = if params.aggregate_functions.is_empty() {
            0.into()
        } else {
            state.alloc_layout2(&params)
        };

        Self {
            is_generated: false,
            states_dropped: false,
            state,
            method,
            params,
            temp_place,
            ctx,
        }
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> FinalAggregator<true, Method> {
    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(
        params: &AggregatorParams,
        state: &mut Method::State,
        keys: &[<Method::State as AggregatorState<Method>>::Key],
    ) -> StateAddrs {
        let mut places = Vec::with_capacity(keys.len());

        let mut inserted = true;
        for key in keys {
            let entity = state.entity_by_key(key, &mut inserted);

            match inserted {
                true => {
                    let place = state.alloc_layout2(params);
                    places.push(place);
                    entity.set_state_value(place.addr());
                }
                false => {
                    let place: StateAddr = (*entity.get_state_value()).into();
                    places.push(place);
                }
            }
        }
        places
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for FinalAggregator<true, Method>
{
    const NAME: &'static str = "";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        // 1.1 and 1.2.
        let aggregate_function_len = self.params.aggregate_functions.len();
        let keys_column = block.column(aggregate_function_len);
        let keys_iter = self.method.keys_iter_from_column(keys_column)?;

        let group_by_two_level_threshold =
            self.ctx.get_settings().get_group_by_two_level_threshold()? as usize;
        if !self.state.is_two_level() && self.state.len() >= group_by_two_level_threshold {
            self.state.convert_to_two_level();
        }

        // first state places of current block
        let places = Self::lookup_state(&self.params, &mut self.state, keys_iter.get_slice());

        let states_columns = (0..aggregate_function_len)
            .map(|i| block.column(i))
            .collect::<Vec<_>>();
        let mut states_binary_columns = Vec::with_capacity(states_columns.len());

        for agg in states_columns.iter().take(aggregate_function_len) {
            let aggr_column: &StringColumn = Series::check_get(agg)?;
            states_binary_columns.push(aggr_column);
        }

        let aggregate_functions = &self.params.aggregate_functions;
        let offsets_aggregate_states = &self.params.offsets_aggregate_states;

        for (row, place) in places.iter().enumerate() {
            for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                let final_place = place.next(offsets_aggregate_states[idx]);
                let state_place = self.temp_place.next(offsets_aggregate_states[idx]);

                let mut data = states_binary_columns[idx].get_data(row);

                aggregate_function.deserialize(state_place, &mut data)?;
                aggregate_function.merge(final_place, state_place)?;
            }
        }
        Ok(())
    }

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.state.len() == 0 || self.is_generated {
            true => {
                self.drop_states();
                Ok(None)
            }
            false => {
                self.is_generated = true;
                let mut group_columns_builder = self
                    .method
                    .group_columns_builder(self.state.len(), &self.params);

                let aggregate_functions = &self.params.aggregate_functions;
                let offsets_aggregate_states = &self.params.offsets_aggregate_states;

                let mut aggregates_column_builder: Vec<Box<dyn MutableColumn>> = {
                    let mut values = vec![];
                    for aggregate_function in aggregate_functions {
                        let builder = aggregate_function.return_type()?.create_mutable(1024);
                        values.push(builder)
                    }
                    values
                };

                for group_entity in self.state.iter() {
                    let place: StateAddr = (*group_entity.get_state_value()).into();

                    for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                        let arg_place = place.next(offsets_aggregate_states[idx]);
                        let builder: &mut dyn MutableColumn =
                            aggregates_column_builder[idx].borrow_mut();
                        aggregate_function.merge_result(arg_place, builder)?;
                    }

                    group_columns_builder.append_value(group_entity.get_state_key());
                }

                // Build final state block.
                let fields_len = self.params.schema.fields().len();
                let mut columns = Vec::with_capacity(fields_len);

                for mut array in aggregates_column_builder {
                    columns.push(array.to_column());
                }

                columns.extend_from_slice(&group_columns_builder.finish()?);

                Ok(Some(DataBlock::create(self.params.schema.clone(), columns)))
            }
        }
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for FinalAggregator<false, Method>
{
    const NAME: &'static str = "";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        let key_array = block.column(0);
        let keys_iter = self.method.keys_iter_from_column(key_array)?;

        let group_by_two_level_threshold =
            self.ctx.get_settings().get_group_by_two_level_threshold()? as usize;
        if !self.state.is_two_level() && self.state.len() >= group_by_two_level_threshold {
            self.state.convert_to_two_level();
        }

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
                let mut columns_builder = self
                    .method
                    .group_columns_builder(self.state.len(), &self.params);
                for group_entity in self.state.iter() {
                    columns_builder.append_value(group_entity.get_state_key());
                }

                let columns = columns_builder.finish()?;
                Ok(Some(DataBlock::create(self.params.schema.clone(), columns)))
            }
        }
    }
}

impl<const FINAL: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send>
    FinalAggregator<FINAL, Method>
{
    fn drop_states(&mut self) {
        if !self.states_dropped {
            let aggregator_params = self.params.as_ref();
            let aggregate_functions = &aggregator_params.aggregate_functions;
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            let functions = aggregate_functions
                .iter()
                .filter(|p| p.need_manual_drop_state())
                .collect::<Vec<_>>();

            let state_offsets = offsets_aggregate_states
                .iter()
                .enumerate()
                .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
                .map(|(_, s)| *s)
                .collect::<Vec<_>>();

            for group_entity in self.state.iter() {
                let place: StateAddr = (*group_entity.get_state_value()).into();

                for (function, state_offset) in functions.iter().zip(state_offsets.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }

            for (state_offset, function) in state_offsets.iter().zip(functions.iter()) {
                let place = self.temp_place.next(*state_offset);
                unsafe { function.drop_state(place) }
            }
            self.states_dropped = true;
        }
    }
}

impl<const FINAL: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Drop
    for FinalAggregator<FINAL, Method>
{
    fn drop(&mut self) {
        self.drop_states();
    }
}
