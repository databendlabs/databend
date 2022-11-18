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

use std::sync::Arc;

use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::HashMethod;
use common_expression::HashMethodKeysU128;
use common_expression::HashMethodKeysU16;
use common_expression::HashMethodKeysU256;
use common_expression::HashMethodKeysU32;
use common_expression::HashMethodKeysU512;
use common_expression::HashMethodKeysU64;
use common_expression::HashMethodKeysU8;
use common_expression::HashMethodSerializer;
use common_expression::Value;
use common_functions::aggregates::StateAddr;
use common_functions::aggregates::StateAddrs;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;

pub type Keys8Grouper = PartialAggregator<false, HashMethodKeysU8>;
pub type Keys16Grouper = PartialAggregator<false, HashMethodKeysU16>;
pub type Keys32Grouper = PartialAggregator<false, HashMethodKeysU32>;
pub type Keys64Grouper = PartialAggregator<false, HashMethodKeysU64>;
pub type Keys128Grouper = PartialAggregator<false, HashMethodKeysU128>;
pub type Keys256Grouper = PartialAggregator<false, HashMethodKeysU256>;
pub type Keys512Grouper = PartialAggregator<false, HashMethodKeysU512>;
pub type KeysSerializerGrouper = PartialAggregator<false, HashMethodSerializer>;

pub type Keys8Aggregator = PartialAggregator<true, HashMethodKeysU8>;
pub type Keys16Aggregator = PartialAggregator<true, HashMethodKeysU16>;
pub type Keys32Aggregator = PartialAggregator<true, HashMethodKeysU32>;
pub type Keys64Aggregator = PartialAggregator<true, HashMethodKeysU64>;
pub type Keys128Aggregator = PartialAggregator<true, HashMethodKeysU128>;
pub type Keys256Aggregator = PartialAggregator<true, HashMethodKeysU256>;
pub type Keys512Aggregator = PartialAggregator<true, HashMethodKeysU512>;
pub type KeysSerializerAggregator = PartialAggregator<true, HashMethodSerializer>;

pub struct PartialAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method>
{
    pub states_dropped: bool,

    pub area: Option<Area>,
    pub method: Method,
    pub hash_table: Method::HashTable,
    pub params: Arc<AggregatorParams>,
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send>
    PartialAggregator<HAS_AGG, Method>
{
    pub fn create(method: Method, params: Arc<AggregatorParams>) -> Result<Self> {
        let hash_table = method.create_hash_table()?;
        Ok(Self {
            params,
            method,
            hash_table,
            area: Some(Area::create()),
            states_dropped: false,
        })
    }

    #[inline(always)]
    fn lookup_key(keys_iter: Method::HashKeyIter<'_>, hashtable: &mut Method::HashTable) {
        unsafe {
            for key in keys_iter {
                let _ = hashtable.insert_and_entry(key);
            }
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(
        area: &mut Area,
        params: &Arc<AggregatorParams>,
        keys_iter: Method::HashKeyIter<'_>,
        hashtable: &mut Method::HashTable,
    ) -> StateAddrs {
        let mut places = Vec::with_capacity(keys_iter.size_hint().0);

        unsafe {
            for key in keys_iter {
                match hashtable.insert_and_entry(key) {
                    Ok(mut entry) => {
                        if let Some(place) = params.alloc_layout(area) {
                            places.push(place);
                            *entry.get_mut() = place.addr();
                        }
                    }
                    Err(entry) => {
                        let place = Into::<StateAddr>::into(*entry.get());
                        places.push(place);
                    }
                }
            }
        }

        places
    }

    // Chunk should be `convert_to_full`.
    #[inline(always)]
    fn aggregate_arguments(
        chunk: &Chunk,
        params: &Arc<AggregatorParams>,
    ) -> Result<Vec<Vec<Column>>> {
        let aggregate_functions_arguments = &params.aggregate_functions_arguments;
        let mut aggregate_arguments_columns =
            Vec::with_capacity(aggregate_functions_arguments.len());
        for function_arguments in aggregate_functions_arguments {
            let mut function_arguments_column = Vec::with_capacity(function_arguments.len());

            for argument_index in function_arguments {
                // Unwrap safety: chunk has been `convert_to_full`.
                let argument_column = chunk.column(*argument_index).0.as_column().unwrap();
                function_arguments_column.push(argument_column.clone());
            }

            aggregate_arguments_columns.push(function_arguments_column);
        }

        Ok(aggregate_arguments_columns)
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute(params: &Arc<AggregatorParams>, chunk: &Chunk, places: &StateAddrs) -> Result<()> {
        let aggregate_functions = &params.aggregate_functions;
        let offsets_aggregate_states = &params.offsets_aggregate_states;
        let aggregate_arguments_columns = Self::aggregate_arguments(&chunk, params)?;

        // This can benificial for the case of dereferencing
        // This will help improve the performance ~hundreds of megabits per second
        let aggr_arg_columns_slice = &aggregate_arguments_columns;

        for index in 0..aggregate_functions.len() {
            let rows = chunk.num_rows();
            let function = &aggregate_functions[index];
            let state_offset = offsets_aggregate_states[index];
            let function_arguments = &aggr_arg_columns_slice[index];
            function.accumulate_keys(places, state_offset, function_arguments, rows)?;
        }

        Ok(())
    }

    #[inline(always)]
    pub fn group_columns<'a>(
        chunk: &'a Chunk,
        indices: &[usize],
    ) -> Vec<&'a (Value<AnyType>, DataType)> {
        indices
            .iter()
            .map(|&index| chunk.column(index))
            .collect::<Vec<_>>()
    }

    #[inline(always)]
    fn generate_data(&mut self) -> Result<Vec<Chunk>> {
        if self.hash_table.len() == 0 {
            return Ok(vec![]);
        }

        let state_groups_len = self.hash_table.len();
        let aggregator_params = self.params.as_ref();
        let funcs = &aggregator_params.aggregate_functions;
        let aggr_len = funcs.len();
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        // Builders.
        let mut state_builders = (0..aggr_len)
            .map(|_| StringColumnBuilder::with_capacity(state_groups_len, state_groups_len * 4))
            .collect::<Vec<_>>();

        let mut group_key_builder = self.method.keys_column_builder(state_groups_len);
        for group_entity in self.hash_table.iter() {
            let place = Into::<StateAddr>::into(*group_entity.get());

            for (idx, func) in funcs.iter().enumerate() {
                let arg_place = place.next(offsets_aggregate_states[idx]);
                func.serialize(arg_place, &mut state_builders[idx].data)?;
                state_builders[idx].commit_row();
            }

            group_key_builder.append_value(group_entity.key());
        }

        let schema = &self.params.output_schema;
        let mut columns: Vec<(Value<AnyType>, DataType)> =
            Vec::with_capacity(schema.fields().len());
        for (builder, f) in state_builders.into_iter().zip(schema.fields().iter()) {
            columns.push((Value::Column(builder.build()), f.data_type().into()));
        }

        let group_key_col = group_key_builder.finish();
        let num_rows = group_key_col.len();
        columns.push((
            Value::Column(group_key_col),
            schema.fields().last().unwrap(),
        ));
        Ok(vec![Chunk::new(columns, num_rows)])
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for PartialAggregator<true, Method>
{
    const NAME: &'static str = "GroupByPartialTransform";

    fn consume(&mut self, chunk: Chunk) -> Result<()> {
        let chunk = chunk.convert_to_full();
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&self.params.group_columns, &chunk);
        let group_columns = group_columns
            .iter()
            .map(|(c, ty)| (c.as_column().unwrap(), ty.clone()))
            .collect::<Vec<_>>();
        let group_keys_state = self
            .method
            .build_keys_state(&group_columns, chunk.num_rows())?;

        let group_keys_iter = self.method.build_keys_iter(&group_keys_state)?;

        let area = self.area.as_mut().unwrap();
        let places = Self::lookup_state(area, &self.params, group_keys_iter, &mut self.hash_table);
        Self::execute(&self.params, &chunk, &places)
    }

    fn generate(&mut self) -> Result<Vec<Chunk>> {
        self.generate_data()
    }
}

impl<Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for PartialAggregator<false, Method>
{
    const NAME: &'static str = "GroupByPartialTransform";

    fn consume(&mut self, chunk: Chunk) -> Result<()> {
        let chunk = chunk.convert_to_full();
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&self.params.group_columns, &chunk);
        let group_columns = group_columns
            .iter()
            .map(|(c, ty)| (c.as_column().unwrap(), ty.clone()))
            .collect::<Vec<_>>();

        let keys_state = self
            .method
            .build_keys_state(&group_columns, chunk.num_rows())?;
        let group_keys_iter = self.method.build_keys_iter(&keys_state)?;

        Self::lookup_key(group_keys_iter, &mut self.hash_table);
        Ok(())
    }

    fn generate(&mut self) -> Result<Vec<Chunk>> {
        if self.hash_table.len() == 0 {
            self.drop_states();
            return Ok(vec![]);
        }

        let capacity = self.hash_table.len();
        let mut keys_column_builder = self.method.keys_column_builder(capacity);
        for group_entity in self.hash_table.iter() {
            keys_column_builder.append_value(group_entity.key());
        }

        let columns = keys_column_builder.finish();

        self.drop_states();
        let data_type = self.params.output_schema.field(0).data_type().into();
        Ok(vec![Chunk::new(
            vec![(Value::Column(columns), data_type)],
            columns.len(),
        )])
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>>
    PartialAggregator<HAS_AGG, Method>
{
    pub fn drop_states(&mut self) {
        if !self.states_dropped {
            let aggregator_params = self.params.as_ref();
            let aggregate_functions = &aggregator_params.aggregate_functions;
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            let functions = aggregate_functions
                .iter()
                .filter(|p| p.need_manual_drop_state())
                .collect::<Vec<_>>();

            let states = offsets_aggregate_states
                .iter()
                .enumerate()
                .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
                .map(|(_, s)| *s)
                .collect::<Vec<_>>();

            for group_entity in self.hash_table.iter() {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (function, state_offset) in functions.iter().zip(states.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }

            self.states_dropped = true;
        }
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>> Drop
    for PartialAggregator<HAS_AGG, Method>
{
    fn drop(&mut self) {
        self.drop_states();
    }
}
