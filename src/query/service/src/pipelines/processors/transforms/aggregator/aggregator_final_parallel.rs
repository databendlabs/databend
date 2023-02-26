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
use std::vec;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncastHelper;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_functions::aggregates::StateAddr;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use super::estimated_key_size;
use super::AggregateHashStateInfo;
use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::AggregatorParams;

pub struct BucketAggregator<const HAS_AGG: bool, Method: HashMethodBounds> {
    area: Area,
    method: Method,
    params: Arc<AggregatorParams>,
    hash_table: Method::HashTable<usize>,
    state_holders: Vec<Option<ArenaHolder>>,

    pub(crate) reach_limit: bool,
    // used for deserialization only if has agg, so we can reuse it during the loop
    temp_place: StateAddr,
}

impl<const HAS_AGG: bool, Method: HashMethodBounds> BucketAggregator<HAS_AGG, Method> {
    pub fn create(method: Method, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut area = Area::create();
        let hash_table = method.create_hash_table()?;
        let temp_place = match params.aggregate_functions.is_empty() {
            true => StateAddr::new(0),
            false => params.alloc_layout(&mut area),
        };

        Ok(Self {
            area,
            method,
            params,
            hash_table,
            reach_limit: false,
            state_holders: Vec::with_capacity(16),
            temp_place,
        })
    }

    fn merge_partial_hashstates(&mut self, hashtable: &mut Method::HashTable<usize>) -> Result<()> {
        // Note: We can't swap the ptr here, there maybe some bugs if the original hashtable
        // if self.hash_table.len() == 0 {
        //     std::mem::swap(&mut self.hash_table, hashtable);
        //     return Ok(());
        // }

        if !HAS_AGG {
            unsafe {
                for key in hashtable.iter() {
                    let _ = self.hash_table.insert_and_entry(key.key());
                }
                if let Some(limit) = self.params.limit {
                    if self.hash_table.len() >= limit {
                        return Ok(());
                    }
                }
            }
        } else {
            let aggregate_functions = &self.params.aggregate_functions;
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;

            for entry in hashtable.iter() {
                let key = entry.key();
                unsafe {
                    match self.hash_table.insert(key) {
                        Ok(e) => {
                            // just set new places and the arena will be keeped in partial state
                            e.write(*entry.get());
                        }
                        Err(place) => {
                            // place already exists
                            // that means we should merge the aggregation
                            let place = StateAddr::new(*place);
                            let old_place = StateAddr::new(*entry.get());

                            for (idx, aggregate_function) in aggregate_functions.iter().enumerate()
                            {
                                let final_place = place.next(offsets_aggregate_states[idx]);
                                let state_place = old_place.next(offsets_aggregate_states[idx]);
                                aggregate_function.merge(final_place, state_place)?;
                                aggregate_function.drop_state(state_place);
                            }
                        }
                    }
                }
            }
        }
        hashtable.clear();
        Ok(())
    }

    pub fn merge_blocks(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        for mut data_block in blocks {
            if let Some(mut meta) = data_block.take_meta() {
                if let Some(mut info) = AggregateHashStateInfo::downcast_from(meta) {
                    let hashtable = info
                        .hash_state
                        .downcast_mut::<Method::HashTable<usize>>()
                        .unwrap();
                    self.state_holders.push(info.state_holder);
                    self.merge_partial_hashstates(hashtable)?;
                    continue;
                }
            }

            let block = data_block.convert_to_full();
            // 1.1 and 1.2.
            let aggregate_function_len = self.params.aggregate_functions.len();
            let keys_column = block
                .get_by_offset(aggregate_function_len)
                .value
                .as_column()
                .unwrap();
            let keys_iter = self.method.keys_iter_from_column(keys_column)?;

            if !HAS_AGG {
                unsafe {
                    for key in keys_iter.iter() {
                        let _ = self.hash_table.insert_and_entry(key);
                    }

                    if let Some(limit) = self.params.limit {
                        if self.hash_table.len() >= limit {
                            break;
                        }
                    }
                }
            } else {
                // first state places of current block
                let places = self.lookup_state(&keys_iter);

                let states_columns = (0..aggregate_function_len)
                    .map(|i| block.get_by_offset(i))
                    .collect::<Vec<_>>();
                let mut states_binary_columns = Vec::with_capacity(states_columns.len());

                for agg in states_columns.iter().take(aggregate_function_len) {
                    let aggr_column =
                        agg.value.as_column().unwrap().as_string().ok_or_else(|| {
                            ErrorCode::IllegalDataType(format!(
                                "Aggregation column should be StringType, but got {:?}",
                                agg.value
                            ))
                        })?;
                    states_binary_columns.push(aggr_column);
                }

                let aggregate_functions = &self.params.aggregate_functions;
                let offsets_aggregate_states = &self.params.offsets_aggregate_states;

                for (row, place) in places.iter() {
                    for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                        let final_place = place.next(offsets_aggregate_states[idx]);
                        let state_place = self.temp_place.next(offsets_aggregate_states[idx]);

                        let mut data = unsafe { states_binary_columns[idx].index_unchecked(*row) };
                        aggregate_function.deserialize(state_place, &mut data)?;
                        aggregate_function.merge(final_place, state_place)?;
                    }
                }
            }
        }

        let value_size = estimated_key_size(&self.hash_table);
        let keys_len = self.hash_table.len();

        let mut group_columns_builder =
            self.method
                .group_columns_builder(keys_len, value_size, &self.params);

        if !HAS_AGG {
            for group_entity in self.hash_table.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            let columns = group_columns_builder.finish()?;

            Ok(vec![DataBlock::new_from_columns(columns)])
        } else {
            let aggregate_functions = &self.params.aggregate_functions;
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;

            let mut aggregates_column_builder = {
                let mut values = vec![];
                for aggregate_function in aggregate_functions {
                    let data_type = aggregate_function.return_type()?;
                    let builder = ColumnBuilder::with_capacity(&data_type, self.hash_table.len());
                    values.push(builder)
                }
                values
            };

            let mut places = Vec::with_capacity(keys_len);
            for group_entity in self.hash_table.iter() {
                places.push(StateAddr::new(*group_entity.get()));
                group_columns_builder.append_value(group_entity.key());
            }

            for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                let builder = aggregates_column_builder[idx].borrow_mut();

                if idx > 0 {
                    for place in places.iter_mut() {
                        *place = place.next(
                            offsets_aggregate_states[idx] - offsets_aggregate_states[idx - 1],
                        );
                    }
                }
                aggregate_function.batch_merge_result(&places, builder)?;
            }

            // Build final state block.
            let mut columns = aggregates_column_builder
                .into_iter()
                .map(|builder| builder.build())
                .collect::<Vec<_>>();

            let group_columns = group_columns_builder.finish()?;
            columns.extend_from_slice(&group_columns);

            Ok(vec![DataBlock::new_from_columns(columns)])
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(&mut self, keys_iter: &Method::KeysColumnIter) -> Vec<(usize, StateAddr)> {
        let iter = keys_iter.iter();
        let (len, _) = iter.size_hint();
        let mut places = Vec::with_capacity(len);

        let mut current_len = self.hash_table.len();
        unsafe {
            for (row, key) in iter.enumerate() {
                if self.reach_limit {
                    let entry = self.hash_table.entry(key);
                    if let Some(entry) = entry {
                        let place = Into::<StateAddr>::into(*entry.get());
                        places.push((row, place));
                    }
                    continue;
                }

                match self.hash_table.insert_and_entry(key) {
                    Ok(mut entry) => {
                        let place = self.params.alloc_layout(&mut self.area);
                        places.push((row, place));

                        *entry.get_mut() = place.addr();

                        if let Some(limit) = self.params.limit {
                            current_len += 1;
                            if current_len >= limit {
                                self.reach_limit = true;
                            }
                        }
                    }
                    Err(entry) => {
                        let place = Into::<StateAddr>::into(*entry.get());
                        places.push((row, place));
                    }
                }
            }
        }

        places
    }

    fn drop_states(&mut self) {
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

        if !state_offsets.is_empty() {
            for group_entity in self.hash_table.iter() {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (function, state_offset) in functions.iter().zip(state_offsets.iter()) {
                    unsafe { function.drop_state(place.next(*state_offset)) }
                }
            }
        }

        if HAS_AGG {
            for (state_offset, function) in state_offsets.iter().zip(functions.iter()) {
                let place = self.temp_place.next(*state_offset);
                unsafe { function.drop_state(place) }
            }
        }
        self.state_holders.clear();
    }
}

impl<const HAS_AGG: bool, Method: HashMethodBounds> Drop for BucketAggregator<HAS_AGG, Method> {
    fn drop(&mut self) {
        self.drop_states();
    }
}
