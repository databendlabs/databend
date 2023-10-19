// Copyright 2021 Datafuse Labs
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

use bumpalo::Bump;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_functions::aggregates::StateAddr;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_cell::AggregateHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::HashTableCell;
use crate::pipelines::processors::AggregatorParams;

pub struct TransformFinalAggregate<Method: HashMethodBounds> {
    method: Method,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformFinalAggregate<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(BlockMetaTransformer::create(
            input,
            output,
            TransformFinalAggregate::<Method> { method, params },
        )))
    }
}

impl<Method> BlockMetaTransform<AggregateMeta<Method, usize>> for TransformFinalAggregate<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformFinalAggregate";

    fn transform(&mut self, meta: AggregateMeta<Method, usize>) -> Result<DataBlock> {
        if let AggregateMeta::Partitioned { bucket, data } = meta {
            let mut reach_limit = false;
            let arena = Arc::new(Bump::new());
            let hashtable = self.method.create_hash_table::<usize>(arena)?;
            let _dropper = AggregateHashTableDropper::create(self.params.clone());
            let mut hash_cell = HashTableCell::<Method, usize>::create(hashtable, _dropper);

            for bucket_data in data {
                match bucket_data {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(payload) => {
                        debug_assert!(bucket == payload.bucket);

                        let aggregate_function_len = self.params.aggregate_functions.len();

                        let column = payload.get_group_by_column();
                        let keys_iter = self.method.keys_iter_from_column(column)?;

                        // first state places of current block
                        let places = {
                            let keys_iter = keys_iter.iter();
                            let (len, _) = keys_iter.size_hint();
                            let mut places = Vec::with_capacity(len);

                            let mut current_len = hash_cell.hashtable.len();
                            unsafe {
                                for key in keys_iter {
                                    if reach_limit {
                                        let entry = hash_cell.hashtable.entry(key);
                                        if let Some(entry) = entry {
                                            let place = Into::<StateAddr>::into(*entry.get());
                                            places.push(place);
                                        }
                                        continue;
                                    }

                                    match hash_cell.hashtable.insert_and_entry(key) {
                                        Ok(mut entry) => {
                                            let place =
                                                self.params.alloc_layout(&mut hash_cell.arena);
                                            places.push(place);

                                            *entry.get_mut() = place.addr();

                                            if let Some(limit) = self.params.limit {
                                                current_len += 1;
                                                if current_len >= limit {
                                                    reach_limit = true;
                                                }
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
                        };

                        let states_columns = (0..aggregate_function_len)
                            .map(|i| payload.data_block.get_by_offset(i))
                            .collect::<Vec<_>>();
                        let mut states_binary_columns = Vec::with_capacity(states_columns.len());

                        for agg in states_columns.iter().take(aggregate_function_len) {
                            let col = agg.value.as_column().unwrap();
                            states_binary_columns.push(col.slice(0..places.len()));
                        }

                        let aggregate_functions = &self.params.aggregate_functions;
                        let offsets_aggregate_states = &self.params.offsets_aggregate_states;

                        for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                            aggregate_function.batch_merge(
                                &places,
                                offsets_aggregate_states[idx],
                                &states_binary_columns[idx],
                            )?;
                        }
                    }
                    AggregateMeta::HashTable(payload) => unsafe {
                        debug_assert!(bucket == payload.bucket);

                        let aggregate_functions = &self.params.aggregate_functions;
                        let offsets_aggregate_states = &self.params.offsets_aggregate_states;

                        for entry in payload.cell.hashtable.iter() {
                            let place = match hash_cell.hashtable.insert(entry.key()) {
                                Err(place) => StateAddr::new(*place),
                                Ok(entry) => {
                                    let place = self.params.alloc_layout(&mut hash_cell.arena);
                                    entry.write(place.addr());
                                    place
                                }
                            };

                            let old_place = StateAddr::new(*entry.get());
                            for (idx, aggregate_function) in aggregate_functions.iter().enumerate()
                            {
                                let final_place = place.next(offsets_aggregate_states[idx]);
                                let state_place = old_place.next(offsets_aggregate_states[idx]);
                                aggregate_function.merge_states(final_place, state_place)?;
                            }
                        }
                    },
                }
            }

            let keys_len = hash_cell.hashtable.len();
            let value_size = estimated_key_size(&hash_cell.hashtable);

            let mut group_columns_builder =
                self.method
                    .group_columns_builder(keys_len, value_size, &self.params);

            let aggregate_functions = &self.params.aggregate_functions;
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;

            let mut aggregates_column_builder = {
                let mut values = vec![];
                for aggregate_function in aggregate_functions {
                    let data_type = aggregate_function.return_type()?;
                    let builder = ColumnBuilder::with_capacity(&data_type, keys_len);
                    values.push(builder)
                }
                values
            };

            let mut places = Vec::with_capacity(keys_len);
            for group_entity in hash_cell.hashtable.iter() {
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

            return Ok(DataBlock::new_from_columns(columns));
        }

        Err(ErrorCode::Internal(
            "TransformFinalAggregate only recv AggregateMeta::Partitioned",
        ))
    }
}
