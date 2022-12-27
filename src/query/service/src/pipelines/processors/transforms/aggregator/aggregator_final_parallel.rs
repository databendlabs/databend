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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use common_base::runtime::ThreadPool;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::ColumnBuilder;
use common_expression::HashMethod;
use common_expression::Value;
use common_functions_v2::aggregates::StateAddr;
use common_functions_v2::aggregates::StateAddrs;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use tracing::info;

use crate::data_type_of_group_key_column;
use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;
use crate::sessions::QueryContext;

pub struct ParallelFinalAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    method: Method,
    query_ctx: Arc<QueryContext>,
    params: Arc<AggregatorParams>,
    buckets_chunks: HashMap<isize, Vec<Chunk>>,
}

impl<Method, const HAS_AGG: bool> ParallelFinalAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    pub fn create(
        ctx: Arc<QueryContext>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<Self> {
        Ok(Self {
            params,
            method,
            query_ctx: ctx,
            buckets_chunks: HashMap::new(),
        })
    }
}

impl<Method, const HAS_AGG: bool> Aggregator for ParallelFinalAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    const NAME: &'static str = "GroupByFinalTransform";

    fn consume(&mut self, chunk: Chunk) -> Result<()> {
        let mut bucket = -1;
        if let Some(meta_info) = chunk.get_meta() {
            if let Some(meta_info) = meta_info.as_any().downcast_ref::<AggregateInfo>() {
                bucket = meta_info.bucket;
            }
        }

        match self.buckets_chunks.entry(bucket) {
            Entry::Vacant(v) => {
                v.insert(vec![chunk]);
                Ok(())
            }
            Entry::Occupied(mut v) => {
                v.get_mut().push(chunk);
                Ok(())
            }
        }
    }

    fn generate(&mut self) -> Result<Vec<Chunk>> {
        let mut generate_chunks = Vec::new();
        let settings = self.query_ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;

        if max_threads <= 1
            || self.buckets_chunks.len() == 1
            || self.buckets_chunks.contains_key(&-1)
        {
            let mut chunks = vec![];
            for (_, bucket_chunks) in std::mem::take(&mut self.buckets_chunks) {
                chunks.extend(bucket_chunks);
            }

            let method = self.method.clone();
            let params = self.params.clone();
            let mut bucket_aggregator = BucketAggregator::<HAS_AGG, _>::create(method, params)?;
            generate_chunks = bucket_aggregator.merge_chunks(chunks)?;
        } else if self.buckets_chunks.len() > 1 {
            info!("Merge to final state using a parallel algorithm.");

            let thread_pool = ThreadPool::create(max_threads)?;
            let mut join_handles = Vec::with_capacity(self.buckets_chunks.len());

            for (_, bucket_chunks) in std::mem::take(&mut self.buckets_chunks) {
                let method = self.method.clone();
                let params = self.params.clone();
                let mut bucket_aggregator = BucketAggregator::<HAS_AGG, _>::create(method, params)?;
                join_handles.push(
                    thread_pool.execute(move || bucket_aggregator.merge_chunks(bucket_chunks)),
                );
            }

            generate_chunks.reserve(join_handles.len());
            for join_handle in join_handles {
                generate_chunks.extend(join_handle.join()?);
            }
        }

        Ok(generate_chunks)
    }
}

struct BucketAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    area: Area,
    method: Method,
    params: Arc<AggregatorParams>,
    hash_table: Method::HashTable,

    // used for deserialization only, so we can reuse it during the loop
    temp_place: Option<StateAddr>,
}

impl<const HAS_AGG: bool, Method> BucketAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    pub fn create(method: Method, params: Arc<AggregatorParams>) -> Result<Self> {
        let mut area = Area::create();
        let hash_table = method.create_hash_table()?;
        let temp_place = match params.aggregate_functions.is_empty() {
            true => None,
            false => params.alloc_layout(&mut area),
        };

        Ok(Self {
            area,
            method,
            params,
            hash_table,
            temp_place,
        })
    }

    pub fn merge_chunks(&mut self, chunks: Vec<Chunk>) -> Result<Vec<Chunk>> {
        if chunks.is_empty() {
            return Ok(vec![]);
        }
        for chunk in chunks {
            let chunk = chunk.convert_to_full();
            // 1.1 and 1.2.
            let aggregate_function_len = self.params.aggregate_functions.len();
            let keys_column = chunk
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
                }
            } else {
                // first state places of current chunk
                let places = self.lookup_state(&keys_iter);

                let states_columns = (0..aggregate_function_len)
                    .map(|i| chunk.get_by_offset(i))
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
                if let Some(temp_place) = self.temp_place {
                    for (row, place) in places.iter().enumerate() {
                        for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                            let final_place = place.next(offsets_aggregate_states[idx]);
                            let state_place = temp_place.next(offsets_aggregate_states[idx]);

                            let mut data =
                                unsafe { states_binary_columns[idx].index_unchecked(row) };
                            aggregate_function.deserialize(state_place, &mut data)?;
                            aggregate_function.merge(final_place, state_place)?;
                        }
                    }
                }
            }
        }

        let mut group_columns_builder = self
            .method
            .group_columns_builder(self.hash_table.len(), &self.params);

        if !HAS_AGG {
            for group_entity in self.hash_table.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            let columns = group_columns_builder.finish()?;
            let mut num_rows = 0;
            let columns = columns
                .into_iter()
                .map(|col| {
                    num_rows = col.len();
                    let data_type = data_type_of_group_key_column!(col);
                    (Value::Column(col), data_type)
                })
                .collect();

            Ok(vec![Chunk::new_from_sequence(columns, num_rows)])
        } else {
            let aggregate_functions = &self.params.aggregate_functions;
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;

            let mut aggregates_column_builder = {
                let mut values = vec![];
                for aggregate_function in aggregate_functions {
                    let data_type = aggregate_function.return_type()?;
                    let builder = ColumnBuilder::with_capacity(&data_type, self.hash_table.len());
                    values.push((builder, data_type))
                }
                values
            };

            for (idx, aggregate_function) in aggregate_functions.iter().enumerate() {
                let places = self
                    .hash_table
                    .iter()
                    .map(|group_entity| {
                        let place = Into::<StateAddr>::into(*group_entity.get());
                        place.next(offsets_aggregate_states[idx])
                    })
                    .collect();

                let (builder, _) = aggregates_column_builder[idx].borrow_mut();
                aggregate_function.batch_merge_result(places, builder)?;
            }

            for group_entity in self.hash_table.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            // Build final state chunk.
            let mut num_rows = 0;
            let mut columns = aggregates_column_builder
                .into_iter()
                .map(|(builder, ty)| {
                    let col = builder.build();
                    num_rows = col.len();
                    (Value::Column(col), ty)
                })
                .collect::<Vec<_>>();

            let group_columns = group_columns_builder.finish()?;
            let group_columns = group_columns
                .into_iter()
                .map(|col| {
                    let datatype = data_type_of_group_key_column!(col);
                    (Value::Column(col), datatype)
                })
                .collect::<Vec<_>>();
            columns.extend_from_slice(&group_columns);
            Ok(vec![Chunk::new_from_sequence(columns, num_rows)])
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(&mut self, keys_iter: &Method::KeysColumnIter) -> StateAddrs {
        let iter = keys_iter.iter();
        let (len, _) = iter.size_hint();
        let mut places = Vec::with_capacity(len);

        unsafe {
            for key in iter {
                match self.hash_table.insert_and_entry(key) {
                    Ok(mut entry) => {
                        if let Some(place) = self.params.alloc_layout(&mut self.area) {
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
}

impl<const HAS_AGG: bool, Method> Drop for BucketAggregator<HAS_AGG, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method> + Send + 'static
{
    fn drop(&mut self) {
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

        for group_entity in self.hash_table.iter() {
            let place = Into::<StateAddr>::into(*group_entity.get());

            for (function, state_offset) in functions.iter().zip(state_offsets.iter()) {
                unsafe { function.drop_state(place.next(*state_offset)) }
            }
        }

        if let Some(temp_place) = self.temp_place {
            for (state_offset, function) in state_offsets.iter().zip(functions.iter()) {
                let place = temp_place.next(*state_offset);
                unsafe { function.drop_state(place) }
            }
        }
    }
}
