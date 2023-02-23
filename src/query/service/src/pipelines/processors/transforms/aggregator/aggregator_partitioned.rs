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

use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Value;
use common_functions::aggregates::StateAddr;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use tracing::info;

use super::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::aggregator_final_parallel::ParallelFinalAggregator;
use crate::pipelines::processors::transforms::aggregator::AggregateHashStateInfo;
use crate::pipelines::processors::transforms::aggregator::PartialAggregator;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;

pub trait PartitionedAggregatorLike
where Self: Aggregator + Send
{
    const SUPPORT_PARTITION: bool;

    type PartitionedAggregator: Aggregator;

    fn get_state_cardinality(&self) -> usize {
        0
    }

    fn get_state_bytes(&self) -> usize {
        0
    }

    fn convert_partitioned(self) -> Result<PartitionedAggregator<Self>> {
        Err(ErrorCode::Unimplemented(format!(
            "Partitioned aggregator is unimplemented for {}",
            Self::NAME
        )))
    }

    fn convert_partitioned_block(_agg: &mut Self::PartitionedAggregator) -> Result<Vec<DataBlock>> {
        Err(ErrorCode::Unimplemented(format!(
            "Partitioned aggregator is unimplemented for {}",
            Self::NAME
        )))
    }
}

impl<Method: HashMethodBounds, const HAS_AGG: bool> PartitionedAggregatorLike
    for PartialAggregator<HAS_AGG, Method>
{
    const SUPPORT_PARTITION: bool = Method::SUPPORT_PARTITIONED;

    type PartitionedAggregator = PartialAggregator<HAS_AGG, PartitionedHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    fn get_state_bytes(&self) -> usize {
        self.hash_table.bytes_len()
    }

    // PartialAggregator<HAS_AGG, Method> -> PartitionedAggregator<PartialAggregator<HAS_AGG, Method>>
    fn convert_partitioned(mut self) -> Result<PartitionedAggregator<Self>> {
        let instant = Instant::now();
        let method = self.method.clone();
        let two_level_method = PartitionedHashMethod::create(method);
        let mut two_level_hashtable = two_level_method.create_hash_table()?;

        unsafe {
            for item in self.hash_table.iter() {
                match two_level_hashtable.insert_and_entry(item.key()) {
                    Ok(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                    Err(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                };
            }
        }

        info!(
            "Convert to Partitioned aggregator elapsed: {:?}",
            instant.elapsed()
        );

        self.states_dropped = true;
        Ok(PartitionedAggregator::<Self> {
            inner: PartialAggregator::<HAS_AGG, PartitionedHashMethod<Method>> {
                area: self.area.take(),
                area_holder: None,
                params: self.params.clone(),
                states_dropped: false,
                method: two_level_method,
                hash_table: two_level_hashtable,
                generated: false,
                input_rows: self.input_rows,
                pass_state_to_final: self.pass_state_to_final,
                two_level_mode: true,
            },
        })
    }

    fn convert_partitioned_block(agg: &mut Self::PartitionedAggregator) -> Result<Vec<DataBlock>> {
        let mut data_blocks = Vec::with_capacity(256);

        fn clear_table<T: HashtableLike<Value = usize>>(table: &mut T, params: &AggregatorParams) {
            let aggregate_functions = &params.aggregate_functions;
            let offsets_aggregate_states = &params.offsets_aggregate_states;

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

            if !states.is_empty() {
                for group_entity in table.iter() {
                    let place = Into::<StateAddr>::into(*group_entity.get());

                    for (function, state_offset) in functions.iter().zip(states.iter()) {
                        unsafe { function.drop_state(place.next(*state_offset)) }
                    }
                }
            }

            table.clear();
        }

        for (bucket, inner_table) in agg.hash_table.iter_tables_mut().enumerate() {
            if inner_table.len() == 0 {
                continue;
            }

            if agg.pass_state_to_final {
                let table = std::mem::replace(inner_table, agg.method.method.create_hash_table()?);
                let rows = table.len();
                agg.try_holder_state();
                let meta = AggregateHashStateInfo::create(
                    bucket,
                    Box::new(table),
                    agg.area_holder.clone(),
                );
                let block = DataBlock::new_with_meta(vec![], rows, Some(meta));
                return Ok(vec![block]);
            }

            let capacity = inner_table.len();
            let iterator = inner_table.iter();

            let aggregator_params = agg.params.as_ref();
            let funcs = &aggregator_params.aggregate_functions;
            let aggr_len = funcs.len();
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            // Builders.
            let mut state_builders: Vec<StringColumnBuilder> = (0..aggr_len)
                .map(|_| StringColumnBuilder::with_capacity(capacity, capacity * 4))
                .collect();

            let value_size = estimated_key_size(inner_table);
            let mut group_key_builder = agg.method.keys_column_builder(capacity, value_size);

            for group_entity in iterator {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (idx, func) in funcs.iter().enumerate() {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.serialize(arg_place, &mut state_builders[idx].data)?;
                    state_builders[idx].commit_row();
                }

                group_key_builder.append_value(group_entity.key());
            }

            let mut columns = Vec::with_capacity(state_builders.len() + 1);
            for builder in state_builders.into_iter() {
                let col = builder.build();
                columns.push(BlockEntry {
                    value: Value::Column(Column::String(col)),
                    data_type: DataType::String,
                });
            }

            let col = group_key_builder.finish();
            let num_rows = col.len();
            let group_key_type = col.data_type();

            columns.push(BlockEntry {
                value: Value::Column(col),
                data_type: group_key_type,
            });

            data_blocks.push(DataBlock::new_with_meta(
                columns,
                num_rows,
                Some(AggregateInfo::create(bucket as isize)),
            ));

            clear_table(inner_table, &agg.params);

            // streaming return Partitioned blocks by bucket
            return Ok(data_blocks);
        }

        if !agg.pass_state_to_final {
            drop(agg.area.take());
            drop(agg.area_holder.take());
        }

        Ok(data_blocks)
    }
}

impl<Method: HashMethodBounds, const HAS_AGG: bool> PartitionedAggregatorLike
    for ParallelFinalAggregator<HAS_AGG, Method>
{
    const SUPPORT_PARTITION: bool = false;
    type PartitionedAggregator = ParallelFinalAggregator<HAS_AGG, PartitionedHashMethod<Method>>;
}

// Example: PartitionedAggregator<PartialAggregator<HAS_AGG, Method>> ->
//      PartitionedAggregator {
//          inner: PartialAggregator<HAS_AGG, PartitionedMethod<Method>>
//      }
pub struct PartitionedAggregator<T: PartitionedAggregatorLike> {
    inner: T::PartitionedAggregator,
}

impl<T: PartitionedAggregatorLike> Aggregator for PartitionedAggregator<T> {
    const NAME: &'static str = "PartitionedAggregator";

    #[inline(always)]
    fn consume(&mut self, data: DataBlock) -> Result<()> {
        self.inner.consume(data)
    }

    #[inline(always)]
    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        T::convert_partitioned_block(&mut self.inner)
    }
}
