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

use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datavalues::ColumnRef;
use common_datavalues::MutableColumn;
use common_datavalues::MutableStringColumn;
use common_datavalues::ScalarColumnBuilder;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::StateAddr;
use common_hashtable::FastHash;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use tracing::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::aggregator_final_parallel::ParallelFinalAggregator;
use crate::pipelines::processors::transforms::aggregator::PartialAggregator;
use crate::pipelines::processors::transforms::aggregator::SingleStateAggregator;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::group_by::TwoLevelHashMethod;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;

pub trait TwoLevelAggregatorLike
where Self: Aggregator + Send
{
    const SUPPORT_TWO_LEVEL: bool;

    type TwoLevelAggregator: Aggregator;

    fn get_state_cardinality(&self) -> usize {
        0
    }

    fn convert_two_level(self) -> Result<TwoLevelAggregator<Self>> {
        Err(ErrorCode::Unimplemented(format!(
            "Two level aggregator is unimplemented for {}",
            Self::NAME
        )))
    }

    fn convert_two_level_block(_agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        Err(ErrorCode::Unimplemented(format!(
            "Two level aggregator is unimplemented for {}",
            Self::NAME
        )))
    }
}

impl<Method> TwoLevelAggregatorLike for PartialAggregator<true, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = Method::SUPPORT_TWO_LEVEL;

    type TwoLevelAggregator = PartialAggregator<true, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    // PartialAggregator<true, Method> -> TwoLevelAggregator<PartialAggregator<true, Method>>
    fn convert_two_level(mut self) -> Result<TwoLevelAggregator<Self>> {
        let instant = Instant::now();
        let method = self.method.clone();
        let two_level_method = TwoLevelHashMethod::create(method);
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
            "Convert to two level aggregator elapsed: {:?}",
            instant.elapsed()
        );

        self.states_dropped = true;
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<true, TwoLevelHashMethod<Method>> {
                area: self.area.take(),
                params: self.params.clone(),
                states_dropped: false,
                method: two_level_method,
                hash_table: two_level_hashtable,
            },
        })
    }

    fn convert_two_level_block(agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        let mut data_blocks = Vec::with_capacity(256);
        for (bucket, iterator) in agg.hash_table.two_level_iter() {
            let (capacity, _) = iterator.size_hint();

            let aggregator_params = agg.params.as_ref();
            let funcs = &aggregator_params.aggregate_functions;
            let aggr_len = funcs.len();
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            // Builders.
            let mut state_builders: Vec<MutableStringColumn> = (0..aggr_len)
                .map(|_| MutableStringColumn::with_capacity(capacity * 4))
                .collect();

            let mut group_key_builder = agg.method.keys_column_builder(capacity);

            for group_entity in iterator {
                let place = Into::<StateAddr>::into(*group_entity.get());

                for (idx, func) in funcs.iter().enumerate() {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.serialize(arg_place, state_builders[idx].values_mut())?;
                    state_builders[idx].commit_row();
                }

                group_key_builder.append_value(group_entity.key());
            }

            let schema = &agg.params.output_schema;
            let mut columns: Vec<ColumnRef> = Vec::with_capacity(schema.fields().len());

            for mut builder in state_builders {
                columns.push(builder.to_column());
            }

            columns.push(group_key_builder.finish());

            data_blocks.push(DataBlock::create_with_meta(
                agg.params.output_schema.clone(),
                columns,
                Some(AggregateInfo::create(bucket)),
            ));
        }

        agg.drop_states();
        Ok(data_blocks)
    }
}

impl<Method> TwoLevelAggregatorLike for PartialAggregator<false, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = Method::SUPPORT_TWO_LEVEL;

    type TwoLevelAggregator = PartialAggregator<false, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    // PartialAggregator<false, Method> -> TwoLevelAggregator<PartialAggregator<false, Method>>
    fn convert_two_level(mut self) -> Result<TwoLevelAggregator<Self>> {
        let instant = Instant::now();
        let method = self.method.clone();
        let two_level_method = TwoLevelHashMethod::create(method);
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
            "Convert to two level aggregator elapsed: {:?}",
            instant.elapsed()
        );

        self.states_dropped = true;
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<false, TwoLevelHashMethod<Method>> {
                area: self.area.take(),
                params: self.params.clone(),
                states_dropped: false,
                method: two_level_method,
                hash_table: two_level_hashtable,
            },
        })
    }

    fn convert_two_level_block(agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        let mut data_blocks = Vec::with_capacity(256);
        for (bucket, iterator) in agg.hash_table.two_level_iter() {
            let (capacity, _) = iterator.size_hint();
            let mut keys_column_builder = agg.method.keys_column_builder(capacity);

            for group_entity in iterator {
                keys_column_builder.append_value(group_entity.key());
            }

            let columns = keys_column_builder.finish();

            data_blocks.push(DataBlock::create_with_meta(
                agg.params.output_schema.clone(),
                vec![columns],
                Some(AggregateInfo::create(bucket)),
            ));
        }

        agg.drop_states();
        Ok(data_blocks)
    }
}

impl TwoLevelAggregatorLike for SingleStateAggregator<true> {
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = SingleStateAggregator<true>;
}

impl TwoLevelAggregatorLike for SingleStateAggregator<false> {
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = SingleStateAggregator<false>;
}

impl<Method> TwoLevelAggregatorLike for ParallelFinalAggregator<true, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = ParallelFinalAggregator<true, TwoLevelHashMethod<Method>>;
}

impl<Method> TwoLevelAggregatorLike for ParallelFinalAggregator<false, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = ParallelFinalAggregator<false, TwoLevelHashMethod<Method>>;
}

// Example: TwoLevelAggregator<PartialAggregator<true, Method>> ->
//      TwoLevelAggregator {
//          inner: PartialAggregator<true, TwoLevelMethod<Method>>
//      }
pub struct TwoLevelAggregator<T: TwoLevelAggregatorLike> {
    inner: T::TwoLevelAggregator,
}

impl<T: TwoLevelAggregatorLike> Aggregator for TwoLevelAggregator<T> {
    const NAME: &'static str = "TwoLevelAggregator";

    #[inline(always)]
    fn consume(&mut self, data: DataBlock) -> Result<()> {
        self.inner.consume(data)
    }

    #[inline(always)]
    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        T::convert_two_level_block(&mut self.inner)
    }
}
