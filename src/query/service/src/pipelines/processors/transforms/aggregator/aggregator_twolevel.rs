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
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_hashtable::FastHash;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use tracing::info;

use super::BucketAggregator;
use crate::pipelines::processors::transforms::aggregator::aggregate_info::AggregateInfo;
use crate::pipelines::processors::transforms::aggregator::aggregator_final_parallel::ParallelFinalAggregator;
use crate::pipelines::processors::transforms::aggregator::PartialAggregator;
use crate::pipelines::processors::transforms::aggregator::SingleStateAggregator;
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

impl<Method, const HAS_AGG: bool> TwoLevelAggregatorLike for PartialAggregator<HAS_AGG, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = Method::SUPPORT_TWO_LEVEL;

    type TwoLevelAggregator = PartialAggregator<HAS_AGG, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    // PartialAggregator<HAS_AGG, Method> -> TwoLevelAggregator<PartialAggregator<HAS_AGG, Method>>
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
            inner: PartialAggregator::<HAS_AGG, TwoLevelHashMethod<Method>> {
                area: self.area.take(),
                params: self.params.clone(),
                states_dropped: false,
                method: two_level_method,
                hash_table: two_level_hashtable,
                generated: false,
                input_rows: self.input_rows,
            },
        })
    }

    fn convert_two_level_block(agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        for (bucket, inner_table) in agg.hash_table.iter_tables_mut().enumerate() {
            if inner_table.len() == 0 {
                continue;
            }

            let blocks = Self::generate_data(inner_table, &agg.params, &agg.method.method)?;
            Self::clear_table(inner_table, &agg.params);
            // streaming return two level blocks by bucket
            let blocks = blocks
                .into_iter()
                .map(|block| {
                    block
                        .add_meta(Some(AggregateInfo::create(bucket as isize)))
                        .unwrap()
                })
                .collect::<Vec<_>>();

            return Ok(blocks);
        }

        drop(agg.area.take());
        agg.states_dropped = true;
        Ok(vec![])
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

impl<Method, const HAS_AGG: bool> TwoLevelAggregatorLike
    for ParallelFinalAggregator<HAS_AGG, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = ParallelFinalAggregator<HAS_AGG, TwoLevelHashMethod<Method>>;
}

impl<Method, const HAS_AGG: bool> TwoLevelAggregatorLike for BucketAggregator<HAS_AGG, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = true;
    type TwoLevelAggregator = BucketAggregator<HAS_AGG, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

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
            "Convert to two level BucketAggregator elapsed: {:?}",
            instant.elapsed()
        );

        self.states_dropped = true;
        let mut inner = BucketAggregator::<HAS_AGG, TwoLevelHashMethod<Method>>::create(
            two_level_method,
            self.params.clone(),
        )?;
        inner.temp_place = self.temp_place.clone();

        Ok(TwoLevelAggregator::<Self> { inner })
    }

    fn convert_two_level_block(agg: &mut Self::TwoLevelAggregator) -> Result<Vec<DataBlock>> {
        for inner_table in agg.hash_table.iter_tables_mut() {
            if inner_table.len() == 0 {
                continue;
            }

            let blocks = Self::generate_data(inner_table, &agg.params, &agg.method.method)?;
            Self::clear_table(inner_table, &agg.params);
            return Ok(blocks);
        }

        // some temp place stats dropped in drop function
        // agg.states_dropped = true;
        Ok(vec![])
    }
}

// Example: TwoLevelAggregator<PartialAggregator<HAS_AGG, Method>> ->
//      TwoLevelAggregator {
//          inner: PartialAggregator<HAS_AGG, TwoLevelMethod<Method>>
//      }
pub struct TwoLevelAggregator<T: TwoLevelAggregatorLike> {
    inner: T::TwoLevelAggregator,
}

impl<T: TwoLevelAggregatorLike> Aggregator for TwoLevelAggregator<T> {
    const NAME: &'static str = "TwoLevelAggregator";

    #[inline(always)]
    fn consume(&mut self, data: DataBlock) -> Result<bool> {
        self.inner.consume(data)
    }

    #[inline(always)]
    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        T::convert_two_level_block(&mut self.inner)
    }
    #[inline(always)]
    fn should_expand_table(&self) -> bool {
        self.inner.should_expand_table()
    }

    // only implement for partial aggregator
    fn streaming_consume(&mut self, block: DataBlock) -> Result<DataBlock> {
        self.inner.streaming_consume(block)
    }
}
