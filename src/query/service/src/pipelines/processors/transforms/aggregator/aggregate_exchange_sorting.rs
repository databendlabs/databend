// Copyright 2023 Datafuse Labs.
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

use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_hashtable::FastHash;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use strength_reduce::StrengthReducedU64;

use crate::api::DataExchange;
use crate::api::ExchangeInjector;
use crate::api::ExchangeSorting;
use crate::api::FlightScatter;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::serde::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::serde::BUCKET_TYPE;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::HashTableCell;
use crate::sessions::QueryContext;

struct AggregateExchangeSorting<Method: HashMethodBounds, V: Send + Sync + 'static> {
    _phantom: PhantomData<(Method, V)>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> ExchangeSorting
    for AggregateExchangeSorting<Method, V>
{
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        match data_block.get_meta() {
            None => Ok(-1),
            Some(block_meta_info) => {
                match AggregateMeta::<Method, V>::downcast_ref_from(block_meta_info) {
                    None => Err(ErrorCode::Internal(
                        "Internal error, AggregateExchangeSorting only recv AggregateMeta",
                    )),
                    Some(meta_info) => match meta_info {
                        AggregateMeta::Partitioned { .. } => unreachable!(),
                        AggregateMeta::Serialized(v) => Ok(v.bucket),
                        AggregateMeta::HashTable(v) => Ok(v.bucket),
                        AggregateMeta::Spilling(_) | AggregateMeta::Spilled(_) => Ok(-1),
                    },
                }
            }
        }
    }
}

struct HashTableHashScatter<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> {
    method: Method,
    buckets: usize,
    _phantom: PhantomData<V>,
}

impl<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> HashTableHashScatter<Method, V> {
    fn scatter(
        &self,
        mut payload: HashTablePayload<Method, V>,
    ) -> Result<Vec<HashTableCell<Method, V>>> {
        let mut buckets = Vec::with_capacity(self.buckets);

        for _ in 0..self.buckets {
            buckets.push(self.method.create_hash_table()?);
        }

        for item in payload.cell.hashtable.iter() {
            let mods = StrengthReducedU64::new(self.buckets as u64);
            let bucket_index = (item.key().fast_hash() % mods) as usize;

            unsafe {
                match buckets[bucket_index].insert_and_entry(item.key()) {
                    Ok(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                    Err(mut entry) => {
                        *entry.get_mut() = *item.get();
                    }
                }
            }
        }

        let mut res = Vec::with_capacity(buckets.len());
        let dropper = payload.cell._dropper.take();
        for bucket_table in buckets {
            res.push(HashTableCell::<Method, V>::create(
                bucket_table,
                dropper.clone().unwrap(),
            ));
        }

        Ok(res)
    }
}

impl<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> FlightScatter
    for HashTableHashScatter<Method, V>
{
    fn execute(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::<Method, V>::downcast_from(block_meta) {
                let mut blocks = Vec::with_capacity(1);
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Spilling(payload) => {
                        let bucket = payload.bucket;
                        for hashtable_cell in self.scatter(payload)? {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::<Method, V>::create_spilling(bucket, hashtable_cell),
                            ));
                        }
                    }
                    AggregateMeta::HashTable(payload) => {
                        let bucket = payload.bucket;
                        for hashtable_cell in self.scatter(payload)? {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::<Method, V>::create_spilling(bucket, hashtable_cell),
                            ));
                        }
                    }
                };

                return Ok(blocks);
            }
        }

        Err(ErrorCode::Internal(
            "Internal, HashTableHashScatter only recv AggregateMeta",
        ))
    }
}

pub struct AggregateInjector<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> {
    method: Method,
    _phantom: PhantomData<V>,
}

impl<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> AggregateInjector<Method, V> {
    pub fn create(method: Method) -> Arc<dyn ExchangeInjector> {
        Arc::new(AggregateInjector::<Method, V> {
            method,
            _phantom: Default::default(),
        })
    }
}

impl<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> ExchangeInjector
    for AggregateInjector<Method, V>
{
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) => unreachable!(),
            DataExchange::Broadcast(_) => unreachable!(),
            DataExchange::ShuffleDataExchange(exchange) => {
                Ok(Arc::new(Box::new(HashTableHashScatter::<Method, V> {
                    method: self.method.clone(),
                    buckets: exchange.destination_ids.len(),
                    _phantom: Default::default(),
                })))
            }
        }
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        Some(Arc::new(AggregateExchangeSorting::<Method, V> {
            _phantom: Default::default(),
        }))
    }
}
