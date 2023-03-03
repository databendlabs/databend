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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;

use crate::api::{DataExchange, ExchangeInjector, ExchangeSorting, FlightScatter};
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::{AggregateMeta, HashTablePayload};
use crate::pipelines::processors::transforms::aggregator::serde::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::serde::BUCKET_TYPE;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::sessions::QueryContext;

pub struct AggregateExchangeSorting {}

impl AggregateExchangeSorting {
    pub fn create() -> Arc<dyn ExchangeSorting> {
        Arc::new(AggregateExchangeSorting {})
    }
}

impl ExchangeSorting for AggregateExchangeSorting {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        match data_block.get_meta() {
            None => Ok(-1),
            Some(block_meta_info) => match AggregateSerdeMeta::downcast_ref_from(block_meta_info) {
                None => Err(ErrorCode::Internal(
                    "Internal error, AggregateExchangeSorting only recv AggregateSerdeMeta",
                )),
                Some(meta_info) => match meta_info.typ == BUCKET_TYPE {
                    true => Ok(meta_info.bucket),
                    false => Ok(-1),
                },
            },
        }
    }
}

struct HashTableHashScatter<Method: HashMethodBounds, V: Send + Sync + 'static> {}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> HashTableHashScatter<Method, V> {
    fn scatter(&self, payload: HashTablePayload<Method, V>) -> Vec<HashTablePayload<Method, V>> {
        // TODO;
        unimplemented!()
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> FlightScatter for HashTableHashScatter<Method, V> {
    fn execute(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::<Method, V>::downcast_from(block_meta) {
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Spilling(payload) => self.scatter(payload),
                    AggregateMeta::HashTable(payload) => self.scatter(payload),
                }
            }
        }
        todo!()
    }
}

pub struct AggregateInjector {}

impl ExchangeInjector for AggregateInjector {
    fn flight_scatter(&self, ctx: &Arc<QueryContext>, exchange: &DataExchange) -> Result<Arc<Box<dyn FlightScatter>>> {
        // TODO:
        todo!()
    }
}
