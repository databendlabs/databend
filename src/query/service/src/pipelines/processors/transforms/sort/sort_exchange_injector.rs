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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::sorts::SortBound;
use databend_common_settings::FlightCompression;

use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

pub struct SortInjector {}

impl ExchangeInjector for SortInjector {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) | DataExchange::Broadcast(_) => unreachable!(),
            DataExchange::NodeToNodeExchange(exchange) => {
                Ok(Arc::new(Box::new(SortBoundScatter {
                    partitions: exchange.destination_ids.len(),
                })))
            }
        }
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        None
    }

    fn apply_merge_serializer(
        &self,
        _: &MergeExchangeParams,
        _: Option<FlightCompression>,
        _: &mut Pipeline,
    ) -> Result<()> {
        unreachable!()
    }

    fn apply_merge_deserializer(&self, _: &MergeExchangeParams, _: &mut Pipeline) -> Result<()> {
        unreachable!()
    }

    fn apply_shuffle_serializer(
        &self,
        params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        DefaultExchangeInjector::create().apply_shuffle_serializer(params, compression, pipeline)
    }

    fn apply_shuffle_deserializer(
        &self,
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        DefaultExchangeInjector::create().apply_shuffle_deserializer(params, pipeline)
    }
}

pub struct SortBoundScatter {
    partitions: usize,
}

impl FlightScatter for SortBoundScatter {
    fn name(&self) -> &'static str {
        "SortBound"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        bound_scatter(data_block, self.partitions)
    }
}

fn bound_scatter(data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
    let meta = *data_block
        .get_meta()
        .and_then(SortBound::downcast_ref_from)
        .unwrap();

    let empty = data_block.slice(0..0);
    let mut result = vec![empty; n];
    result[meta.index as usize % n] = data_block;

    Ok(result)
}
