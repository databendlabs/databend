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
use databend_common_pipeline::core::Pipeline;
use databend_common_settings::FlightCompression;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateDeserializer;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSerializer;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

/// Serializes the partial aggregate states merged into one node. Shuffled states
/// go through the global hash shuffle with [`AggregateShuffleCodec`] instead.
///
/// [`AggregateShuffleCodec`]: crate::pipelines::processors::transforms::aggregator::AggregateShuffleCodec
pub struct AggregateInjector {
    aggregator_params: Arc<AggregatorParams>,
}

impl AggregateInjector {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<dyn ExchangeInjector> {
        Arc::new(AggregateInjector {
            aggregator_params: params,
        })
    }
}

impl ExchangeInjector for AggregateInjector {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        _: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        unreachable!("aggregate states are shuffled by the global hash shuffle")
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        None
    }

    fn apply_merge_serializer(
        &self,
        _: &MergeExchangeParams,
        _compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let params = self.aggregator_params.clone();

        pipeline.add_transform(|input, output| {
            TransformAggregateSerializer::try_create(input, output, params.clone())
        })
    }

    fn apply_shuffle_serializer(
        &self,
        _: &ShuffleExchangeParams,
        _: Option<FlightCompression>,
        _: &mut Pipeline,
    ) -> Result<()> {
        unreachable!("aggregate states are shuffled by the global hash shuffle")
    }

    fn apply_merge_deserializer(
        &self,
        params: &MergeExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            TransformAggregateDeserializer::try_create(input, output, &params.schema)
        })
    }

    fn apply_shuffle_deserializer(
        &self,
        _: &ShuffleExchangeParams,
        _: &mut Pipeline,
    ) -> Result<()> {
        unreachable!("aggregate states are shuffled by the global hash shuffle")
    }
}
