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
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_settings::FlightCompression;

use crate::physical_plans::AggregateShuffleMode;
use crate::pipelines::processors::transforms::aggregator::AggregateBucketScatter;
use crate::pipelines::processors::transforms::aggregator::AggregateRowScatter;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateDeserializer;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSerializer;
use crate::pipelines::processors::transforms::aggregator::serde::TransformExchangeAggregateSerializer;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

pub struct AggregateInjector {
    ctx: Arc<QueryContext>,
    aggregator_params: Arc<AggregatorParams>,
    shuffle_mode: AggregateShuffleMode,
}

impl AggregateInjector {
    pub fn create(
        ctx: Arc<QueryContext>,
        params: Arc<AggregatorParams>,
        shuffle_mode: AggregateShuffleMode,
    ) -> Arc<dyn ExchangeInjector> {
        Arc::new(AggregateInjector {
            ctx,
            aggregator_params: params,
            shuffle_mode,
        })
    }
}

impl ExchangeInjector for AggregateInjector {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) => unreachable!(),
            DataExchange::Broadcast(_) => unreachable!(),
            DataExchange::GlobalShuffleExchange(_) => unreachable!(),
            DataExchange::NodeToNodeExchange(exchange) => match self.shuffle_mode {
                AggregateShuffleMode::Row => Ok(Arc::new(Box::new(AggregateRowScatter {
                    buckets: exchange.destination_ids.len(),
                    aggregate_params: self.aggregator_params.clone(),
                }))),
                AggregateShuffleMode::Bucket(_) => Ok(Arc::new(Box::new(AggregateBucketScatter {
                    buckets: exchange.destination_ids.len(),
                }))),
            },
        }
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
        shuffle_params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let params = self.aggregator_params.clone();

        let local_id = &shuffle_params.executor_id;
        let local_pos = shuffle_params
            .destination_ids
            .iter()
            .position(|x| x == local_id)
            .unwrap();

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformExchangeAggregateSerializer::try_create(
                    self.ctx.clone(),
                    input,
                    output,
                    params.clone(),
                    compression,
                    local_pos,
                )?,
            ))
        })?;

        Ok(())
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
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_transform(|input, output| {
            TransformAggregateDeserializer::try_create(input, output, &params.schema)
        })?;
        Ok(())
    }
}
