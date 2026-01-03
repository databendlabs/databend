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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_settings::FlightCompression;
use databend_common_storage::DataOperator;

use crate::physical_plans::AggregateShuffleMode;
use crate::pipelines::processors::transforms::aggregator::AggregateBucketScatter;
use crate::pipelines::processors::transforms::aggregator::AggregateRowScatter;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateDeserializer;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSerializer;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillWriter;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::TransformExchangeAggregateSerializer;
use crate::pipelines::processors::transforms::aggregator::serde::TransformExchangeAsyncBarrier;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

struct AggregateExchangeSorting {}

pub fn compute_block_number(bucket: isize, max_partition_count: usize) -> Result<isize> {
    Ok(max_partition_count as isize * 1000 + bucket)
}

impl ExchangeSorting for AggregateExchangeSorting {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        match data_block.get_meta() {
            None => Ok(-1),
            Some(block_meta_info) => match AggregateMeta::downcast_ref_from(block_meta_info) {
                None => Err(ErrorCode::Internal(format!(
                    "Internal error, AggregateExchangeSorting only recv AggregateMeta {:?}",
                    serde_json::to_string(block_meta_info)
                ))),
                Some(meta_info) => match meta_info {
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(v) => {
                        compute_block_number(v.bucket, v.max_partition_count)
                    }
                    AggregateMeta::AggregatePayload(v) => {
                        compute_block_number(v.bucket, v.max_partition_count)
                    }
                    AggregateMeta::AggregateSpilling(_)
                    | AggregateMeta::Spilled(_)
                    | AggregateMeta::BucketSpilled(_)
                    | AggregateMeta::NewBucketSpilled(_)
                    | AggregateMeta::NewSpilled(_) => Ok(-1),
                },
            },
        }
    }
}

pub struct AggregateInjector<const ENABLE_EXPERIMENT: bool> {
    ctx: Arc<QueryContext>,
    aggregator_params: Arc<AggregatorParams>,
    shuffle_mode: AggregateShuffleMode,
}

impl<const ENABLE_EXPERIMENT: bool> AggregateInjector<ENABLE_EXPERIMENT> {
    pub fn create(
        ctx: Arc<QueryContext>,
        params: Arc<AggregatorParams>,
        shuffle_mode: AggregateShuffleMode,
    ) -> Arc<dyn ExchangeInjector>
    where
        Self: ExchangeInjector,
    {
        Arc::new(AggregateInjector::<ENABLE_EXPERIMENT> {
            ctx,
            aggregator_params: params,
            shuffle_mode,
        })
    }
}

impl ExchangeInjector for AggregateInjector<false> {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) => unreachable!(),
            DataExchange::Broadcast(_) => unreachable!(),
            DataExchange::NodeToNodeExchange(exchange) => {
                Ok(Arc::new(Box::new(AggregateRowScatter {
                    buckets: exchange.destination_ids.len(),
                    aggregate_params: self.aggregator_params.clone(),
                })))
            }
        }
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        Some(Arc::new(AggregateExchangeSorting {}))
    }

    fn apply_merge_serializer(
        &self,
        _: &MergeExchangeParams,
        _compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let params = self.aggregator_params.clone();

        let operator = DataOperator::instance().spill_operator();
        let location_prefix = self.ctx.query_id_spill_prefix();

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformAggregateSpillWriter::try_create(
                    self.ctx.clone(),
                    input,
                    output,
                    operator.clone(),
                    params.clone(),
                    location_prefix.clone(),
                )?,
            ))
        })?;

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
        let operator = DataOperator::instance().spill_operator();
        let location_prefix = self.ctx.query_id_spill_prefix();

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
                    operator.clone(),
                    location_prefix.clone(),
                    params.clone(),
                    compression,
                    local_pos,
                )?,
            ))
        })?;
        pipeline.add_transform(TransformExchangeAsyncBarrier::try_create)
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

impl ExchangeInjector for AggregateInjector<true> {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) => unreachable!(),
            DataExchange::Broadcast(_) => unreachable!(),
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
        let operator = DataOperator::instance().spill_operator();
        let location_prefix = self.ctx.query_id_spill_prefix();

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
                    operator.clone(),
                    location_prefix.clone(),
                    params.clone(),
                    compression,
                    local_pos,
                )?,
            ))
        })?;

        // TODO: this could be removed when experiment aggregate stabilize
        pipeline.add_transform(TransformExchangeAsyncBarrier::try_create)
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
