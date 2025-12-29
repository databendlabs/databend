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

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_settings::FlightCompression;
use databend_common_storage::DataOperator;

use crate::physical_plans::AggregateShuffleMode;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::serde::TransformExchangeAggregateSerializer;
use crate::pipelines::processors::transforms::aggregator::serde::TransformExchangeAsyncBarrier;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateDeserializer;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSerializer;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillWriter;
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

pub struct HashTableHashScatter {
    pub buckets: usize,
    pub(crate) aggregate_params: Arc<AggregatorParams>,
}

fn scatter_payload(mut payload: Payload, buckets: usize) -> Result<Vec<Payload>> {
    let mut buckets = Vec::with_capacity(buckets);

    let group_types = payload.group_types.clone();
    let aggrs = payload.aggrs.clone();
    let mut state = PayloadFlushState::default();

    for _ in 0..buckets.capacity() {
        let p = Payload::new(
            payload.arena.clone(),
            group_types.clone(),
            aggrs.clone(),
            payload.states_layout().cloned(),
        );
        buckets.push(p);
    }

    // scatter each page of the payload.
    while payload.scatter(&mut state, buckets.len()) {
        // copy to the corresponding bucket.
        let probe_state = &*state.probe_state;
        for (bucket, (count, sel)) in buckets.iter_mut().zip(&probe_state.partition_entries) {
            if *count > 0 {
                bucket.copy_rows(&sel[..*count as usize], &state.addresses);
            }
        }
    }
    payload.state_move_out = true;

    Ok(buckets)
}

pub fn scatter_partitioned_payload(
    partitioned_payload: PartitionedPayload,
    buckets: usize,
) -> Result<Vec<PartitionedPayload>> {
    let mut buckets = Vec::with_capacity(buckets);

    let group_types = partitioned_payload.group_types.clone();
    let aggrs = partitioned_payload.aggrs.clone();
    let partition_count = partitioned_payload.partition_count() as u64;
    let mut state = PayloadFlushState::default();

    for _ in 0..buckets.capacity() {
        buckets.push(PartitionedPayload::new(
            group_types.clone(),
            aggrs.clone(),
            partition_count,
            partitioned_payload.arenas.clone(),
        ));
    }

    let mut payloads = Vec::with_capacity(buckets.len());

    for _ in 0..payloads.capacity() {
        payloads.push(Payload::new(
            Arc::new(Bump::new()),
            group_types.clone(),
            aggrs.clone(),
            partitioned_payload.states_layout().cloned(),
        ));
    }

    for mut payload in partitioned_payload.payloads.into_iter() {
        // scatter each page of the payload.
        while payload.scatter(&mut state, buckets.len()) {
            // copy to the corresponding bucket.
            for (bucket, (count, sel)) in payloads
                .iter_mut()
                .zip(&state.probe_state.partition_entries)
            {
                if *count > 0 {
                    bucket.copy_rows(&sel[..*count as usize], &state.addresses);
                }
            }
        }
        state.clear();
        payload.state_move_out = true;
    }

    for (idx, payload) in payloads.into_iter().enumerate() {
        buckets[idx].combine_single(payload, &mut state, None);
    }

    Ok(buckets)
}

impl FlightScatter for HashTableHashScatter {
    fn name(&self) -> &'static str {
        "HashTableHash"
    }

    fn execute(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
                let mut blocks = Vec::with_capacity(self.buckets);
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Serialized(payload) => {
                        let mut partition = payload.convert_to_partitioned_payload(
                            self.aggregate_params.group_data_types.clone(),
                            self.aggregate_params.aggregate_functions.clone(),
                            self.aggregate_params.num_states(),
                            0,
                            Arc::new(Bump::new()),
                        )?;
                        let payload = partition.payloads.pop();
                        if let Some(payload) = payload {
                            for (bucket, payload) in scatter_payload(payload, self.buckets)?
                                .into_iter()
                                .enumerate()
                            {
                                blocks.push(DataBlock::empty_with_meta(
                                    AggregateMeta::create_agg_payload(
                                        bucket as isize,
                                        payload,
                                        MAX_AGGREGATE_HASHTABLE_BUCKETS_NUM as usize,
                                    ),
                                ));
                            }
                        }
                    }
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::NewBucketSpilled(_) => unreachable!(),
                    AggregateMeta::NewSpilled(_) => unreachable!(),
                    AggregateMeta::AggregateSpilling(payload) => {
                        for p in scatter_partitioned_payload(payload, self.buckets)? {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::create_agg_spilling(p),
                            ));
                        }
                    }
                    AggregateMeta::AggregatePayload(p) => {
                        for payload in scatter_payload(p.payload, self.buckets)? {
                            blocks.push(DataBlock::empty_with_meta(
                                AggregateMeta::create_agg_payload(
                                    p.bucket,
                                    payload,
                                    p.max_partition_count,
                                ),
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
    ) -> Arc<dyn ExchangeInjector> {
        Arc::new(AggregateInjector {
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
                Ok(Arc::new(Box::new(HashTableHashScatter {
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
                AggregateShuffleMode::Row => Ok(Arc::new(Box::new(HashTableHashScatter {
                    buckets: exchange.destination_ids.len(),
                    aggregate_params: self.aggregator_params.clone(),
                }))),
                AggregateShuffleMode::Bucket(_) => {}
            },
        }
    }
}
