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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use bumpalo::Bump;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::MultiwayStrategy;
use databend_common_settings::FlightCompression;
use itertools::Itertools;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::servers::flight::v1::exchange::serde::serialize_block;
use crate::servers::flight::v1::exchange::serde::ExchangeSerializeMeta;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeSorting;
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
                        compute_block_number(v.partition, v.max_partition_count)
                    }
                    AggregateMeta::BucketSpilled(_) => Ok(-1),
                },
            },
        }
    }
}

struct HashTableHashScatter {
    bucket_lookup: HashMap<String, usize>,
}

impl HashTableHashScatter {
    pub fn create(nodes: &[String]) -> Arc<Box<dyn FlightScatter>> {
        Arc::new(Box::new(HashTableHashScatter {
            bucket_lookup: nodes
                .iter()
                .cloned()
                .enumerate()
                .map(|(l, r)| (r, l))
                .collect::<HashMap<String, usize>>(),
        }))
    }
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
            payload.states_layout.clone(),
        );
        buckets.push(p);
    }

    // scatter each page of the payload.
    while payload.scatter(&mut state, buckets.len()) {
        // copy to the corresponding bucket.
        for (idx, bucket) in buckets.iter_mut().enumerate() {
            let count = state.probe_state.partition_count[idx];

            if count > 0 {
                let sel = &state.probe_state.partition_entries[idx];
                bucket.copy_rows(sel, count, &state.addresses);
            }
        }
    }
    payload.state_move_out = true;

    Ok(buckets)
}

// fn scatter_partitioned_payload(
//     partitioned_payload: PartitionedPayload,
//     buckets: usize,
// ) -> Result<Vec<PartitionedPayload>> {
//     let mut buckets = Vec::with_capacity(buckets);
//
//     let group_types = partitioned_payload.group_types.clone();
//     let aggrs = partitioned_payload.aggrs.clone();
//     let partition_count = partitioned_payload.partition_count() as u64;
//     let mut state = PayloadFlushState::default();
//
//     for _ in 0..buckets.capacity() {
//         buckets.push(PartitionedPayload::new(
//             group_types.clone(),
//             aggrs.clone(),
//             partition_count,
//             partitioned_payload.arenas.clone(),
//         ));
//     }
//
//     let mut payloads = Vec::with_capacity(buckets.len());
//
//     for _ in 0..payloads.capacity() {
//         payloads.push(Payload::new(
//             Arc::new(Bump::new()),
//             group_types.clone(),
//             aggrs.clone(),
//             partitioned_payload.states_layout.clone(),
//         ));
//     }
//
//     for mut payload in partitioned_payload.payloads.into_iter() {
//         // scatter each page of the payload.
//         while payload.scatter(&mut state, buckets.len()) {
//             // copy to the corresponding bucket.
//             for (idx, bucket) in payloads.iter_mut().enumerate() {
//                 let count = state.probe_state.partition_count[idx];
//
//                 if count > 0 {
//                     let sel = &state.probe_state.partition_entries[idx];
//                     bucket.copy_rows(sel, count, &state.addresses);
//                 }
//             }
//         }
//         state.clear();
//         payload.state_move_out = true;
//     }
//
//     for (idx, payload) in payloads.into_iter().enumerate() {
//         buckets[idx].combine_single(payload, &mut state, None);
//     }
//
//     Ok(buckets)
// }

impl FlightScatter for HashTableHashScatter {
    fn execute(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        // if let Some(block_meta) = data_block.take_meta() {
        //     if let Some(block_meta) = AggregateMeta::downcast_from(block_meta) {
        //         let mut blocks = Vec::with_capacity(self.bucket_lookup.len());
        //         match block_meta {
        //             AggregateMeta::Spilled(_) => unreachable!(),
        //             AggregateMeta::Serialized(_) => unreachable!(),
        //             AggregateMeta::Partitioned { .. } => unreachable!(),
        //             AggregateMeta::AggregateSpilling(_) => unreachable!(),
        //             AggregateMeta::BucketSpilled(v) => {
        //                 // v.destination_node
        //             }
        //             AggregateMeta::AggregatePayload(p) => {
        //                 for payload in scatter_payload(p.payload, self.buckets)? {
        //                     blocks.push(DataBlock::empty_with_meta(
        //                         AggregateMeta::create_agg_payload(
        //                             payload,
        //                             p.partition,
        //                             p.max_partition_count,
        //                         ),
        //                     ));
        //                 }
        //             }
        //         };
        //
        //         return Ok(blocks);
        //     }
        // }

        Err(ErrorCode::Internal(
            "Internal, HashTableHashScatter only recv AggregateMeta",
        ))
    }
}

pub struct AggregateInjector {
    ctx: Arc<QueryContext>,
    aggregator_params: Arc<AggregatorParams>,
}

impl AggregateInjector {
    pub fn create(
        ctx: Arc<QueryContext>,
        params: Arc<AggregatorParams>,
    ) -> Arc<dyn ExchangeInjector> {
        Arc::new(AggregateInjector {
            ctx,
            aggregator_params: params,
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
            DataExchange::ShuffleDataExchange(exchange) => {
                Ok(HashTableHashScatter::create(&exchange.destination_ids))
            }
        }
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        Some(Arc::new(AggregateExchangeSorting {}))
    }

    // fn apply_merge_serializer(
    //     &self,
    //     _: &MergeExchangeParams,
    //     _compression: Option<FlightCompression>,
    //     pipeline: &mut Pipeline,
    // ) -> Result<()> {
    //     let params = self.aggregator_params.clone();
    //
    //     let operator = DataOperator::instance().spill_operator();
    //     let location_prefix = self.ctx.query_id_spill_prefix();
    //
    //     pipeline.add_transform(|input, output| {
    //         Ok(ProcessorPtr::create(
    //             TransformAggregateSpillWriter::try_create(
    //                 self.ctx.clone(),
    //                 input,
    //                 output,
    //                 operator.clone(),
    //                 params.clone(),
    //                 location_prefix.clone(),
    //             )?,
    //         ))
    //     })?;
    //
    //     pipeline.add_transform(|input, output| {
    //         TransformAggregateSerializer::try_create(input, output, params.clone())
    //     })
    // }
    //
    // fn apply_shuffle_serializer(
    //     &self,
    //     shuffle_params: &ShuffleExchangeParams,
    //     compression: Option<FlightCompression>,
    //     pipeline: &mut Pipeline,
    // ) -> Result<()> {
    //     // let params = self.aggregator_params.clone();
    //     // let operator = DataOperator::instance().spill_operator();
    //     // let location_prefix = self.ctx.query_id_spill_prefix();
    //     //
    //     // let schema = shuffle_params.schema.clone();
    //     // let local_id = &shuffle_params.executor_id;
    //     // let local_pos = shuffle_params
    //     //     .destination_ids
    //     //     .iter()
    //     //     .position(|x| x == local_id)
    //     //     .unwrap();
    //     //
    //     // pipeline.add_transform(|input, output| {
    //     //     Ok(ProcessorPtr::create(
    //     //         TransformExchangeAggregateSerializer::try_create(
    //     //             self.ctx.clone(),
    //     //             input,
    //     //             output,
    //     //             operator.clone(),
    //     //             location_prefix.clone(),
    //     //             params.clone(),
    //     //             compression,
    //     //             schema.clone(),
    //     //             local_pos,
    //     //         )?,
    //     //     ))
    //     // })?;
    //     //
    //     // pipeline.add_transform(TransformExchangeAsyncBarrier::try_create)
    //     Ok(())
    // }
    //
    // fn apply_merge_deserializer(
    //     &self,
    //     params: &MergeExchangeParams,
    //     pipeline: &mut Pipeline,
    // ) -> Result<()> {
    //     pipeline.add_transform(|input, output| {
    //         TransformAggregateDeserializer::try_create(input, output, &params.schema)
    //     })
    // }
    //
    // fn apply_shuffle_deserializer(
    //     &self,
    //     params: &ShuffleExchangeParams,
    //     pipeline: &mut Pipeline,
    // ) -> Result<()> {
    //     pipeline.add_transform(|input, output| {
    //         TransformAggregateDeserializer::try_create(input, output, &params.schema)
    //     })
    // }
}

pub struct FlightExchange {
    local_id: String,
    bucket_lookup: HashMap<String, usize>,
    recv_bucket_lookup: Vec<String>,
    options: IpcWriteOptions,
    shuffle_scatter: Arc<Box<dyn FlightScatter>>,
}

impl FlightExchange {
    pub fn create(
        lookup: Vec<String>,
        compression: Option<FlightCompression>,
        shuffle_scatter: Arc<Box<dyn FlightScatter>>,
    ) -> Arc<FlightExchange> {
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                FlightCompression::Zstd => Some(CompressionType::ZSTD),
            },
        };

        let bucket_lookup = lookup
            .iter()
            .cloned()
            .enumerate()
            .map(|(x, y)| (y, x))
            .collect::<HashMap<String, usize>>();

        Arc::new(FlightExchange {
            local_id: GlobalConfig::instance().query.node_id.clone(),
            bucket_lookup,
            recv_bucket_lookup: lookup,
            options: IpcWriteOptions::default()
                .try_with_compression(compression)
                .unwrap(),
            shuffle_scatter,
        })
    }
}

impl FlightExchange {
    fn default_partition(&self, data_block: DataBlock) -> Result<Vec<(usize, DataBlock)>> {
        let data_blocks = self.shuffle_scatter.execute(data_block)?;

        let mut blocks = Vec::with_capacity(data_blocks.len());
        for (idx, data_block) in data_blocks.into_iter().enumerate() {
            if data_block.is_empty() || self.recv_bucket_lookup[idx] == self.local_id {
                blocks.push((idx, data_block));
                continue;
            }

            let data_block = serialize_block(0, data_block, &self.options)?;
            blocks.push((idx, data_block));
        }

        Ok(blocks)
    }
}

impl Exchange for FlightExchange {
    const NAME: &'static str = "AggregateExchange";
    const STRATEGY: MultiwayStrategy = MultiwayStrategy::Custom;

    fn partition(&self, mut data_block: DataBlock, n: usize) -> Result<Vec<(usize, DataBlock)>> {
        assert_eq!(self.bucket_lookup.len(), n);

        let Some(meta) = data_block.take_meta() else {
            return self.default_partition(data_block);
        };

        let Some(meta) = AggregateMeta::downcast_from(meta) else {
            return self.default_partition(data_block);
        };

        match meta {
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::Partitioned { .. } => unreachable!(),
            AggregateMeta::BucketSpilled(v) => match self.bucket_lookup.get(&v.destination_node) {
                None => unreachable!(),
                Some(idx) => match v.destination_node == self.local_id {
                    true => Ok(vec![(
                        *idx,
                        DataBlock::empty_with_meta(AggregateMeta::create_bucket_spilled(v)),
                    )]),
                    false => {
                        let block =
                            DataBlock::empty_with_meta(AggregateMeta::create_bucket_spilled(v));
                        Ok(vec![(*idx, serialize_block(-2, block, &self.options)?)])
                    }
                },
            },
            AggregateMeta::AggregatePayload(p) => {
                let mut blocks = Vec::with_capacity(n);
                for (idx, payload) in scatter_payload(p.payload, n)?.into_iter().enumerate() {
                    if self.recv_bucket_lookup[idx] == self.local_id {
                        blocks.push((
                            idx,
                            DataBlock::empty_with_meta(AggregateMeta::create_agg_payload(
                                payload,
                                p.partition,
                                p.max_partition_count,
                            )),
                        ));

                        continue;
                    }

                    let data_block = payload.aggregate_flush_all()?;
                    let data_block = serialize_block(p.partition, data_block, &self.options)?;
                    blocks.push((idx, data_block));
                }

                Ok(blocks)
            }
        }
    }

    fn multiway_pick(&self, data_blocks: &[Option<DataBlock>]) -> Result<usize> {
        let position = data_blocks.iter().position_min_by(|left, right| {
            let Some(left_block) = left else {
                return Ordering::Greater;
            };
            let Some(left_meta) = left_block.get_meta() else {
                return Ordering::Greater;
            };
            let Some(left_meta) = ExchangeSerializeMeta::downcast_ref_from(left_meta) else {
                return Ordering::Greater;
            };

            let Some(right_block) = right else {
                return Ordering::Less;
            };
            let Some(right_meta) = right_block.get_meta() else {
                return Ordering::Less;
            };
            let Some(right_meta) = ExchangeSerializeMeta::downcast_ref_from(right_meta) else {
                return Ordering::Less;
            };

            left_meta.block_number.cmp(&right_meta.block_number)
        });

        position.ok_or_else(|| ErrorCode::Internal("Cannot multiway pick with all none"))
    }
}
