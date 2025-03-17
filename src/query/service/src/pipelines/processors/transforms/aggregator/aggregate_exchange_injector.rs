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
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_settings::FlightCompression;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::servers::flight::v1::exchange::serde::serialize_block;
use crate::servers::flight::v1::exchange::serde::ExchangeSerializeMeta;
use crate::servers::flight::v1::scatter::FlightScatter;

pub fn compute_block_number(bucket: isize, max_partition_count: usize) -> Result<isize> {
    match bucket.is_negative() {
        true => Ok(((1_usize << 16) & (bucket.unsigned_abs() << 8) & max_partition_count) as isize),
        false => Ok((((bucket as usize) << 8) & max_partition_count) as isize),
    }
}

pub fn restore_block_number(value: isize) -> (isize, usize) {
    let mut value = value as usize;
    let max_partition = value & 0xFF_usize;
    value >>= 8;
    let abs_partition = value & 0xFF_usize;
    value >>= 8;

    match value & 1 {
        1 => (0 - abs_partition as isize, max_partition),
        _ => (abs_partition as isize, max_partition),
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

pub struct FlightExchange<const MULTIWAY_SORT: bool> {
    local_id: String,
    bucket_lookup: HashMap<String, usize>,
    rev_bucket_lookup: Vec<String>,
    options: IpcWriteOptions,
    shuffle_scatter: Arc<Box<dyn FlightScatter>>,
}

impl<const MULTIWAY_SORT: bool> FlightExchange<MULTIWAY_SORT> {
    pub fn create(
        lookup: Vec<String>,
        compression: Option<FlightCompression>,
        shuffle_scatter: Arc<Box<dyn FlightScatter>>,
    ) -> Arc<Self> {
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
            rev_bucket_lookup: lookup,
            options: IpcWriteOptions::default()
                .try_with_compression(compression)
                .unwrap(),
            shuffle_scatter,
        })
    }
}

impl<const MULTIWAY_SORT: bool> FlightExchange<MULTIWAY_SORT> {
    fn default_partition(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.rev_bucket_lookup.is_empty() {
            let data_block = serialize_block(0, 0, data_block, &self.options)?;
            return Ok(vec![data_block]);
        }

        let data_blocks = self.shuffle_scatter.execute(data_block)?;

        let mut blocks = Vec::with_capacity(data_blocks.len());
        for (idx, data_block) in data_blocks.into_iter().enumerate() {
            if self.rev_bucket_lookup[idx] == self.local_id {
                blocks.push(data_block);
                continue;
            }

            blocks.push(serialize_block(0, 0, data_block, &self.options)?);
        }

        Ok(blocks)
    }
}

impl<const MULTIWAY_SORT: bool> Exchange for FlightExchange<MULTIWAY_SORT> {
    const NAME: &'static str = "AggregateExchange";
    const MULTIWAY_SORT: bool = MULTIWAY_SORT;

    fn partition(&self, mut data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let Some(meta) = data_block.take_meta() else {
            // only exchange data
            if data_block.is_empty() {
                return Ok(vec![]);
            }

            return self.default_partition(data_block);
        };

        let Some(_) = AggregateMeta::downcast_ref_from(&meta) else {
            return self.default_partition(data_block.add_meta(Some(meta))?);
        };

        assert!(MULTIWAY_SORT);
        assert_eq!(self.bucket_lookup.len(), n);
        match AggregateMeta::downcast_from(meta).unwrap() {
            AggregateMeta::Serialized(_) => unreachable!(),
            AggregateMeta::FinalPartition => unreachable!(),
            AggregateMeta::InFlightPayload(_) => unreachable!(),
            AggregateMeta::SpilledPayload(v) => {
                let block_num = compute_block_number(-1, v.max_partition)?;

                let mut blocks = Vec::with_capacity(n);
                for local in &self.rev_bucket_lookup {
                    blocks.push(match *local == self.local_id {
                        true => DataBlock::empty_with_meta(
                            AggregateMeta::create_in_flight_payload(-1, v.max_partition),
                        ),
                        false => DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
                            block_num,
                            v.global_max_partition,
                            Vec::with_capacity(0),
                        )),
                    });
                }

                let index = *self.bucket_lookup.get(&v.destination_node).unwrap();
                blocks[index] = match v.destination_node == self.local_id {
                    true => DataBlock::empty_with_meta(AggregateMeta::create_spilled_payload(v)),
                    false => serialize_block(
                        block_num,
                        v.global_max_partition,
                        DataBlock::empty_with_meta(AggregateMeta::create_spilled_payload(v)),
                        &self.options,
                    )?,
                };

                Ok(blocks)
            }
            AggregateMeta::AggregatePayload(p) => {
                if p.payload.len() == 0 {
                    return Ok(vec![]);
                }

                let mut blocks = Vec::with_capacity(n);
                for (idx, payload) in scatter_payload(p.payload, n)?.into_iter().enumerate() {
                    if self.rev_bucket_lookup[idx] == self.local_id {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::create_agg_payload(
                                payload,
                                p.partition,
                                p.max_partition,
                            ),
                        ));

                        continue;
                    }

                    let data_block = match payload.len() == 0 {
                        true => DataBlock::empty(),
                        false => payload.aggregate_flush_all()?,
                    };

                    let data_block = data_block.add_meta(Some(
                        AggregateMeta::create_in_flight_payload(p.partition, p.max_partition),
                    ))?;

                    let block_number = compute_block_number(p.partition, p.max_partition)?;
                    let data_block = serialize_block(
                        block_number,
                        p.global_max_partition,
                        data_block,
                        &self.options,
                    )?;
                    blocks.push(data_block);
                }

                Ok(blocks)
            }
        }
    }

    fn multiway_pick(data_blocks: &mut [Option<DataBlock>]) -> Option<usize> {
        let mut global_max_partition = 0_usize;
        let global_max_partition_ref = &mut global_max_partition;

        let min_position = data_blocks
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| x.as_ref().map(|d| (idx, d)))
            .min_by(move |(left_idx, left_block), (right_idx, right_block)| {
                let Some(left_meta) = left_block.get_meta() else {
                    return Ordering::Equal;
                };
                let Some(left_meta) = ExchangeSerializeMeta::downcast_ref_from(left_meta) else {
                    return Ordering::Equal;
                };

                let Some(right_meta) = right_block.get_meta() else {
                    return Ordering::Equal;
                };
                let Some(right_meta) = ExchangeSerializeMeta::downcast_ref_from(right_meta) else {
                    return Ordering::Equal;
                };

                let max_block_number = left_meta.max_block_number.max(right_meta.max_block_number);
                *global_max_partition_ref = (*global_max_partition_ref).max(max_block_number);
                let (l_partition, l_max_partition) = restore_block_number(left_meta.block_number);
                let (r_partition, r_max_partition) = restore_block_number(right_meta.block_number);

                // ORDER BY max_partition asc, partition asc, idx asc
                match l_max_partition.cmp(&r_max_partition) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Equal => match l_partition.cmp(&r_partition) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Equal => left_idx.cmp(right_idx),
                    },
                }
            })
            .map(|(idx, _)| idx);

        if let Some(min_pos) = min_position {
            if global_max_partition == 0 {
                return Some(min_pos);
            }

            if let Some(mut block) = data_blocks[min_pos].take() {
                let mut meta =
                    ExchangeSerializeMeta::downcast_from(block.take_meta().unwrap()).unwrap();
                meta.max_block_number = global_max_partition;
                data_blocks[min_pos] = Some(block.add_meta(Some(Box::new(meta))).unwrap());
            }
        }

        min_position
    }
}
