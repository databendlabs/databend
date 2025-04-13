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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering as AtomicOrdering;
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
    node_list: Vec<String>,
    node_list_lookup: HashMap<String, usize>,

    options: IpcWriteOptions,
    global_max_partition: Arc<AtomicUsize>,
    shuffle_scatter: Arc<Box<dyn FlightScatter>>,
}

impl<const MULTIWAY_SORT: bool> FlightExchange<MULTIWAY_SORT> {
    pub fn create(
        node_list: Vec<String>,
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

        let node_list_lookup = node_list
            .iter()
            .cloned()
            .enumerate()
            .map(|(x, y)| (y, x))
            .collect::<HashMap<String, usize>>();

        Arc::new(FlightExchange {
            local_id: GlobalConfig::instance().query.node_id.clone(),
            node_list,
            node_list_lookup,
            options: IpcWriteOptions::default()
                .try_with_compression(compression)
                .unwrap(),
            shuffle_scatter,
            global_max_partition: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl<const MULTIWAY_SORT: bool> FlightExchange<MULTIWAY_SORT> {
    fn default_partition(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.node_list.is_empty() {
            let data_block = serialize_block(0, 0, 0, data_block, &self.options)?;
            return Ok(vec![data_block]);
        }

        let data_blocks = self.shuffle_scatter.execute(data_block)?;

        let mut blocks = Vec::with_capacity(data_blocks.len());
        for (idx, data_block) in data_blocks.into_iter().enumerate() {
            if self.node_list[idx] == self.local_id {
                blocks.push(data_block);
                continue;
            }

            blocks.push(serialize_block(0, 0, 0, data_block, &self.options)?);
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
        assert_eq!(self.node_list_lookup.len(), n);
        match AggregateMeta::downcast_from(meta).unwrap() {
            AggregateMeta::FinalPartition(_) => unreachable!(),
            AggregateMeta::InFlightPayload(_) => unreachable!(),
            AggregateMeta::SpilledPayload(v) => {
                let mut blocks = Vec::with_capacity(n);
                let global_max_partition = self.global_max_partition.load(AtomicOrdering::SeqCst);
                for node_id in &self.node_list {
                    let mut node_data_block = match *node_id == v.destination_node {
                        true => DataBlock::empty_with_meta(AggregateMeta::create_spilled_payload(
                            v.clone(),
                        )),
                        false => {
                            DataBlock::empty_with_meta(AggregateMeta::create_in_flight_payload(
                                v.get_sorting_partition(),
                                v.max_partition,
                                global_max_partition,
                            ))
                        }
                    };

                    if *node_id != self.local_id {
                        node_data_block = serialize_block(
                            v.get_sorting_partition(),
                            v.max_partition,
                            global_max_partition,
                            node_data_block,
                            &self.options,
                        )?
                    }

                    blocks.push(node_data_block);
                }

                Ok(blocks)
            }
            AggregateMeta::AggregatePayload(p) => {
                if p.payload.len() == 0 {
                    return Ok(vec![]);
                }

                let mut blocks = Vec::with_capacity(n);
                let global_max_partition = self.global_max_partition.load(AtomicOrdering::SeqCst);
                for (idx, payload) in scatter_payload(p.payload, n)?.into_iter().enumerate() {
                    if self.node_list[idx] == self.local_id {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::create_agg_payload(
                                payload,
                                p.partition,
                                p.max_partition,
                                global_max_partition,
                            ),
                        ));

                        continue;
                    }

                    let data_block = match payload.len() == 0 {
                        true => DataBlock::empty(),
                        false => payload.aggregate_flush_all()?,
                    };

                    let data_block =
                        data_block.add_meta(Some(AggregateMeta::create_in_flight_payload(
                            p.partition,
                            p.max_partition,
                            global_max_partition,
                        )))?;

                    let data_block = serialize_block(
                        p.partition,
                        p.max_partition,
                        global_max_partition,
                        data_block,
                        &self.options,
                    )?;
                    blocks.push(data_block);
                }

                Ok(blocks)
            }
        }
    }

    fn init_way(&self, _index: usize, block: &DataBlock) -> Result<()> {
        let max_partition = match block.get_meta() {
            None => 0,
            Some(meta) => match AggregateMeta::downcast_ref_from(meta) {
                None => 0,
                Some(v) => v.get_max_partition(),
            },
        };

        self.global_max_partition
            .fetch_max(max_partition, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn sorting_function(left_block: &DataBlock, right_block: &DataBlock) -> Ordering {
        let Some(left_meta) = left_block.get_meta() else {
            return Ordering::Equal;
        };

        let (l_partition, l_max_partition) =
            match ExchangeSerializeMeta::downcast_ref_from(left_meta) {
                Some(meta) => (meta.partition, meta.max_partition),
                None => {
                    let Some(meta) = AggregateMeta::downcast_ref_from(left_meta) else {
                        return Ordering::Equal;
                    };

                    (meta.get_sorting_partition(), meta.get_max_partition())
                }
            };

        let Some(right_meta) = right_block.get_meta() else {
            return Ordering::Equal;
        };

        let (r_partition, r_max_partition) =
            match ExchangeSerializeMeta::downcast_ref_from(right_meta) {
                Some(meta) => (meta.partition, meta.max_partition),
                None => {
                    let Some(meta) = AggregateMeta::downcast_ref_from(right_meta) else {
                        return Ordering::Equal;
                    };

                    (meta.get_sorting_partition(), meta.get_max_partition())
                }
            };

        // ORDER BY max_partition asc, partition asc
        match l_max_partition.cmp(&r_max_partition) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => l_partition.cmp(&r_partition),
        }
    }
}
