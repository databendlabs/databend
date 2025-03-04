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
use std::time::Instant;
use std::vec;

use bumpalo::Bump;
use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::write_column;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::InputColumns;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::MemorySettings;
use opendal::Operator;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::sessions::QueryContext;
use crate::spillers::SpillWriter;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

#[allow(clippy::enum_variant_names)]
enum HashTable {
    MovedOut,
    AggregateHashTable(AggregateHashTable),
}

impl Default for HashTable {
    fn default() -> Self {
        Self::MovedOut
    }
}

// SELECT column_name, agg(xxx) FROM table_name GROUP BY column_name
pub struct TransformPartialAggregate {
    hash_table: HashTable,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    start: Instant,
    first_block_start: Option<Instant>,
    processed_bytes: usize,
    processed_rows: usize,
    settings: MemorySettings,
    configure_peer_nodes: Vec<String>,
    spilling_state: Option<HashtableSpillingState>,
    spiller: Arc<Spiller>,
    spill_blocks: Vec<DataBlock>,
}

impl TransformPartialAggregate {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
        location_prefix: String,
    ) -> Result<Box<dyn Processor>> {
        let hash_table = {
            let arena = Arc::new(Bump::new());
            match !params.has_distinct_combinator() {
                true => HashTable::AggregateHashTable(AggregateHashTable::new(
                    params.group_data_types.clone(),
                    params.aggregate_functions.clone(),
                    config,
                    arena,
                )),
                false => {
                    let max_radix_bits = config.max_radix_bits;
                    HashTable::AggregateHashTable(AggregateHashTable::new(
                        params.group_data_types.clone(),
                        params.aggregate_functions.clone(),
                        config.with_initial_radix_bits(max_radix_bits),
                        arena,
                    ))
                }
            }
        };

        let config = SpillerConfig {
            spiller_type: SpillerType::Aggregation,
            location_prefix,
            disk_spill: None,
            use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
        };

        let spiller = Spiller::create(ctx.clone(), operator, config.clone())?;

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate {
                params,
                hash_table,
                probe_state: ProbeState::default(),
                settings: MemorySettings::from_aggregate_settings(&ctx)?,
                start: Instant::now(),
                first_block_start: None,
                processed_bytes: 0,
                processed_rows: 0,
                configure_peer_nodes: vec![],
                spilling_state: None,
                spiller: Arc::new(spiller),
                spill_blocks: vec![],
            },
        ))
    }

    // Block should be `convert_to_full`.
    #[inline(always)]
    fn aggregate_arguments<'a>(
        block: &'a DataBlock,
        aggregate_functions_arguments: &'a [Vec<usize>],
    ) -> Vec<InputColumns<'a>> {
        aggregate_functions_arguments
            .iter()
            .map(|function_arguments| InputColumns::new_block_proxy(function_arguments, block))
            .collect::<Vec<_>>()
    }

    #[inline(always)]
    fn execute_one_block(&mut self, block: DataBlock) -> Result<()> {
        let is_agg_index_block = block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default();

        let block = block.consume_convert_to_full();
        let group_columns = InputColumns::new_block_proxy(&self.params.group_columns, &block);
        let rows_num = block.num_rows();

        self.processed_bytes += block.memory_size();
        self.processed_rows += rows_num;
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

        {
            match &mut self.hash_table {
                HashTable::MovedOut => unreachable!(),
                HashTable::AggregateHashTable(hashtable) => {
                    let (params_columns, states_index) = if is_agg_index_block {
                        let num_columns = block.num_columns();
                        let states_count = self
                            .params
                            .states_layout
                            .as_ref()
                            .map(|layout| layout.states_loc.len())
                            .unwrap_or(0);
                        (
                            vec![],
                            (num_columns - states_count..num_columns).collect::<Vec<_>>(),
                        )
                    } else {
                        (
                            Self::aggregate_arguments(
                                &block,
                                &self.params.aggregate_functions_arguments,
                            ),
                            vec![],
                        )
                    };

                    let agg_states = if !states_index.is_empty() {
                        InputColumns::new_block_proxy(&states_index, &block)
                    } else {
                        (&[]).into()
                    };

                    let _ = hashtable.add_groups(
                        &mut self.probe_state,
                        group_columns,
                        &params_columns,
                        agg_states,
                        rows_num,
                    )?;
                    Ok(())
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AccumulatingTransform for TransformPartialAggregate {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.execute_one_block(block)?;

        // if self.settings.check_spill() {
        //     if let HashTable::AggregateHashTable(v) = std::mem::take(&mut self.hash_table) {
        //         let group_types = v.payload.group_types.clone();
        //         let aggrs = v.payload.aggrs.clone();
        //         v.config.update_current_max_radix_bits();
        //         let config = v
        //             .config
        //             .clone()
        //             .with_initial_radix_bits(v.config.max_radix_bits);
        //
        //         let mut state = PayloadFlushState::default();
        //
        //         // repartition to max for normalization
        //         let partitioned_payload = v
        //             .payload
        //             .repartition(1 << config.max_radix_bits, &mut state);
        //
        //         let blocks = vec![DataBlock::empty_with_meta(
        //             AggregateMeta::create_agg_spilling(partitioned_payload),
        //         )];
        //
        //         let arena = Arc::new(Bump::new());
        //         self.hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
        //             group_types,
        //             aggrs,
        //             config,
        //             arena,
        //         ));
        //         return Ok(blocks);
        //     }
        //
        //     unreachable!()
        // }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output && std::thread::panicking() {
                true => vec![],
                false => unreachable!(),
            },
            HashTable::AggregateHashTable(hashtable) => {
                let partition_count = hashtable.payload.partition_count();
                let mut blocks = Vec::with_capacity(partition_count);

                log::info!(
                    "Aggregated {} to {} rows in {} sec(real: {}). ({} rows/sec, {}/sec, {})",
                    self.processed_rows,
                    hashtable.payload.len(),
                    self.start.elapsed().as_secs_f64(),
                    if let Some(t) = &self.first_block_start {
                        t.elapsed().as_secs_f64()
                    } else {
                        self.start.elapsed().as_secs_f64()
                    },
                    convert_number_size(
                        self.processed_rows as f64 / self.start.elapsed().as_secs_f64()
                    ),
                    convert_byte_size(
                        self.processed_bytes as f64 / self.start.elapsed().as_secs_f64()
                    ),
                    convert_byte_size(self.processed_bytes as f64),
                );

                for (bucket, payload) in hashtable.payload.payloads.into_iter().enumerate() {
                    if payload.len() != 0 {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::create_agg_payload(
                                bucket as isize,
                                payload,
                                partition_count,
                            ),
                        ));
                    }
                }

                blocks
            }
        })
    }

    fn configure_peer_nodes(&mut self, nodes: &[String]) {
        self.configure_peer_nodes = nodes.to_vec();
    }

    fn need_spill(&self) -> bool {
        self.settings.check_spill()
    }

    fn prepare_spill_payload(&mut self) -> Result<bool> {
        if self.spilling_state.is_none() {
            let HashTable::AggregateHashTable(hashtable) = std::mem::take(&mut self.hash_table)
            else {
                return Ok(false);
            };

            if hashtable.len() == 0 {
                return Ok(false);
            }

            self.spilling_state = Some(HashtableSpillingState::create(
                hashtable,
                self.configure_peer_nodes.len(),
            ));
        }

        let Some(hashtable_spilling_state) = &mut self.spilling_state else {
            return Ok(false);
        };

        if hashtable_spilling_state.finished {
            return Ok(false);
        }

        hashtable_spilling_state.last_prepare_payload =
            hashtable_spilling_state.serialize_partition_payload()?;
        Ok(true)
    }

    async fn flush_spill_payload(&mut self) -> Result<bool> {
        let hashtable_spilling_state = self.spilling_state.as_mut().unwrap();
        let max_partition = 1 << hashtable_spilling_state.ht.config.max_radix_bits;

        if hashtable_spilling_state.writer.is_none() {
            let location = self.spiller.create_unique_location();
            hashtable_spilling_state.writer =
                Some(self.spiller.create_aggregate_writer(location).await?);
        }

        let writer = hashtable_spilling_state.writer.as_mut().unwrap();

        if !hashtable_spilling_state.flush_data.is_empty() {
            let mut flush_data = Vec::with_capacity(4 * 1024 * 1024);
            std::mem::swap(&mut flush_data, &mut hashtable_spilling_state.flush_data);
            writer.write(flush_data).await?;
            hashtable_spilling_state.flush_data.clear();
        }

        if hashtable_spilling_state.last_prepare_payload {
            if writer.write_bytes() > hashtable_spilling_state.last_flush_partition_offset {
                // TODO:
                self.spill_blocks.push(DataBlock::empty_with_meta(
                    AggregateMeta::create_bucket_spilled(BucketSpilledPayload {
                        bucket: hashtable_spilling_state.work_partition as isize,
                        location: "".to_string(),
                        data_range: Default::default(),
                        columns_layout: vec![],
                        max_partition_count: 0,
                    }),
                ));

                hashtable_spilling_state.last_flush_partition_offset = writer.write_bytes();
            }

            hashtable_spilling_state.work_partition += 1;

            if hashtable_spilling_state.work_partition < max_partition {
                return Ok(true);
            }

            writer.complete().await?;
            let location = self.spiller.create_unique_location();
            hashtable_spilling_state.writer =
                Some(self.spiller.create_aggregate_writer(location).await?);

            hashtable_spilling_state.payload_idx = 0;
            hashtable_spilling_state.work_partition = 0;
            hashtable_spilling_state.scatter_work_bucket += 1;

            if hashtable_spilling_state.scatter_work_bucket
                < hashtable_spilling_state.scatter_max_bucket
            {
                return Ok(true);
            }

            hashtable_spilling_state.finished = true;
            hashtable_spilling_state
                .ht
                .config
                .update_current_max_radix_bits();

            let config = hashtable_spilling_state
                .ht
                .config
                .clone()
                .with_initial_radix_bits(hashtable_spilling_state.ht.config.max_radix_bits);

            let aggrs = hashtable_spilling_state.ht.payload.aggrs.clone();
            let group_types = hashtable_spilling_state.ht.payload.group_types.clone();
            self.spilling_state = None;
            self.hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
                group_types,
                aggrs,
                config,
                Arc::new(Bump::new()),
            ));

            return Ok(false);
        }

        Ok(true)
    }
}

pub struct HashtableSpillingState {
    ht: AggregateHashTable,
    payload_idx: usize,
    work_partition: usize,
    partition_state: PayloadFlushState,

    scatter_max_bucket: usize,
    scatter_work_bucket: usize,
    scatter_state: PayloadFlushState,

    serialize_state: PayloadFlushState,

    flush_data: Vec<u8>,
    writer: Option<SpillWriter>,
    finished: bool,
    last_prepare_payload: bool,

    last_flush_partition_offset: usize,
}

impl HashtableSpillingState {
    pub fn create(ht: AggregateHashTable, scatter_max_bucket: usize) -> Self {
        HashtableSpillingState {
            ht,
            payload_idx: 0,
            work_partition: 0,
            partition_state: PayloadFlushState::default(),
            scatter_max_bucket,
            scatter_work_bucket: 0,
            scatter_state: PayloadFlushState::default(),
            serialize_state: PayloadFlushState::default(),
            flush_data: Vec::with_capacity(6 * 1024 * 1024),
            writer: None,
            finished: false,
            last_prepare_payload: false,
            last_flush_partition_offset: 0,
        }
    }
    pub fn serialize_payload(&mut self, payload: Option<Payload>) -> Result<bool> {
        let payload = match payload.as_ref() {
            Some(payload) => payload,
            None => &self.ht.payload.payloads[self.work_partition],
        };

        if payload.len() == 0 {
            return Ok(true);
        }

        if let Some(data_block) = payload.aggregate_flush(&mut self.serialize_state)? {
            if data_block.num_rows() == 0 {
                return Ok(true);
            }

            let columns = data_block.columns().to_vec();
            for column in columns.into_iter() {
                let column = column.to_column(data_block.num_rows());

                let offset = self.flush_data.len();
                self.flush_data
                    .write_u64::<BigEndian>(0)
                    .map_err(|_| ErrorCode::Internal("Cannot serialize column"))?;

                write_column(&column, &mut self.flush_data)
                    .map_err(|_| ErrorCode::Internal("Cannot serialize column"))?;

                let len = self.flush_data.len();
                let mut buffer = &mut self.flush_data[offset..];
                buffer
                    .write_u64::<BigEndian>((len - offset) as u64)
                    .map_err(|_| ErrorCode::Internal("Cannot serialize column"))?;
            }
        }

        Ok(self.flush_data.len() < 4 * 1024 * 1024)
    }

    pub fn serialize_scatter_payload(&mut self, raw_payload: Option<Payload>) -> Result<bool> {
        if self.scatter_max_bucket <= 1 {
            return self.serialize_payload(raw_payload);
        }

        // using if-else to avoid mutable borrow occurs here
        if let Some(payload) = raw_payload {
            while payload.scatter(&mut self.scatter_state, self.scatter_max_bucket) {
                let idx = self.scatter_work_bucket;
                let rows = self.scatter_state.probe_state.partition_count[idx];

                if rows == 0 {
                    continue;
                }

                let sel = &self.scatter_state.probe_state.partition_entries[idx];

                let mut scattered_payload = Payload::new(
                    payload.arena.clone(),
                    payload.group_types.clone(),
                    payload.aggrs.clone(),
                    payload.states_layout.clone(),
                );

                scattered_payload.copy_rows(sel, rows, &self.scatter_state.addresses);

                if !self.serialize_payload(Some(scattered_payload))? {
                    return Ok(false);
                }
            }
        } else {
            while self.ht.payload.payloads[self.work_partition]
                .scatter(&mut self.scatter_state, self.scatter_max_bucket)
            {
                let idx = self.scatter_work_bucket;
                let rows = self.scatter_state.probe_state.partition_count[idx];

                if rows == 0 {
                    continue;
                }

                let sel = &self.scatter_state.probe_state.partition_entries[idx];

                let mut scattered_payload = Payload::new(
                    self.ht.payload.payloads[self.work_partition].arena.clone(),
                    self.ht.payload.payloads[self.work_partition]
                        .group_types
                        .clone(),
                    self.ht.payload.payloads[self.work_partition].aggrs.clone(),
                    self.ht.payload.payloads[self.work_partition]
                        .states_layout
                        .clone(),
                );

                scattered_payload.copy_rows(sel, rows, &self.scatter_state.addresses);

                if !self.serialize_payload(Some(scattered_payload))? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    pub fn serialize_partition_payload(&mut self) -> Result<bool> {
        let max_partitions = 1 << self.ht.config.max_radix_bits;
        if self.ht.payload.partition_count() == max_partitions {
            return self.serialize_scatter_payload(None);
        }

        let partition_payload = PartitionedPayload::new(
            self.ht.payload.group_types.clone(),
            self.ht.payload.aggrs.clone(),
            max_partitions as u64,
            self.ht.payload.arenas.clone(),
        );

        for idx in self.payload_idx..self.ht.payload.payloads.len() {
            while partition_payload
                .gather_flush(&self.ht.payload.payloads[idx], &mut self.partition_state)
            {
                let rows = self.partition_state.probe_state.partition_count[self.work_partition];

                if rows == 0 {
                    continue;
                }

                let selector =
                    &self.partition_state.probe_state.partition_entries[self.work_partition];
                let addresses = &self.partition_state.addresses;

                let mut new_payload = Payload::new(
                    self.ht.payload.payloads[idx].arena.clone(),
                    self.ht.payload.payloads[idx].group_types.clone(),
                    self.ht.payload.payloads[idx].aggrs.clone(),
                    self.ht.payload.payloads[idx].states_layout.clone(),
                );

                new_payload.copy_rows(selector, rows, addresses);

                if !self.serialize_scatter_payload(Some(new_payload))? {
                    return Ok(false);
                }
            }

            self.payload_idx += 1;
        }

        Ok(true)
    }
}
