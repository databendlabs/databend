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
use databend_common_config::GlobalConfig;
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
use crate::pipelines::processors::transforms::aggregator::SpilledPayload;
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
    output_blocks: Vec<DataBlock>,
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
                configure_peer_nodes: vec![GlobalConfig::instance().query.node_id.clone()],
                spilling_state: None,
                spiller: Arc::new(spiller),
                output_blocks: vec![],
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

    fn reset_hashtable(&mut self) {
        let hashtable_spilling_state = self.spilling_state.as_mut().unwrap();

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
    }
}

#[async_trait::async_trait]
impl AccumulatingTransform for TransformPartialAggregate {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.execute_one_block(block)?;
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

                for (partition, payload) in hashtable.payload.payloads.into_iter().enumerate() {
                    self.output_blocks.push(DataBlock::empty_with_meta(
                        AggregateMeta::create_agg_payload(
                            payload,
                            partition as isize,
                            partition_count,
                            partition_count,
                        ),
                    ));
                }

                std::mem::take(&mut self.output_blocks)
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
            let HashTable::AggregateHashTable(ht) = std::mem::take(&mut self.hash_table) else {
                return Ok(false);
            };

            if ht.len() == 0 {
                self.hash_table = HashTable::AggregateHashTable(ht);
                return Ok(false);
            }

            let max_bucket = self.configure_peer_nodes.len();
            self.spilling_state = Some(HashtableSpillingState::create(ht, max_bucket));
        }

        if let Some(spilling_state) = self.spilling_state.as_mut() {
            spilling_state.last_prepare_payload = spilling_state.serialize_partition_payload()?;
            return Ok(true);
        }

        Ok(false)
    }

    async fn flush_spill_payload(&mut self) -> Result<bool> {
        let spilling_state = self.spilling_state.as_mut().unwrap();

        let max_bucket = spilling_state.max_bucket;
        let max_partition = 1 << spilling_state.ht.config.max_radix_bits;

        if !spilling_state.data_payload.is_empty() {
            if spilling_state.writer.is_none() {
                let location = self.spiller.create_unique_location();
                spilling_state.writer = Some(self.spiller.create_aggregate_writer(location).await?);
            }

            let writer = spilling_state.writer.as_mut().unwrap();

            let mut flush_data = Vec::with_capacity(4 * 1024 * 1024);
            std::mem::swap(&mut flush_data, &mut spilling_state.data_payload);
            writer.write(flush_data).await?;
        }

        if spilling_state.last_prepare_payload {
            if let Some(writer) = spilling_state.writer.as_mut() {
                let last_offset = spilling_state.last_flush_partition_offset;
                if writer.write_bytes() > last_offset {
                    let spilled_payload = SpilledPayload {
                        partition: spilling_state.working_partition as isize,
                        location: writer.location(),
                        data_range: last_offset as u64..writer.write_bytes() as u64,
                        destination_node: self.configure_peer_nodes[spilling_state.working_bucket]
                            .clone(),
                        max_partition,
                        global_max_partition: max_partition,
                    };

                    self.output_blocks.push(DataBlock::empty_with_meta(
                        AggregateMeta::create_spilled_payload(spilled_payload),
                    ));

                    spilling_state.last_flush_partition_offset = writer.write_bytes();
                }
            }

            spilling_state.payload_idx = 0;
            spilling_state.working_partition += 1;
            if spilling_state.working_partition < max_partition {
                return Ok(true);
            }

            if let Some(writer) = spilling_state.writer.as_mut() {
                writer.complete().await?;
                spilling_state.writer = None;
                spilling_state.last_flush_partition_offset = 0;
            }

            spilling_state.payload_idx = 0;
            spilling_state.working_bucket += 1;
            spilling_state.working_partition = 0;

            if spilling_state.working_bucket < max_bucket {
                return Ok(true);
            }

            spilling_state.finished = true;
            self.reset_hashtable();

            return Ok(false);
        }

        Ok(true)
    }
}

pub struct HashtableSpillingState {
    ht: AggregateHashTable,
    payload_idx: usize,
    working_partition: usize,
    partition_flush_state: PayloadFlushState,

    max_bucket: usize,
    working_bucket: usize,
    bucket_flush_state: PayloadFlushState,

    serialize_flush_state: PayloadFlushState,

    data_payload: Vec<u8>,

    finished: bool,
    last_prepare_payload: bool,
    writer: Option<SpillWriter>,

    last_flush_partition_offset: usize,
}

impl HashtableSpillingState {
    pub fn create(ht: AggregateHashTable, scatter_max_bucket: usize) -> Self {
        HashtableSpillingState {
            ht,
            payload_idx: 0,
            working_partition: 0,
            partition_flush_state: PayloadFlushState::default(),
            max_bucket: scatter_max_bucket,
            working_bucket: 0,
            bucket_flush_state: PayloadFlushState::default(),
            serialize_flush_state: PayloadFlushState::default(),
            data_payload: Vec::with_capacity(6 * 1024 * 1024),
            writer: None,
            finished: false,
            last_prepare_payload: false,
            last_flush_partition_offset: 0,
        }
    }
    pub fn serialize_payload(&mut self, payload: Option<Payload>) -> Result<bool> {
        let payload = match payload.as_ref() {
            Some(payload) => payload,
            None => &self.ht.payload.payloads[self.working_partition],
        };

        if payload.len() == 0 {
            return Ok(true);
        }

        while let Some(data_block) = payload.aggregate_flush(&mut self.serialize_flush_state)? {
            if data_block.num_rows() == 0 {
                // next batch rows
                continue;
            }

            let columns = data_block.columns().to_vec();
            for column in columns.into_iter() {
                let column = column.into_column(data_block.num_rows());

                let offset = self.data_payload.len();

                self.data_payload.write_u64::<BigEndian>(0)?;
                write_column(&column, &mut self.data_payload)?;

                // rewrite column length
                let len = self.data_payload.len();
                let mut buffer = &mut self.data_payload[offset..];
                buffer.write_u64::<BigEndian>((len - offset - size_of::<u64>()) as u64)?;
            }

            if self.data_payload.len() >= 4 * 1024 * 1024 {
                // flush data if >= 4MB
                return Ok(false);
            }
        }

        self.serialize_flush_state.clear();
        Ok(true)
    }

    pub fn serialize_scatter_payload(&mut self, raw_payload: Option<Payload>) -> Result<bool> {
        // If no need scatter
        if self.max_bucket <= 1 {
            return self.serialize_payload(raw_payload);
        }

        // using if-else to avoid mutable borrow occurs here
        if let Some(payload) = raw_payload {
            while payload.scatter(&mut self.bucket_flush_state, self.max_bucket) {
                let working_bucket = self.working_bucket;
                let flush_state = &mut self.bucket_flush_state;

                let rows = flush_state.probe_state.partition_count[working_bucket];

                if rows == 0 {
                    // next batch rows
                    continue;
                }

                let sel = &flush_state.probe_state.partition_entries[working_bucket];

                let mut scattered_payload = Payload::new(
                    payload.arena.clone(),
                    payload.group_types.clone(),
                    payload.aggrs.clone(),
                    payload.states_layout.clone(),
                );

                scattered_payload.state_move_out = true;
                scattered_payload.copy_rows(sel, rows, &flush_state.addresses);

                if !self.serialize_payload(Some(scattered_payload))? {
                    return Ok(false);
                }
            }
        } else {
            while self.ht.payload.payloads[self.working_partition]
                .scatter(&mut self.bucket_flush_state, self.max_bucket)
            {
                let working_bucket = self.working_bucket;
                let flush_state = &mut self.bucket_flush_state;
                let rows = flush_state.probe_state.partition_count[working_bucket];

                if rows == 0 {
                    // next batch rows
                    continue;
                }

                let sel = &flush_state.probe_state.partition_entries[working_bucket];

                let working_payload = &self.ht.payload.payloads[self.working_partition];
                let mut scattered_payload = Payload::new(
                    working_payload.arena.clone(),
                    working_payload.group_types.clone(),
                    working_payload.aggrs.clone(),
                    working_payload.states_layout.clone(),
                );

                scattered_payload.state_move_out = true;
                scattered_payload.copy_rows(sel, rows, &flush_state.addresses);

                if !self.serialize_payload(Some(scattered_payload))? {
                    return Ok(false);
                }
            }
        }

        self.bucket_flush_state.clear();
        Ok(true)
    }

    pub fn serialize_partition_payload(&mut self) -> Result<bool> {
        let max_partitions = 1 << self.ht.config.max_radix_bits;

        // If no need repartition
        if self.ht.payload.partition_count() == max_partitions {
            return self.serialize_scatter_payload(None);
        }

        let mut partition_payload = PartitionedPayload::new(
            self.ht.payload.group_types.clone(),
            self.ht.payload.aggrs.clone(),
            max_partitions as u64,
            self.ht.payload.arenas.clone(),
        );

        for payload in &mut partition_payload.payloads {
            payload.state_move_out = true;
        }

        // repartition and get current partition payload
        for idx in self.payload_idx..self.ht.payload.payloads.len() {
            while partition_payload.gather_flush(
                &self.ht.payload.payloads[idx],
                &mut self.partition_flush_state,
            ) {
                let working_partition = self.working_partition;
                let flush_state = &mut self.partition_flush_state;

                let rows = flush_state.probe_state.partition_count[working_partition];

                if rows == 0 {
                    // next batch rows
                    continue;
                }

                let address = &flush_state.addresses;
                let selector = &flush_state.probe_state.partition_entries[working_partition];

                let working_payload = &self.ht.payload.payloads[idx];
                let mut working_partition_payload = Payload::new(
                    working_payload.arena.clone(),
                    working_payload.group_types.clone(),
                    working_payload.aggrs.clone(),
                    working_payload.states_layout.clone(),
                );

                working_partition_payload.state_move_out = true;
                working_partition_payload.copy_rows(selector, rows, address);

                if !self.serialize_scatter_payload(Some(working_partition_payload))? {
                    return Ok(false);
                }
            }

            self.payload_idx += 1;
            self.partition_flush_state.clear();
        }

        self.partition_flush_state.clear();
        Ok(true)
    }
}
