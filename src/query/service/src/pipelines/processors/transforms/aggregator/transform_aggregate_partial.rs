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
use std::vec;

use bumpalo::Bump;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartialAddStats;
use databend_common_expression::PartialAggregateMode;
use databend_common_expression::PartialDecision;
use databend_common_expression::PartialEnvironment;
use databend_common_expression::PartialState;
use databend_common_expression::PartitionMask;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::group_hash_entries;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::pipelines::processors::transforms::aggregator::RawPayload;
use crate::pipelines::processors::transforms::aggregator::SharedPartitionStream;
use crate::pipelines::processors::transforms::aggregator::statistics::AggregationStatistics;
use crate::sessions::QueryContext;

#[allow(clippy::enum_variant_names)]
pub(crate) enum HashTable {
    MovedOut,
    AggregateHashTable(AggregateHashTable),
}

impl Default for HashTable {
    fn default() -> Self {
        Self::MovedOut
    }
}

pub struct Spiller {
    inner: AggregateSpiller<SharedPartitionStream>,
    bucket_num: usize,
}

impl Spiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        partition_streams: SharedPartitionStream,
        bucket_num: usize,
    ) -> Result<Self> {
        let spiller =
            AggregateSpiller::try_create(ctx.clone(), bucket_num, schema, partition_streams)?;
        Ok(Self {
            inner: spiller,
            bucket_num,
        })
    }

    pub fn spill(&mut self, partition: PartitionedPayload, is_row_shuffle: bool) -> Result<()> {
        if is_row_shuffle {
            for (bucket, p) in partition
                .scatter_into_buckets(self.bucket_num)
                .into_iter()
                .enumerate()
            {
                for (_, payload) in p.into_non_empty_bucket_payloads() {
                    check_interrupt()?;
                    let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                    self.inner.spill(bucket, data_block)?;
                }
            }
        } else {
            for (bucket, payload) in partition.into_non_empty_bucket_payloads() {
                check_interrupt()?;
                let data_block = payload.aggregate_flush_all()?.consume_convert_to_full();
                self.inner.spill(bucket, data_block)?;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<DataBlock>> {
        let payloads = self.inner.spill_finish()?;

        if payloads.is_empty() {
            return Ok(vec![]);
        }
        let partitioned_payload = DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
            None,
            PartitionedData::BucketSpilled(payloads),
        ));
        return Ok(vec![partitioned_payload]);
    }
}

/// Parse the `partial_aggregate_mode` setting. `None` means the legacy strategy.
pub fn parse_partial_aggregate_mode(mode: &str) -> Result<Option<PartialAggregateMode>> {
    Ok(match mode.to_lowercase().as_str() {
        "legacy" => None,
        "auto" => Some(PartialAggregateMode::Auto),
        "bypass" => Some(PartialAggregateMode::Bypass),
        other => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid partial_aggregate_mode: {other}"
            )));
        }
    })
}

enum PartialStrategy {
    /// Fixed cache-sized table that is cleared when full.
    Legacy,
    /// Runtime-adaptive table, see `PartialState`.
    Adaptive(Box<AdaptiveState>),
}

struct AdaptiveState {
    state: PartialState,
    /// Routes raw rows to the same buckets as the payload of the table.
    partition_mask: PartitionMask,
    partition_count: usize,
    /// The index restarted since the table was last emitted: the payload is no longer bounded
    /// by the index, so emit it once it holds `flush_bytes`.
    windowed: bool,
    flush_bytes: usize,
    concurrency: usize,
    /// Bytes per group of the table, updated once the table holds enough groups that page
    /// preallocation does not dominate.
    bytes_per_group: usize,
    /// Buffers the raw rows of each bucket in bypass, so they leave in blocks of the hash
    /// shuffle thresholds rather than as a slice of every input block per bucket.
    raw_stream: BlockPartitionStream,
    output_rows: usize,
    /// Stats of all calls to the table, and times the index grew.
    total: PartialAddStats,
    grows: usize,
}

/// TransformPartialAggregate combine partial aggregation and spilling logic
/// When memory exceeds threshold, it will spill out current hash table into a buffer
/// and real spill out will happen when the buffer is full.
///
/// In adaptive mode, the table is emitted downstream whenever `PartialState` asks for it, and
/// rows that are not aggregated are forwarded as raw rows for the final aggregate to aggregate.
pub struct TransformPartialAggregate {
    hash_table: HashTable,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
    statistics: AggregationStatistics,
    settings: MemorySettings,
    spillers: Spiller,
    is_row_shuffle: bool,
    strategy: PartialStrategy,
}

impl TransformPartialAggregate {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
        partition_stream: SharedPartitionStream,
        bucket_num: usize,
        is_row_shuffle: bool,
        partial_mode: Option<PartialAggregateMode>,
    ) -> Result<Box<dyn Processor>> {
        let strategy = match partial_mode {
            None => PartialStrategy::Legacy,
            Some(mode) => {
                debug_assert!(config.partial_adaptive);
                let settings = ctx.get_settings();
                let concurrency = settings.get_max_threads()?.max(1) as usize;
                let partition_count = 1_usize << config.initial_radix_bits;
                PartialStrategy::Adaptive(Box::new(AdaptiveState {
                    state: PartialState::new(mode),
                    partition_mask: PartitionMask::with_start_bit(
                        partition_count as u64,
                        config.partition_start_bit,
                    ),
                    partition_count,
                    windowed: false,
                    flush_bytes: params.max_block_bytes,
                    concurrency,
                    bytes_per_group: 64,
                    raw_stream: BlockPartitionStream::create(
                        settings.get_hash_shuffle_rows_threshold()?,
                        settings.get_hash_shuffle_bytes_threshold()?,
                        partition_count,
                    ),
                    output_rows: 0,
                    total: PartialAddStats::default(),
                    grows: 0,
                }))
            }
        };

        let spill_schema = params.spill_schema();
        let spillers = Spiller::create(ctx.clone(), spill_schema, partition_stream, bucket_num)?;

        let arena = Arc::new(Bump::new());

        let hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            config,
            arena,
        ));

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate {
                params,
                hash_table,
                probe_state: ProbeState::default(),
                settings: MemorySettings::from_aggregate_settings(&ctx)?,
                statistics: AggregationStatistics::new("PartialAggregate"),
                spillers,
                is_row_shuffle,
                strategy,
            },
        ))
    }

    #[inline(always)]
    fn aggregate_arguments<'a>(
        block: &'a DataBlock,
        aggregate_functions_arguments: &'a [Vec<usize>],
    ) -> Vec<ProjectedBlock<'a>> {
        aggregate_functions_arguments
            .iter()
            .map(|function_arguments| ProjectedBlock::project(function_arguments, block))
            .collect::<Vec<_>>()
    }

    #[inline(always)]
    fn execute_one_block(&mut self, block: DataBlock) -> Result<()> {
        let group_columns = ProjectedBlock::project(&self.params.group_columns, &block);
        let rows_num = block.num_rows();
        let block_bytes = block.memory_size();

        self.statistics.record_block(rows_num, block_bytes);

        {
            match &mut self.hash_table {
                HashTable::MovedOut => {
                    unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out")
                }
                HashTable::AggregateHashTable(hashtable) => {
                    let params_columns = Self::aggregate_arguments(
                        &block,
                        &self.params.aggregate_functions_arguments,
                    );
                    let agg_states = (&[]).into();

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

    fn transform_adaptive(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let rows_num = block.num_rows();
        self.statistics.record_block(rows_num, block.memory_size());

        let HashTable::AggregateHashTable(hashtable) = &mut self.hash_table else {
            unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out")
        };
        let PartialStrategy::Adaptive(adaptive) = &mut self.strategy else {
            unreachable!("[TRANSFORM-AGGREGATOR] Adaptive partial aggregate expected")
        };
        let group_columns = ProjectedBlock::project(&self.params.group_columns, &block);

        if matches!(adaptive.state, PartialState::Bypass) {
            adaptive.output_rows += rows_num;
            let mut buckets = vec![0; rows_num];
            if adaptive.partition_count > 1 {
                group_hash_entries(group_columns, &mut buckets);
                let mask = adaptive.partition_mask;
                for bucket in buckets.iter_mut() {
                    *bucket = mask.index(*bucket) as u64;
                }
            }
            let ready = adaptive.raw_stream.partition(buckets, block, true);
            return Ok(Self::raw_block(ready).into_iter().collect());
        }

        let params_columns =
            Self::aggregate_arguments(&block, &self.params.aggregate_functions_arguments);
        let capacity = adaptive.state.capacity();
        let stats = hashtable.add_groups_partial(
            &mut self.probe_state,
            group_columns,
            &params_columns,
            rows_num,
            capacity,
            adaptive.state.sampler(),
        )?;
        adaptive.windowed |= stats.windows > 0;
        adaptive.total += stats;

        // An index entry costs a control byte and a row pointer at the index load factor.
        const INDEX_BYTES_PER_GROUP: usize = 13;
        if hashtable.len() >= 16 * 1024 {
            adaptive.bytes_per_group =
                hashtable.partial_hot_bytes() / hashtable.len() + INDEX_BYTES_PER_GROUP;
        }
        let env = PartialEnvironment {
            headroom_bytes: self
                .settings
                .check_spill_remain()
                .map(|remain| remain.max(0) as usize),
            bytes_per_group: adaptive.bytes_per_group,
            concurrency: adaptive.concurrency,
        };
        let decision = adaptive.state.observe(stats, &env);
        adaptive.grows += matches!(decision, PartialDecision::Grow { .. }) as usize;

        let mut blocks = vec![];
        if self.settings.check_spill() {
            // Memory pressure: spill the table like any other operator, then learn again.
            let hot = hashtable.take_partial_hot();
            adaptive.windowed = false;
            self.spillers.spill(hot, self.is_row_shuffle)?;
            adaptive.state.on_hot_spilled();
        } else if decision != PartialDecision::Continue
            || (adaptive.windowed && hashtable.partial_hot_bytes() >= adaptive.flush_bytes)
        {
            let hot = hashtable.take_partial_hot();
            adaptive.windowed = false;
            adaptive.output_rows += hot.len();
            blocks.extend(Self::partitioned_blocks(hot, &self.params));
        }
        Ok(blocks)
    }

    /// Wrap the non-empty buckets of raw rows into a block for the final aggregate.
    fn raw_block(buckets: Vec<(usize, DataBlock)>) -> Option<DataBlock> {
        let payloads = buckets
            .into_iter()
            .filter(|(_, block)| !block.is_empty())
            .map(|(bucket, data_block)| RawPayload {
                bucket: bucket as isize,
                data_block,
            })
            .collect::<Vec<_>>();
        (!payloads.is_empty()).then(|| {
            DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
                None,
                PartitionedData::Raw(payloads),
            ))
        })
    }

    /// Wrap the non-empty buckets of `payload` into blocks for the final aggregate, each of at
    /// most about `max_block_rows` rows and `max_block_bytes` bytes. Buckets are split at page
    /// boundaries without copying rows, so the final aggregate checks memory and the exchange
    /// sends messages at block granularity whatever the size of the table.
    fn partitioned_blocks(
        payload: PartitionedPayload,
        params: &AggregatorParams,
    ) -> Vec<DataBlock> {
        let (max_rows, max_bytes) = (params.max_block_rows, params.max_block_bytes);
        let mut blocks = vec![];
        let mut payloads = vec![];
        let (mut rows, mut bytes) = (0, 0);
        for (bucket, payload) in payload.into_non_empty_bucket_payloads() {
            for payload in payload.split_pages(max_rows, max_bytes) {
                if !payloads.is_empty()
                    && (rows + payload.len() > max_rows
                        || bytes + payload.memory_size() > max_bytes)
                {
                    blocks.push(Self::payload_block(std::mem::take(&mut payloads)));
                    (rows, bytes) = (0, 0);
                }
                rows += payload.len();
                bytes += payload.memory_size();
                payloads.push(AggregatePayload {
                    bucket: bucket as isize,
                    payload,
                    max_partition_count: 0,
                });
            }
        }
        if !payloads.is_empty() {
            blocks.push(Self::payload_block(payloads));
        }
        blocks
    }

    fn payload_block(payloads: Vec<AggregatePayload>) -> DataBlock {
        DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
            None,
            PartitionedData::AggregatePayload(payloads),
        ))
    }

    fn spill_out(&mut self) -> Result<()> {
        if let HashTable::AggregateHashTable(v) = std::mem::take(&mut self.hash_table) {
            let config = v.config.clone();

            self.spillers.spill(v.payload, self.is_row_shuffle)?;

            let arena = Arc::new(Bump::new());
            self.hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
                self.params.group_data_types.clone(),
                self.params.aggregate_functions.clone(),
                config,
                arena,
            ));
        } else {
            unreachable!("[TRANSFORM-AGGREGATOR] Invalid hash table state during spill check")
        }
        Ok(())
    }
}

impl AccumulatingTransform for TransformPartialAggregate {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if matches!(self.strategy, PartialStrategy::Adaptive(_)) {
            return self.transform_adaptive(block);
        }

        self.execute_one_block(block)?;

        if self.settings.check_spill() {
            self.spill_out()?;
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output {
                true => vec![],
                false => {
                    unreachable!("[TRANSFORM-AGGREGATOR] Hash table already moved out in finish")
                }
            },
            HashTable::AggregateHashTable(hashtable) => {
                let mut blocks = self.spillers.finish()?;

                if let PartialStrategy::Adaptive(adaptive) = &mut self.strategy {
                    let stream = &mut adaptive.raw_stream;
                    let buckets = stream
                        .partition_ids()
                        .into_iter()
                        .filter_map(|id| stream.finalize_partition(id).map(|block| (id, block)))
                        .collect();
                    stream.reset();
                    blocks.extend(Self::raw_block(buckets));
                }

                match &self.strategy {
                    PartialStrategy::Legacy => self.statistics.log_finish_statistics(&hashtable),
                    PartialStrategy::Adaptive(adaptive) => {
                        self.statistics.log_partial_finish_statistics(
                            adaptive.output_rows + hashtable.len(),
                            hashtable.hash_index_resize_count(),
                        );
                        let total = &adaptive.total;
                        log::info!(
                            "[PartialAggregate] Adaptive: final_state={:?}, rows={}, new_groups={}, windows={}, grows={}",
                            adaptive.state,
                            total.rows,
                            total.new_groups,
                            total.windows,
                            adaptive.grows,
                        );
                    }
                }

                blocks.extend(Self::partitioned_blocks(hashtable.payload, &self.params));
                blocks
            }
        })
    }
}
