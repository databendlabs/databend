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

use async_trait::async_trait;
use common_base::base::ProgressValues;
use common_base::runtime::GlobalIORuntime;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchema;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;

use crate::io;
use crate::io::BlockBuilder;
use crate::io::MetaWriter;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::metrics::metrics_inc_block_index_write_bytes;
use crate::metrics::metrics_inc_block_index_write_milliseconds;
use crate::metrics::metrics_inc_block_index_write_nums;
use crate::metrics::metrics_inc_block_write_bytes;
use crate::metrics::metrics_inc_block_write_milliseconds;
use crate::metrics::metrics_inc_block_write_nums;
use crate::operations::merge_into::mutation_meta::mutation_log::AppendOperationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLogs;
use crate::statistics::ClusterStatsGenerator;
use crate::statistics::StatisticsAccumulator;

// Write down data blocks, generate indexes, emits append log entries
pub struct AppendTransform {
    data_accessor: Operator,
    meta_locations: TableMetaLocationGenerator,
    accumulator: StatisticsAccumulator,
    write_settings: WriteSettings,
    block_builder: BlockBuilder,
}

impl AppendTransform {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        write_settings: WriteSettings,
        data_accessor: Operator,
        meta_locations: TableMetaLocationGenerator,
        source_schema: Arc<TableSchema>,
        thresholds: BlockThresholds,
        cluster_stats_gen: ClusterStatsGenerator,
    ) -> AppendTransform {
        let block_builder = BlockBuilder {
            ctx,
            meta_locations: meta_locations.clone(),
            source_schema,
            write_settings: write_settings.clone(),
            cluster_stats_gen,
        };
        AppendTransform {
            data_accessor,
            meta_locations,
            accumulator: StatisticsAccumulator::new(thresholds),
            write_settings,
            block_builder,
        }
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let transformer = AsyncAccumulatingTransformer::create(input.clone(), output.clone(), self);
        PipeItem {
            processor: ProcessorPtr::create(transformer),
            inputs_port: vec![input],
            outputs_port: vec![output],
        }
    }

    pub fn get_block_builder(&self) -> BlockBuilder {
        self.block_builder.clone()
    }

    #[async_backtrace::framed]
    pub async fn try_output_mutation(&mut self) -> Result<Option<AppendOperationLogEntry>> {
        if self.accumulator.summary_block_count >= self.write_settings.block_per_seg as u64 {
            self.output_mutation().await
        } else {
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    pub async fn output_mutation(&mut self) -> Result<Option<AppendOperationLogEntry>> {
        let acc = std::mem::take(&mut self.accumulator);

        if acc.summary_row_count == 0 {
            return Ok(None);
        }

        let col_stats = acc.summary()?;

        // build new segment
        let segment_info = SegmentInfo::new(acc.blocks_metas, Statistics {
            row_count: acc.summary_row_count,
            block_count: acc.summary_block_count,
            perfect_block_count: acc.perfect_block_count,
            uncompressed_byte_size: acc.in_memory_size,
            compressed_byte_size: acc.file_size,
            index_size: acc.index_size,
            col_stats,
        });

        let location = self.meta_locations.gen_segment_info_location();
        let segment = Arc::new(segment_info);
        segment
            .as_ref()
            .write_meta(&self.data_accessor, &location)
            .await?;

        if let Some(segment_cache) = SegmentInfo::cache() {
            segment_cache.put(location.clone(), Arc::new(segment.as_ref().try_into()?));
        }

        // emit log entry.
        // for newly created segment, always use the latest version
        let log_entry = AppendOperationLogEntry::new(location, segment, SegmentInfo::VERSION);
        Ok(Some(log_entry))
    }

    pub fn output_mutation_block(
        &self,
        log: Option<AppendOperationLogEntry>,
    ) -> Result<Option<DataBlock>> {
        if let Some(log_entry) = log {
            let mut mutation_logs = MutationLogs::default();
            mutation_logs.push_append(log_entry);
            let data_block = DataBlock::try_from(mutation_logs)?;
            Ok(Some(data_block))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl AsyncAccumulatingTransform for AppendTransform {
    const NAME: &'static str = "AppendTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<Option<DataBlock>> {
        if data_block.is_empty() {
            // data source like
            //  `select number from numbers(3000000) where number >=2000000 and number < 3000000`
            // may generate empty data blocks
            return Ok(None);
        }
        // 1. serialize block and index
        let block_builder = self.block_builder.clone();
        let serialized_block_state = GlobalIORuntime::instance()
            .spawn_blocking(move || {
                block_builder.build(data_block, |block, generator| {
                    generator.gen_stats_for_append(block)
                })
            })
            .await?;

        let progress_values = ProgressValues {
            rows: serialized_block_state.block_meta.row_count as usize,
            bytes: serialized_block_state.block_meta.block_size as usize,
        };
        self.block_builder
            .ctx
            .get_write_progress()
            .incr(&progress_values);

        let start = Instant::now();

        // persistent data block
        let raw_block_data = serialized_block_state.block_raw_data;
        let path = serialized_block_state.block_meta.location.0.as_str();
        let raw_block_data_len = raw_block_data.len();
        io::write_data(raw_block_data, &self.data_accessor, path).await?;

        // metrics
        {
            metrics_inc_block_write_nums(1);
            metrics_inc_block_write_bytes(raw_block_data_len as u64);
            metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);
        }

        let start = Instant::now();

        let bloom_index_state = serialized_block_state.bloom_index_state;

        // 2. persistent bloom filter index
        if let Some(bloom_index_state) = bloom_index_state {
            let bloom_index_state_data_len = bloom_index_state.data.len();
            io::write_data(
                bloom_index_state.data,
                &self.data_accessor,
                &bloom_index_state.location.0,
            )
            .await?;

            // metrics
            {
                metrics_inc_block_index_write_nums(1);
                metrics_inc_block_index_write_bytes(bloom_index_state_data_len as u64);
                metrics_inc_block_index_write_milliseconds(start.elapsed().as_millis() as u64);
            }
        }

        self.accumulator
            .add_with_block_meta(serialized_block_state.block_meta);

        // 3. output operation log if any
        let append_log = self.try_output_mutation().await?;
        self.output_mutation_block(append_log)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        // output final operation log if any
        let append_log = self.output_mutation().await?;
        self.output_mutation_block(append_log)
    }
}
