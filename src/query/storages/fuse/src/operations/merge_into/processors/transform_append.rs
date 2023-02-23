//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;

use crate::io;
use crate::io::BlockBuilder;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::metrics::metrics_inc_block_index_write_bytes;
use crate::metrics::metrics_inc_block_index_write_milliseconds;
use crate::metrics::metrics_inc_block_index_write_nums;
use crate::metrics::metrics_inc_block_write_bytes;
use crate::metrics::metrics_inc_block_write_milliseconds;
use crate::metrics::metrics_inc_block_write_nums;
use crate::operations::merge_into::mutation_meta::mutation_meta::AppendOperationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_meta::MutationLogs;
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
        write_settings: WriteSettings,
        data_accessor: Operator,
        meta_locations: TableMetaLocationGenerator,
        thresholds: BlockThresholds,
        block_builder: BlockBuilder,
    ) -> Result<AppendTransform> {
        Ok(AppendTransform {
            data_accessor,
            meta_locations,
            accumulator: StatisticsAccumulator::new(thresholds),
            write_settings,
            block_builder,
        })
    }

    pub async fn try_output_mutation(&mut self) -> Result<Option<AppendOperationLogEntry>> {
        if self.accumulator.summary_block_count >= self.write_settings.block_per_seg as u64 {
            self.output_mutation().await
        } else {
            Ok(None)
        }
    }

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

        let data = serde_json::to_vec(&segment_info)?;
        let location = self.meta_locations.gen_segment_info_location();
        let segment = Arc::new(segment_info);

        // write down segments (TODO use meta writer, cache & retry)
        self.data_accessor.object(&location).write(data).await?;

        if let Some(segment_cache) = SegmentInfo::cache() {
            segment_cache.put(location.clone(), segment.clone());
        }

        // emit log entry
        let log_entry = AppendOperationLogEntry::new(location, segment);
        Ok(Some(log_entry))
    }

    pub fn output_mutation_block(
        &self,
        log: Option<AppendOperationLogEntry>,
    ) -> Result<Option<DataBlock>> {
        if let Some(log_entry) = log {
            let mut mutation_logs = MutationLogs::new();
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

    async fn transform(&mut self, data_block: DataBlock) -> Result<Option<DataBlock>> {
        // 1. serialize block and index
        let block_builder = self.block_builder.clone();
        let serialized_block_state =
            tokio_rayon::spawn(move || block_builder.build(data_block)).await?;

        let start = Instant::now();

        // persistent data block
        let raw_block_data = &serialized_block_state.block_raw_data;
        let path = serialized_block_state.block_meta.location.0.as_str();
        io::write_data(&raw_block_data, &self.data_accessor, path).await?;

        // metrics
        {
            metrics_inc_block_write_nums(1);
            metrics_inc_block_write_bytes(raw_block_data.len() as u64);
            metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);
        }

        let start = Instant::now();

        let bloom_index_state = &serialized_block_state.bloom_index_state;

        // 2. persistent bloom filter index
        if let Some(bloom_index_state) = bloom_index_state {
            io::write_data(
                &bloom_index_state.data,
                &self.data_accessor,
                &bloom_index_state.location.0,
            )
            .await?;

            // metrics
            {
                metrics_inc_block_index_write_nums(1);
                metrics_inc_block_index_write_bytes(bloom_index_state.data.len() as u64);
                metrics_inc_block_index_write_milliseconds(start.elapsed().as_millis() as u64);
            }
        }

        self.accumulator
            .add_with_block_meta(serialized_block_state.block_meta);

        // 3. output operation log if any
        let append_log = self.try_output_mutation().await?;
        self.output_mutation_block(append_log)
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        // output final operation log if any
        let append_log = self.output_mutation().await?;
        self.output_mutation_block(append_log)
    }
}
