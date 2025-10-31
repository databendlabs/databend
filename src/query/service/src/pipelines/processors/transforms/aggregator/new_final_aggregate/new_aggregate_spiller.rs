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
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use opendal::layers::BlockingLayer;
use opendal::BlockingOperator;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::sessions::QueryContext;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataWriter;

struct AggregatePayloadWriter {
    path: String,
    writer: SpillsDataWriter,
}

impl AggregatePayloadWriter {
    fn create(prefix: &str) -> Result<Self> {
        let operator = DataOperator::instance().spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let file_path = format!("{}/{}", prefix, GlobalUniqName::unique());
        let spills_data_writer = buffer_pool.writer(operator, file_path.clone())?;

        Ok(AggregatePayloadWriter {
            path: file_path,
            writer: spills_data_writer,
        })
    }

    fn write_block(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        self.writer.write(block)?;
        self.writer.flush()
    }

    fn close(self) -> Result<(String, usize, Vec<RowGroupMetaData>)> {
        let (bytes_written, row_groups) = self.writer.close()?;
        Ok((self.path, bytes_written, row_groups))
    }
}

#[derive(Default)]
struct WriteStats {
    rows: usize,
    bytes: usize,
    count: usize,
    elapsed: Duration,
}

impl WriteStats {
    fn accumulate(&mut self, elapsed: Duration) {
        self.count += 1;
        self.elapsed += elapsed;
    }

    fn add_rows(&mut self, rows: usize) {
        self.rows += rows;
    }

    fn add_bytes(&mut self, bytes: usize) {
        self.bytes += bytes;
    }

    fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

pub struct NewAggregateSpiller {
    // legacy, will remove latter
    blocking_operator: BlockingOperator,

    pub memory_settings: MemorySettings,
    ctx: Arc<QueryContext>,
    read_setting: ReadSettings,
    spill_prefix: String,
    partition_count: usize,
    rows_threshold: usize,
    bytes_threshold: usize,
    partition_stream: BlockPartitionStream,
    payload_writers: Vec<AggregatePayloadWriter>,
    write_stats: WriteStats,
}

impl NewAggregateSpiller {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        partition_count: usize,
        rows_threshold: usize,
        bytes_threshold: usize,
    ) -> Result<Self> {
        let memory_settings = MemorySettings::from_aggregate_settings(&ctx)?;
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let read_setting = ReadSettings::from_ctx(&table_ctx)?;
        let spill_prefix = ctx.query_id_spill_prefix();
        let partition_stream =
            BlockPartitionStream::create(rows_threshold, bytes_threshold, partition_count);
        let payload_writers = Self::create_payload_writers(&spill_prefix, partition_count)?;

        let blocking_operator = DataOperator::instance()
            .spill_operator()
            .layer(BlockingLayer::create()?)
            .blocking();

        Ok(Self {
            blocking_operator,

            memory_settings,
            ctx,
            read_setting,
            spill_prefix,
            partition_count,
            rows_threshold,
            bytes_threshold,
            partition_stream,
            payload_writers,
            write_stats: WriteStats::default(),
        })
    }

    fn create_payload_writers(
        prefix: &str,
        partition_count: usize,
    ) -> Result<Vec<AggregatePayloadWriter>> {
        (0..partition_count)
            .map(|_| AggregatePayloadWriter::create(prefix))
            .collect()
    }

    fn recreate_resources(&mut self) -> Result<()> {
        self.partition_stream = BlockPartitionStream::create(
            self.rows_threshold,
            self.bytes_threshold,
            self.partition_count,
        );
        self.payload_writers =
            Self::create_payload_writers(&self.spill_prefix, self.partition_count)?;
        Ok(())
    }

    fn write_ready_blocks(&mut self, ready_blocks: Vec<(usize, DataBlock)>) -> Result<()> {
        for (bucket, block) in ready_blocks {
            if block.is_empty() {
                continue;
            }

            if bucket >= self.partition_count {
                return Err(ErrorCode::Internal(
                    "NewAggregateSpiller produced invalid partition id",
                ));
            }

            let start = Instant::now();
            self.payload_writers[bucket].write_block(block)?;
            let elapsed = start.elapsed();
            self.write_stats.accumulate(elapsed);
        }

        Ok(())
    }

    pub fn spill(&mut self, partition_id: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        if partition_id >= self.partition_count {
            return Err(ErrorCode::Internal(
                "NewAggregateSpiller received invalid partition id",
            ));
        }

        let indices = vec![partition_id as u64; block.num_rows()];
        let ready_blocks = self.partition_stream.partition(indices, block, true);
        self.write_ready_blocks(ready_blocks)
    }

    pub fn spill_finish(&mut self) -> Result<Vec<NewSpilledPayload>> {
        let mut pending_blocks = Vec::new();
        for partition_id in 0..self.partition_count {
            if let Some(block) = self.partition_stream.finalize_partition(partition_id) {
                pending_blocks.push((partition_id, block));
            }
        }
        self.write_ready_blocks(pending_blocks)?;

        let mut spilled_payloads = Vec::new();

        let writers = std::mem::take(&mut self.payload_writers);
        for (partition_id, writer) in writers.into_iter().enumerate() {
            let (path, written_size, row_groups) = writer.close()?;
            if row_groups.is_empty() {
                continue;
            }

            if written_size > 0 {
                self.write_stats.add_bytes(written_size);
            }
            for row_group in row_groups {
                self.write_stats.add_rows(row_group.num_rows() as usize);
                spilled_payloads.push(NewSpilledPayload {
                    bucket: partition_id as isize,
                    location: path.clone(),
                    row_group,
                });
            }
        }

        self.flush_write_profile();

        self.recreate_resources()?;

        Ok(spilled_payloads)
    }

    pub fn restore(&self, payload: NewSpilledPayload) -> Result<AggregateMeta> {
        let NewSpilledPayload {
            bucket,
            location,
            row_group,
        } = payload;

        let operator = DataOperator::instance().spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let mut reader = buffer_pool.reader(operator, location, vec![row_group.clone()])?;

        let read_bytes = row_group.total_byte_size() as usize;
        let instant = Instant::now();
        let data_block = reader.read(self.read_setting)?;
        self.flush_read_profile(&instant, read_bytes);

        if let Some(block) = data_block {
            Ok(AggregateMeta::Serialized(SerializedPayload {
                bucket,
                data_block: block,
                max_partition_count: self.partition_count,
            }))
        } else {
            Err(ErrorCode::Internal("read empty block from final aggregate"))
        }
    }

    // legacy, will remove latter
    pub fn restore_legacy(&self, payload: BucketSpilledPayload) -> Result<AggregateMeta> {
        // read
        let instant = Instant::now();
        let data = self
            .blocking_operator
            .read_with(&payload.location)
            .range(payload.data_range.clone())
            .call()?
            .to_vec();

        self.flush_read_profile(&instant, data.len());

        // deserialize
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());
        for &column_layout in &payload.columns_layout {
            columns.push(deserialize_column(
                &data[begin..begin + column_layout as usize],
            )?);
            begin += column_layout as usize;
        }

        Ok(AggregateMeta::Serialized(SerializedPayload {
            bucket: payload.bucket,
            data_block: DataBlock::new_from_columns(columns),
            max_partition_count: payload.max_partition_count,
        }))
    }

    fn flush_read_profile(&self, instant: &Instant, read_bytes: usize) {
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, read_bytes);
        Profile::record_usize_profile(
            ProfileStatisticsName::RemoteSpillReadTime,
            instant.elapsed().as_millis() as usize,
        );
    }

    fn flush_write_profile(&mut self) {
        let stats = self.write_stats.take();
        if stats.count == 0 && stats.bytes == 0 && stats.rows == 0 {
            return;
        }

        if stats.count > 0 {
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillWriteCount,
                stats.count,
            );
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillWriteTime,
                stats.elapsed.as_millis() as usize,
            );
        }
        if stats.bytes > 0 {
            Profile::record_usize_profile(
                ProfileStatisticsName::RemoteSpillWriteBytes,
                stats.bytes,
            );
        }

        if stats.rows > 0 || stats.bytes > 0 {
            let progress_val = ProgressValues {
                rows: stats.rows,
                bytes: stats.bytes,
            };
            self.ctx.get_aggregate_spill_progress().incr(&progress_val);
        }
    }
}
