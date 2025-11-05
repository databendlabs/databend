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

use std::mem;
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
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use parking_lot::Mutex;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::sessions::QueryContext;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataWriter;

struct PayloadWriter {
    path: String,
    writer: SpillsDataWriter,
}

impl PayloadWriter {
    fn try_create(prefix: &str) -> Result<Self> {
        let operator = DataOperator::instance().spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let file_path = format!("{}/{}", prefix, GlobalUniqName::unique());
        let spills_data_writer = buffer_pool.writer(operator, file_path.clone())?;

        Ok(PayloadWriter {
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
        mem::take(self)
    }
}

struct AggregatePayloadWriters {
    spill_prefix: String,
    partition_count: usize,
    writers: Vec<PayloadWriter>,
    write_stats: WriteStats,
}

impl AggregatePayloadWriters {
    pub fn create(prefix: &str, partition_count: usize) -> Self {
        AggregatePayloadWriters {
            spill_prefix: prefix.to_string(),
            partition_count,
            writers: vec![],
            write_stats: WriteStats::default(),
        }
    }

    fn ensure_writers(&mut self) -> Result<()> {
        if self.writers.is_empty() {
            let mut writers = Vec::with_capacity(self.partition_count);
            for _ in 0..self.partition_count {
                writers.push(PayloadWriter::try_create(&self.spill_prefix)?);
            }
            self.writers = writers;
        }
        Ok(())
    }

    pub fn write_ready_blocks(&mut self, ready_blocks: Vec<(usize, DataBlock)>) -> Result<()> {
        if ready_blocks.is_empty() {
            return Ok(());
        }

        self.ensure_writers()?;

        for (bucket, block) in ready_blocks {
            if block.is_empty() {
                continue;
            }

            let start = Instant::now();
            self.writers[bucket].write_block(block)?;

            let elapsed = start.elapsed();
            self.write_stats.accumulate(elapsed);
        }

        Ok(())
    }

    pub fn finalize(&mut self) -> Result<(Vec<NewSpilledPayload>, WriteStats)> {
        let writers = mem::take(&mut self.writers);
        if writers.is_empty() {
            return Ok((Vec::new(), self.write_stats.take()));
        }

        let mut spilled_payloads = Vec::new();
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

        let stats = self.write_stats.take();
        Ok((spilled_payloads, stats))
    }
}

struct SharedPartitionStreamInner {
    partition_stream: BlockPartitionStream,
    worker_count: usize,
    working_count: usize,
}

impl SharedPartitionStreamInner {
    pub fn finish(&mut self) -> Vec<(usize, DataBlock)> {
        self.working_count -= 1;

        if self.working_count == 0 {
            self.working_count = self.worker_count;

            let ids = self.partition_stream.partition_ids();

            let mut pending_blocks = Vec::with_capacity(ids.len());

            for id in ids {
                if let Some(block) = self.partition_stream.finalize_partition(id) {
                    pending_blocks.push((id, block));
                }
            }
        }
        vec![]
    }

    pub fn partition(&mut self, partition_id: u64, block: DataBlock) -> Vec<(usize, DataBlock)> {
        let indices = vec![partition_id; block.num_rows()];
        self.partition_stream.partition(indices, block, true)
    }
}

#[derive(Clone)]
pub struct SharedPartitionStream {
    inner: Arc<Mutex<SharedPartitionStreamInner>>,
}

impl SharedPartitionStream {
    pub fn new(
        worker_count: usize,
        max_rows: usize,
        max_bytes: usize,
        partition_count: usize,
    ) -> Self {
        let partition_stream = BlockPartitionStream::create(max_rows, max_bytes, partition_count);
        SharedPartitionStream {
            inner: Arc::new(Mutex::new(SharedPartitionStreamInner {
                partition_stream,
                worker_count,
                working_count: worker_count,
            })),
        }
    }

    pub fn finish(&self) -> Vec<(usize, DataBlock)> {
        let mut inner = self.inner.lock();
        inner.finish()
    }

    pub fn partition(&self, partition_id: usize, block: DataBlock) -> Vec<(usize, DataBlock)> {
        let mut inner = self.inner.lock();
        inner.partition(partition_id as u64, block)
    }
}

pub struct NewAggregateSpiller {
    pub memory_settings: MemorySettings,
    ctx: Arc<QueryContext>,
    read_setting: ReadSettings,
    partition_count: usize,
    partition_stream: SharedPartitionStream,
    payload_writers: AggregatePayloadWriters,
}

impl NewAggregateSpiller {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        partition_count: usize,
        partition_stream: SharedPartitionStream,
    ) -> Result<Self> {
        let memory_settings = MemorySettings::from_aggregate_settings(&ctx)?;
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let read_setting = ReadSettings::from_settings(&table_ctx.get_settings())?;
        let spill_prefix = ctx.query_id_spill_prefix();

        let payload_writers = AggregatePayloadWriters::create(&spill_prefix, partition_count);

        Ok(Self {
            memory_settings,
            ctx,
            read_setting,
            partition_count,
            partition_stream,
            payload_writers,
        })
    }

    pub fn spill(&mut self, partition_id: usize, block: DataBlock) -> Result<()> {
        let ready_blocks = self.partition_stream.partition(partition_id, block);
        self.payload_writers.write_ready_blocks(ready_blocks)?;
        Ok(())
    }

    pub fn spill_finish(&mut self) -> Result<Vec<NewSpilledPayload>> {
        let pending_blocks = self.partition_stream.finish();
        self.payload_writers.write_ready_blocks(pending_blocks)?;

        let (payloads, write_stats) = self.payload_writers.finalize()?;
        self.flush_write_profile(write_stats);

        Ok(payloads)
    }

    pub fn restore(&self, payload: NewSpilledPayload) -> Result<AggregateMeta> {
        let NewSpilledPayload {
            bucket,
            location,
            row_group,
        } = payload;

        let operator = DataOperator::instance().spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let mut reader = buffer_pool.reader(operator.clone(), location, vec![row_group.clone()])?;

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

    fn flush_read_profile(&self, instant: &Instant, read_bytes: usize) {
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadCount, 1);
        Profile::record_usize_profile(ProfileStatisticsName::RemoteSpillReadBytes, read_bytes);
        Profile::record_usize_profile(
            ProfileStatisticsName::RemoteSpillReadTime,
            instant.elapsed().as_millis() as usize,
        );
    }

    fn flush_write_profile(&self, stats: WriteStats) {
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
