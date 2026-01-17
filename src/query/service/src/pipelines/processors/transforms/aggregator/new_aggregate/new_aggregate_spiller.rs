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

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use log::debug;
use log::info;
use parking_lot::Mutex;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::NewSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::sessions::QueryContext;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataWriter;

struct PayloadWriter {
    path: String,
    writer: SpillsDataWriter,
}

impl PayloadWriter {
    fn try_create(prefix: &str) -> Result<Self> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let file_path = format!("{}/{}", prefix, GlobalUniq::unique());
        let spills_data_writer = buffer_pool.writer(operator, file_path.clone(), target)?;

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
    writers: Vec<Option<PayloadWriter>>,
    write_stats: WriteStats,
    ctx: Arc<QueryContext>,
}

impl AggregatePayloadWriters {
    pub fn create(prefix: &str, partition_count: usize, ctx: Arc<QueryContext>) -> Self {
        AggregatePayloadWriters {
            spill_prefix: prefix.to_string(),
            partition_count,
            writers: Self::empty_writers(partition_count),
            write_stats: WriteStats::default(),
            ctx,
        }
    }

    fn empty_writers(partition_count: usize) -> Vec<Option<PayloadWriter>> {
        std::iter::repeat_with(|| None)
            .take(partition_count)
            .collect::<Vec<_>>()
    }

    fn ensure_writer(&mut self, bucket: usize) -> Result<&mut PayloadWriter> {
        if self.writers[bucket].is_none() {
            self.writers[bucket] = Some(PayloadWriter::try_create(&self.spill_prefix)?);
        }

        Ok(self.writers[bucket].as_mut().unwrap())
    }

    pub fn write_ready_blocks(&mut self, ready_blocks: Vec<(usize, DataBlock)>) -> Result<()> {
        if ready_blocks.is_empty() {
            return Ok(());
        }

        for (bucket, block) in ready_blocks {
            if block.is_empty() {
                continue;
            }

            let start = Instant::now();
            let writer = self.ensure_writer(bucket)?;
            writer.write_block(block)?;

            let elapsed = start.elapsed();
            self.write_stats.accumulate(elapsed);
        }

        Ok(())
    }

    pub fn finalize(&mut self) -> Result<Vec<NewSpilledPayload>> {
        let writers = mem::replace(&mut self.writers, Self::empty_writers(self.partition_count));

        if writers.iter().all(|writer| writer.is_none()) {
            return Ok(Vec::new());
        }

        let mut spilled_payloads = Vec::new();
        for (partition_id, writer) in writers.into_iter().enumerate() {
            let Some(writer) = writer else {
                continue;
            };

            let (path, written_size, row_groups) = writer.close()?;

            if written_size != 0 {
                info!(
                    "Write aggregate spill finished: (bucket: {}, location: {}, bytes: {}, rows: {}, batch_count: {})",
                    partition_id,
                    path,
                    written_size,
                    row_groups.iter().map(|rg| rg.num_rows()).sum::<i64>(),
                    row_groups.len()
                );
            }

            self.ctx.add_spill_file(
                Location::Remote(path.clone()),
                Layout::Aggregate,
                written_size,
            );

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
        flush_write_profile(&self.ctx, stats);

        Ok(spilled_payloads)
    }
}

pub trait PartitionStream: Send {
    fn finish(&mut self) -> Vec<(usize, DataBlock)>;
    fn partition(&mut self, partition_id: usize, block: DataBlock) -> Vec<(usize, DataBlock)>;
}

pub struct LocalPartitionStream {
    partition_stream: BlockPartitionStream,
}

impl LocalPartitionStream {
    pub fn new(max_rows: usize, max_bytes: usize, partition_count: usize) -> Self {
        let partition_stream = BlockPartitionStream::create(max_rows, max_bytes, partition_count);
        Self { partition_stream }
    }
}

impl PartitionStream for LocalPartitionStream {
    fn finish(&mut self) -> Vec<(usize, DataBlock)> {
        let ids = self.partition_stream.partition_ids();
        let mut pending_blocks = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(block) = self.partition_stream.finalize_partition(id) {
                pending_blocks.push((id, block));
            }
        }
        pending_blocks
    }

    fn partition(&mut self, partition_id: usize, block: DataBlock) -> Vec<(usize, DataBlock)> {
        let indices = vec![partition_id as u64; block.num_rows()];
        self.partition_stream.partition(indices, block, true)
    }
}

struct SharedPartitionStreamInner {
    partition_stream: BlockPartitionStream,
    worker_count: usize,
    finish_count: usize,
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
                finish_count: 0,
            })),
        }
    }

    pub fn do_finish(&self) -> Vec<(usize, DataBlock)> {
        let mut inner = self.inner.lock();
        inner.finish_count += 1;

        if inner.finish_count == inner.worker_count {
            inner.finish_count = 0;

            let ids = inner.partition_stream.partition_ids();

            let mut pending_blocks = Vec::with_capacity(ids.len());

            for id in ids {
                if let Some(block) = inner.partition_stream.finalize_partition(id) {
                    pending_blocks.push((id, block));
                }
            }
            return pending_blocks;
        }
        vec![]
    }

    pub fn do_partition(&self, partition_id: usize, block: DataBlock) -> Vec<(usize, DataBlock)> {
        let mut inner = self.inner.lock();
        let indices = vec![partition_id as u64; block.num_rows()];
        inner.partition_stream.partition(indices, block, true)
    }
}

impl PartitionStream for SharedPartitionStream {
    fn finish(&mut self) -> Vec<(usize, DataBlock)> {
        self.do_finish()
    }

    fn partition(&mut self, partition_id: usize, block: DataBlock) -> Vec<(usize, DataBlock)> {
        self.do_partition(partition_id, block)
    }
}

pub struct NewAggregateSpiller<P: PartitionStream = SharedPartitionStream> {
    pub memory_settings: MemorySettings,
    read_setting: ReadSettings,
    partition_stream: P,
    payload_writers: AggregatePayloadWriters,
}

impl<P: PartitionStream> NewAggregateSpiller<P> {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        partition_count: usize,
        partition_stream: P,
    ) -> Result<Self> {
        let memory_settings = MemorySettings::from_aggregate_settings(&ctx)?;
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let read_setting = ReadSettings::from_settings(&table_ctx.get_settings())?;
        let spill_prefix = ctx.query_id_spill_prefix();

        let payload_writers = AggregatePayloadWriters::create(&spill_prefix, partition_count, ctx);

        Ok(Self {
            memory_settings,
            read_setting,
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

        let payloads = self.payload_writers.finalize()?;
        debug!(
            "[NewAggregateSpiller] spill finish with {} payloads",
            payloads.len()
        );
        Ok(payloads)
    }

    pub fn restore(&self, payload: NewSpilledPayload) -> Result<AggregateMeta> {
        restore_payload(self.read_setting, payload)
    }
}

pub struct NewAggregateSpillReader {
    read_setting: ReadSettings,
}

impl NewAggregateSpillReader {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        let table_ctx: Arc<dyn TableContext> = ctx;
        let read_setting = ReadSettings::from_settings(&table_ctx.get_settings())?;
        Ok(Self { read_setting })
    }

    pub fn restore(&self, payload: NewSpilledPayload) -> Result<AggregateMeta> {
        restore_payload(self.read_setting, payload)
    }
}

fn restore_payload(
    read_setting: ReadSettings,
    payload: NewSpilledPayload,
) -> Result<AggregateMeta> {
    let NewSpilledPayload {
        bucket,
        location,
        row_group,
    } = payload;

    let data_operator = DataOperator::instance();
    let target = SpillTarget::from_storage_params(data_operator.spill_params());
    let operator = data_operator.spill_operator();
    let buffer_pool = SpillsBufferPool::instance();

    let mut reader = buffer_pool.reader(
        operator.clone(),
        location.clone(),
        vec![row_group.clone()],
        target,
    )?;

    let instant = Instant::now();
    let data_block = reader.read(read_setting)?;
    let elapsed = instant.elapsed();

    let read_size = reader.read_bytes();

    info!(
        "Read aggregate spill finished: (bucket: {}, location: {}, bytes: {}, rows: {}, elapsed: {:?})",
        bucket,
        location,
        read_size,
        row_group.num_rows(),
        elapsed
    );

    if let Some(block) = data_block {
        Ok(AggregateMeta::Serialized(SerializedPayload {
            bucket,
            data_block: block,
            // this field is no longer used in new aggregate
            max_partition_count: 0,
        }))
    } else {
        Err(ErrorCode::Internal("read empty block from final aggregate"))
    }
}

fn flush_write_profile(ctx: &Arc<QueryContext>, stats: WriteStats) {
    if stats.count == 0 && stats.bytes == 0 && stats.rows == 0 {
        return;
    }

    if stats.rows > 0 || stats.bytes > 0 {
        let progress_val = ProgressValues {
            rows: stats.rows,
            bytes: stats.bytes,
        };
        ctx.get_aggregate_spill_progress().incr(&progress_val);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use databend_common_exception::Result;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;
    use crate::pipelines::processors::transforms::aggregator::new_aggregate::LocalPartitionStream;
    use crate::pipelines::processors::transforms::aggregator::new_aggregate::SharedPartitionStream;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_aggregate_payload_writers_lazy_init() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let partition_count = 4;
        let partition_stream = SharedPartitionStream::new(1, 1024, 1024 * 1024, partition_count);
        let mut spiller =
            NewAggregateSpiller::try_create(ctx.clone(), partition_count, partition_stream)?;

        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32, 2, 3])]);

        spiller.spill(0, block.clone())?;
        spiller.spill(2, block)?;

        let payloads = spiller.spill_finish()?;

        assert_eq!(payloads.len(), 2);

        let spilled_files = ctx.get_spilled_files();
        assert_eq!(spilled_files.len(), 2);

        let buckets: HashSet<_> = payloads.iter().map(|p| p.bucket).collect();
        assert!(buckets.contains(&0));
        assert!(buckets.contains(&2));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_aggregate_payload_writers_local_stream() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let partition_count = 4;
        let partition_stream = LocalPartitionStream::new(1024, 1024 * 1024, partition_count);
        let mut spiller =
            NewAggregateSpiller::try_create(ctx.clone(), partition_count, partition_stream)?;

        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32, 2, 3])]);

        spiller.spill(0, block.clone())?;
        spiller.spill(2, block)?;

        let payloads = spiller.spill_finish()?;

        assert_eq!(payloads.len(), 2);

        let spilled_files = ctx.get_spilled_files();
        assert_eq!(spilled_files.len(), 2);

        let buckets: HashSet<_> = payloads.iter().map(|p| p.bucket).collect();
        assert!(buckets.contains(&0));
        assert!(buckets.contains(&2));

        Ok(())
    }
}
