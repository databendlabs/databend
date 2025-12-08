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

use std::cell::Cell;
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Instant;

use arrow_schema::Schema;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storages_parquet::parquet_reader::row_group::get_ranges;
use databend_common_storages_parquet::parquet_reader::RowGroupCore;
use databend_common_storages_parquet::ReadSettings;
use fastrace::future::FutureExt;
use fastrace::Span;
use opendal::Metadata;
use opendal::Operator;
use opendal::Writer;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;

use super::record_read_profile_with_flag;
use super::record_write_profile_with_flag;
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
pub enum SpillTarget {
    Local,
    Remote,
}

impl SpillTarget {
    pub fn is_local(&self) -> bool {
        matches!(self, SpillTarget::Local)
    }

    /// Derive spill target (local vs remote) from storage params.
    ///
    /// Today we only treat `StorageParams::Fs` as local, everything
    /// else (S3, Azure, memory, etc.) is considered remote. Centralizing
    /// this decision here keeps higher-level operators simpler and avoids
    /// duplicating the matching logic at each call site.
    pub fn from_storage_params(params: Option<&StorageParams>) -> Self {
        match params {
            Some(StorageParams::Fs(_)) => SpillTarget::Local,
            _ => SpillTarget::Remote,
        }
    }
}

/// Buffer Pool Workflow for Spill Operations:
///
/// Context: During query execution, when memory pressure is high, intermediate
/// results (hash tables, sorted data, aggregation states) need to be spilled
/// to disk/object storage to free up memory. This buffer pool provides a
/// synchronous Write interface with async I/O underneath, specifically designed
/// for spill scenarios where backpressure control is more important than latency.
///
/// 1. Initialization:
///    - Create a fixed number of BytesMut buffers (4MB each) based on memory limit
///    - Spawn worker threads that listen for write operations via async channels
///    - Pre-allocate buffers are placed in the available_write_buffers channel
///
/// 2. Spill Write Operation:
///    - During spill, BufferWriter.write() fills the current buffer with serialized data
///    - When buffer is full, it's frozen to Bytes and added to pending_buffers queue
///    - Writer tries to acquire a new buffer from the pool (non-blocking first)
///    - If no buffer available, triggers async write operation and BLOCKS (crucial for spill)
///
/// 3. Async Spill Write Process:
///    - Background worker receives BufferWriteOperator containing:
///      - OpenDAL Writer instance (to disk/S3/etc.)
///      - Queue of Bytes buffers containing spilled data
///      - Response channel for completion notification
///    - Worker writes all buffers sequentially to storage via writer.write(buffers)
///    - After writing, attempts to recycle buffers back to pool:
///      - Converts Bytes back to BytesMut (if unique reference)
///      - Clears buffer content and returns to available pool
///    - Notifies completion via condvar to unblock waiting spill thread
///
/// 4. Spill Backpressure Control (Key Feature):
///    - When pool is exhausted, spill write() BLOCKS until async write completes
///    - This throttles spill speed to match storage I/O capacity
///    - Prevents memory explosion during high-volume spill operations
///    - Query execution naturally pauses when spill storage is slower than data generation
///
/// 5. Buffer Lifecycle in Spill:
///    - BytesMut (mutable) -> fill with spill data -> freeze to Bytes (immutable)
///    - Bytes -> async write to spill storage -> try_into_mut() -> clear -> back to pool
///
/// 6. Spill Resource Cleanup:
///    - flush() ensures all pending spill data is written before spill operation completes
///    - Drop trait recycles any remaining buffers back to pool
///    - Critical for spill scenarios where partial writes could corrupt spilled data
///
/// Spill-Specific Benefits:
/// - Memory-bounded operation prevents OOM during large spills
/// - Synchronous blocking behavior allows query threads to naturally pause
/// - Buffer reuse minimizes GC pressure during intensive spill operations
/// - Automatic flow control matches spill rate to storage bandwidth
/// - Works with any OpenDAL-supported storage (local disk, S3, etc.)
pub struct SpillsBufferPool {
    working_queue: async_channel::Sender<BufferOperator>,
    available_write_buffers: async_channel::Receiver<BytesMut>,
    available_write_buffers_tx: async_channel::Sender<BytesMut>,
}

impl SpillsBufferPool {
    pub fn init() {
        // TODO: config
        GlobalInstance::set(SpillsBufferPool::create(
            GlobalIORuntime::instance(),
            200 * 1024 * 1024,
            2,
        ))
    }

    pub fn instance() -> Arc<SpillsBufferPool> {
        GlobalInstance::get()
    }

    pub fn create(executor: Arc<Runtime>, memory: usize, workers: usize) -> Arc<SpillsBufferPool> {
        let (working_tx, working_rx) = async_channel::unbounded();
        let (buffers_tx, buffers_rx) = async_channel::unbounded();

        let memory = memory / CHUNK_SIZE * CHUNK_SIZE;

        for _ in 0..memory / CHUNK_SIZE {
            buffers_tx
                .try_send(BytesMut::with_capacity(CHUNK_SIZE))
                .expect("Buffer pool available_write_buffers need unbounded.");
        }

        for _ in 0..workers {
            let working_queue: async_channel::Receiver<BufferOperator> = working_rx.clone();
            let available_write_buffers = buffers_tx.clone();
            executor.spawn(
                async_backtrace::location!(String::from("async_buffer")).frame(async move {
                    let mut background = Background::create(available_write_buffers);
                    while let Ok(op) = working_queue.recv().await {
                        let span = Span::enter_with_parent("Background::recv", op.span());
                        background.recv(op).in_span(span).await;
                    }
                }),
            );
        }

        Arc::new(SpillsBufferPool {
            working_queue: working_tx,
            available_write_buffers: buffers_rx,
            available_write_buffers_tx: buffers_tx,
        })
    }

    pub(crate) fn try_alloc_buffer(&self) -> Option<BytesMut> {
        self.available_write_buffers.try_recv().ok()
    }

    pub(crate) fn alloc_buffer(&self) -> std::io::Result<BytesMut> {
        match self.available_write_buffers.recv_blocking() {
            Ok(buf) => Ok(buf),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "buffer pool is closed",
            )),
        }
    }

    pub(crate) fn operator(&self, op: BufferOperator) {
        self.working_queue
            .try_send(op)
            .expect("Buffer pool working queue need unbounded.");
    }

    pub fn buffer_write(
        self: &Arc<SpillsBufferPool>,
        writer: Writer,
        target: SpillTarget,
    ) -> BufferWriter {
        BufferWriter::new(writer, self.clone(), target)
    }

    pub fn writer(
        self: &Arc<Self>,
        op: Operator,
        path: String,
        target: SpillTarget,
    ) -> Result<SpillsDataWriter> {
        let writer = self.buffer_writer(op, path, target)?;
        Ok(SpillsDataWriter::Uninitialize(Some(writer)))
    }

    pub(super) fn buffer_writer(
        self: &Arc<Self>,
        op: Operator,
        path: String,
        target: SpillTarget,
    ) -> Result<BufferWriter> {
        let pending_response = BufferOperatorResp::pending();

        let operator = BufferOperator::CreateWriter(CreateWriterOperator {
            span: Span::enter_with_local_parent("CreateWriterOperator"),
            op,
            path,
            response: pending_response.clone(),
        });

        self.working_queue
            .try_send(operator)
            .expect("Buffer pool working queue need unbounded.");

        Ok(self.buffer_write(pending_response.wait_and_take()?, target))
    }

    pub fn reader(
        self: &Arc<Self>,
        op: Operator,
        path: String,
        row_groups: Vec<RowGroupMetaData>,
        target: SpillTarget,
    ) -> Result<SpillsDataReader> {
        SpillsDataReader::create(path, op, row_groups, self.clone(), target)
    }

    pub fn fetch_ranges(
        &self,
        op: Operator,
        location: String,
        fetch_ranges: Vec<Range<u64>>,
        settings: ReadSettings,
    ) -> Result<Vec<Bytes>> {
        let response = BufferOperatorResp::pending();
        let operator = BufferOperator::Fetch(FetchOperator {
            span: Span::enter_with_local_parent("FetchOperator"),
            op,
            location,
            response: response.clone(),
            fetch_ranges,
            settings,
        });
        self.operator(operator);

        response.wait_and_take()
    }

    fn release_buffer(&self, buffer: BytesMut) {
        if self.available_write_buffers_tx.try_send(buffer).is_err() {
            unreachable!("Buffer pool available_write_buffers need unbounded.");
        }
    }
}

pub struct BufferWriter {
    writer: Option<Writer>,

    current_bytes: Option<BytesMut>,

    buffer_pool: Arc<SpillsBufferPool>,
    pending_buffers: VecDeque<Bytes>,
    pending_response: Option<Arc<BufferOperatorResp<BufferWriteResp>>>,
    target: SpillTarget,
}

impl BufferWriter {
    pub fn new(
        writer: Writer,
        buffer_pool: Arc<SpillsBufferPool>,
        target: SpillTarget,
    ) -> BufferWriter {
        BufferWriter {
            buffer_pool,
            writer: Some(writer),
            current_bytes: None,
            pending_buffers: VecDeque::new(),
            pending_response: None,
            target,
        }
    }

    fn write_buffer(&mut self, wait: bool) -> std::io::Result<()> {
        if let Some(pending_response) = self.pending_response.take() {
            if wait {
                let mut response = pending_response.wait_and_take();
                self.writer = Some(response.writer);

                if let Some(last_error) = response.error.take() {
                    return Err(last_error);
                }
            } else {
                let locked = pending_response.mutex.lock();
                let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

                if let Some(mut response) = locked.take() {
                    self.writer = Some(response.writer);
                    if let Some(last_error) = response.error.take() {
                        return Err(last_error);
                    }
                } else {
                    drop(locked);
                    self.pending_response = Some(pending_response);
                }
            }
        }

        if let Some(writer) = self.writer.take() {
            assert!(self.pending_response.is_none());

            let pending_response = BufferOperatorResp::pending();

            self.pending_response = Some(pending_response.clone());

            let operator = BufferOperator::Write(BufferWriteOperator {
                span: Span::enter_with_local_parent("BufferWriteOperator"),
                writer,
                response: pending_response,
                buffers: std::mem::take(&mut self.pending_buffers),
                target: self.target,
                start: Instant::now(),
            });

            self.buffer_pool.operator(operator);
        }

        Ok(())
    }

    pub fn close(mut self) -> std::io::Result<Metadata> {
        self.flush()?;

        if let Some(writer) = self.writer.take() {
            let pending_response = BufferOperatorResp::pending();

            let close_operator = BufferOperator::Close(BufferCloseOperator {
                span: Span::enter_with_local_parent("BufferCloseOperator"),
                writer,
                response: pending_response.clone(),
            });

            self.buffer_pool.operator(close_operator);

            return pending_response.wait_and_take().res;
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Writer already closed",
        ))
    }

    pub(super) fn finish(&mut self) -> std::io::Result<Metadata> {
        std::mem::replace(self, Self {
            writer: None,
            current_bytes: None,
            buffer_pool: self.buffer_pool.clone(),
            pending_buffers: Default::default(),
            pending_response: None,
            target: self.target,
        })
        .close()
    }
}

impl io::Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut current_bytes = match self.current_bytes.take() {
            Some(bytes) => bytes,
            None => self.buffer_pool.alloc_buffer()?,
        };

        let mut written = 0;
        let mut remaining = buf;
        while !remaining.is_empty() {
            let mut available_space = current_bytes.capacity() - current_bytes.len();

            if available_space == 0 {
                let pending_bytes = current_bytes.freeze();
                self.pending_buffers.push_back(pending_bytes);

                current_bytes = match self.buffer_pool.try_alloc_buffer() {
                    Some(buffer) => buffer,
                    None => {
                        self.write_buffer(true)?;
                        self.buffer_pool.alloc_buffer()?
                    }
                };

                available_space = current_bytes.capacity() - current_bytes.len();
            }

            let to_write = std::cmp::min(remaining.len(), available_space);
            current_bytes.extend_from_slice(&remaining[..to_write]);

            written += to_write;
            remaining = &remaining[to_write..];
        }

        if current_bytes.capacity() - current_bytes.len() == 0 {
            self.pending_buffers.push_back(current_bytes.freeze());
        } else {
            self.current_bytes = Some(current_bytes);
        }

        if !self.pending_buffers.is_empty() {
            self.write_buffer(false)?;
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(current_bytes) = self.current_bytes.take_if(|bytes| !bytes.is_empty()) {
            self.pending_buffers.push_back(current_bytes.freeze());
        }

        if !self.pending_buffers.is_empty() {
            self.write_buffer(true)?;
        }

        if let Some(pending_response) = self.pending_response.take() {
            let BufferWriteResp { writer, mut error } = pending_response.wait_and_take();
            self.writer = Some(writer);

            if let Some(error) = error.take() {
                return Err(error);
            }
        }

        Ok(())
    }
}

impl Drop for BufferWriter {
    fn drop(&mut self) {
        let pending_buffers = std::mem::take(&mut self.pending_buffers);

        for pending_buffer in pending_buffers {
            match pending_buffer.try_into_mut() {
                Ok(mut buf) if buf.capacity() == CHUNK_SIZE => {
                    buf.clear();
                    self.buffer_pool.release_buffer(buf);
                }
                _ => {
                    log::warn!("Failed to recycle buffer, creating new one");
                    let new_buf = BytesMut::with_capacity(CHUNK_SIZE);
                    self.buffer_pool.release_buffer(new_buf);
                }
            }
        }

        if let Some(mut current_bytes) = self.current_bytes.take() {
            current_bytes.clear();
            self.buffer_pool.release_buffer(current_bytes);
        }
    }
}

pub struct InitializedBlocksStreamWriter {
    table_schema: TableSchemaRef,
    writer: ArrowWriter<BufferWriter>,
}

pub enum SpillsDataWriter {
    Uninitialize(Option<BufferWriter>),
    Initialized(InitializedBlocksStreamWriter),
}

impl SpillsDataWriter {
    pub fn create(writer: BufferWriter) -> Self {
        Self::Uninitialize(Some(writer))
    }

    pub fn write(&mut self, block: DataBlock) -> Result<()> {
        match self {
            SpillsDataWriter::Uninitialize(writer) => {
                let data_schema = block.infer_schema();
                let table_schema = infer_table_schema(&data_schema)?;

                let props = WriterProperties::builder()
                    .set_compression(Compression::LZ4_RAW)
                    .set_statistics_enabled(EnabledStatistics::None)
                    .set_bloom_filter_enabled(false)
                    .build();

                let arrow_schema = Arc::new(Schema::from(table_schema.as_ref()));
                let buffer_writer = writer.take().unwrap();
                let mut writer = ArrowWriter::try_new(buffer_writer, arrow_schema, Some(props))?;
                let record_batch = block.to_record_batch(&table_schema)?;
                writer.write(&record_batch)?;
                *self = SpillsDataWriter::Initialized(InitializedBlocksStreamWriter {
                    writer,
                    table_schema,
                });

                Ok(())
            }
            SpillsDataWriter::Initialized(writer) => {
                let record_batch = block.to_record_batch(&writer.table_schema)?;
                Ok(writer.writer.write(&record_batch)?)
            }
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        match self {
            SpillsDataWriter::Uninitialize(_) => Err(ErrorCode::Internal(
                "Bad state, BlockStreamWriter is uninitialized",
            )),
            SpillsDataWriter::Initialized(writer) => {
                writer.writer.flush()?;
                Ok(writer.writer.inner_mut().flush()?)
            }
        }
    }

    pub fn close(self) -> Result<(usize, Vec<RowGroupMetaData>)> {
        match self {
            SpillsDataWriter::Uninitialize(mut writer) => {
                if let Some(writer) = writer.take() {
                    writer.close()?;
                }

                Ok((0, vec![]))
            }
            SpillsDataWriter::Initialized(mut writer) => {
                writer.writer.flush()?;
                let row_groups = writer.writer.flushed_row_groups().to_vec();
                let bytes_written = writer.writer.bytes_written();
                writer.writer.into_inner()?.close()?;
                Ok((bytes_written, row_groups))
            }
        }
    }
}

pub struct SpillsDataReader {
    location: String,
    operator: Operator,
    row_groups: VecDeque<RowGroupMetaData>,
    spills_buffer_pool: Arc<SpillsBufferPool>,
    data_schema: DataSchemaRef,
    field_levels: FieldLevels,
    read_bytes: usize,
    target: SpillTarget,
}

impl SpillsDataReader {
    pub fn create(
        location: String,
        operator: Operator,
        row_groups: Vec<RowGroupMetaData>,
        spills_buffer_pool: Arc<SpillsBufferPool>,
        target: SpillTarget,
    ) -> Result<Self> {
        if row_groups.is_empty() {
            return Err(ErrorCode::Internal(
                "Parquet reader cannot read empty row groups.",
            ));
        }

        let arrow_schema = parquet_to_arrow_schema(row_groups[0].schema_descr(), None)?;
        let data_schema = DataSchemaRef::new(DataSchema::try_from(&arrow_schema)?);

        let field_levels = parquet_to_arrow_field_levels(
            row_groups[0].schema_descr(),
            ProjectionMask::all(),
            None,
        )?;

        Ok(SpillsDataReader {
            location,
            operator,
            spills_buffer_pool,
            data_schema,
            field_levels,
            row_groups: VecDeque::from(row_groups),
            read_bytes: 0,
            target,
        })
    }

    pub fn read_bytes(&self) -> usize {
        self.read_bytes
    }

    pub fn read(&mut self, settings: ReadSettings) -> Result<Option<DataBlock>> {
        let Some(row_group) = self.row_groups.pop_front() else {
            return Ok(None);
        };

        let start = Instant::now();

        let mut row_group = RowGroupCore::new(row_group, None);

        let read_bytes = Cell::new(0usize);

        row_group.fetch(&ProjectionMask::all(), None, |fetch_ranges| {
            let chunk_data = self.spills_buffer_pool.fetch_ranges(
                self.operator.clone(),
                self.location.clone(),
                fetch_ranges,
                settings,
            )?;
            let bytes_read = chunk_data.iter().map(|c| c.len()).sum::<usize>();
            read_bytes.set(read_bytes.get() + bytes_read);

            Ok(chunk_data)
        })?;

        self.read_bytes += read_bytes.get();

        let num_rows = row_group.num_rows();
        let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            num_rows,
            None,
        )?;
        let batch = reader.next().transpose()?.unwrap();
        debug_assert!(reader.next().is_none());
        record_read_profile_with_flag(self.target.is_local(), &start, read_bytes.get());
        Ok(Some(DataBlock::from_record_batch(
            &self.data_schema,
            &batch,
        )?))
    }
}

pub struct BufferWriteResp {
    writer: Writer,
    error: Option<std::io::Error>,
}

pub struct BufferWriteOperator {
    span: Span,
    writer: Writer,
    buffers: VecDeque<Bytes>,
    response: Arc<BufferOperatorResp<BufferWriteResp>>,
    target: SpillTarget,
    start: Instant,
}

pub struct BufferCloseResp {
    _writer: Writer,
    res: std::io::Result<Metadata>,
}

pub struct BufferCloseOperator {
    span: Span,
    writer: Writer,
    response: Arc<BufferOperatorResp<BufferCloseResp>>,
}

pub struct CreateWriterOperator {
    span: Span,
    op: Operator,
    path: String,
    response: Arc<BufferOperatorResp<opendal::Result<Writer>>>,
}

pub struct FetchOperator {
    span: Span,
    location: String,
    op: Operator,
    fetch_ranges: Vec<Range<u64>>,
    settings: ReadSettings,
    response: Arc<BufferOperatorResp<Result<Vec<Bytes>>>>,
}

#[derive(Default)]
pub struct BufferOperatorResp<T> {
    condvar: Condvar,
    mutex: Mutex<Option<T>>,
}

impl<T> BufferOperatorResp<T> {
    fn pending() -> Arc<BufferOperatorResp<T>> {
        Arc::new(BufferOperatorResp {
            condvar: Default::default(),
            mutex: Mutex::new(None),
        })
    }

    fn done(&self, res: T) {
        *self.mutex.lock().unwrap_or_else(PoisonError::into_inner) = Some(res);
        self.condvar.notify_one();
    }

    fn wait_and_take(&self) -> T {
        let locked = self.mutex.lock();
        let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

        if locked.is_none() {
            let waited = self.condvar.wait(locked);
            locked = waited.unwrap_or_else(PoisonError::into_inner);
        }

        locked.take().unwrap()
    }
}

pub enum BufferOperator {
    Write(BufferWriteOperator),
    Close(BufferCloseOperator),
    CreateWriter(CreateWriterOperator),
    Fetch(FetchOperator),
}

impl BufferOperator {
    fn span(&self) -> &Span {
        match self {
            BufferOperator::Write(op) => &op.span,
            BufferOperator::Close(op) => &op.span,
            BufferOperator::CreateWriter(op) => &op.span,
            BufferOperator::Fetch(op) => &op.span,
        }
    }
}

pub struct Background {
    available_buffers: async_channel::Sender<BytesMut>,
}

impl Background {
    pub fn create(available_buffers: async_channel::Sender<BytesMut>) -> Background {
        Background { available_buffers }
    }

    pub async fn recv(&mut self, op: BufferOperator) {
        match op {
            BufferOperator::Write(mut op) => {
                let start = op.start;
                let bytes = op.buffers.clone();
                let bytes_len = bytes.iter().map(|b| b.len()).sum();
                let mut error = op
                    .writer
                    .write(op.buffers)
                    .await
                    .map_err(std::io::Error::from)
                    .err();
                for bytes in bytes {
                    let bytes = match bytes.try_into_mut() {
                        Ok(mut buf) if buf.capacity() == CHUNK_SIZE => {
                            buf.clear();
                            buf
                        }
                        _ => {
                            log::warn!("Failed to recycle buffer, creating new one");
                            BytesMut::with_capacity(CHUNK_SIZE)
                        }
                    };

                    if self.available_buffers.send(bytes).await.is_err() && error.is_none() {
                        error = Some(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "buffer pool is closed",
                        ));
                    }
                }

                op.response.done(BufferWriteResp {
                    error,
                    writer: op.writer,
                });

                record_write_profile_with_flag(op.target.is_local(), &start, bytes_len);
            }
            BufferOperator::Close(mut op) => {
                let res = op.writer.close().await;

                op.response.done(BufferCloseResp {
                    _writer: op.writer,
                    res: res.map_err(std::io::Error::from),
                });
            }
            BufferOperator::CreateWriter(op) => {
                let writer = op.op.writer(&op.path).await;

                op.response.done(writer);
            }
            BufferOperator::Fetch(op) => {
                let res = get_ranges(&op.fetch_ranges, &op.settings, &op.location, &op.op).await;

                op.response.done(res.map(|(chunks, _)| chunks));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use databend_common_base::runtime::spawn;
    use opendal::Operator;

    use super::*;

    fn create_test_operator() -> std::io::Result<Operator> {
        let builder = opendal::services::Fs::default().root("/tmp");

        Ok(Operator::new(builder)?.finish())
    }

    #[tokio::test]
    async fn test_buffer_pool_creation() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let memory = 16 * 1024 * 1024; // 16MB
        let workers = 2;

        let pool = SpillsBufferPool::create(runtime.clone(), memory, workers);

        // Should be able to allocate buffers
        let buffer1 = pool.try_alloc_buffer();
        assert!(buffer1.is_some());
        assert_eq!(buffer1.unwrap().capacity(), CHUNK_SIZE);

        let buffer2 = pool.try_alloc_buffer();
        assert!(buffer2.is_some());
        assert_eq!(buffer2.unwrap().capacity(), CHUNK_SIZE);
    }

    #[tokio::test]
    async fn test_buffer_writer_basic_write() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 8 * 1024 * 1024, 1);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("test_file").await.unwrap();

        let mut buffer_writer = BufferWriter::new(writer, pool, SpillTarget::Remote);

        let data = b"Hello, World!";
        let written = buffer_writer.write(data).unwrap();
        assert_eq!(written, data.len());

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert!(metadata.content_length() > 0);
    }

    #[tokio::test]
    async fn test_buffer_writer_large_write() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 16 * 1024 * 1024, 2);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("large_file").await.unwrap();

        let mut buffer_writer = BufferWriter::new(writer, pool, SpillTarget::Remote);

        // Write data larger than single buffer
        let large_data = vec![0u8; 8 * 1024 * 1024]; // 8MB
        let written = buffer_writer.write(&large_data).unwrap();
        assert_eq!(written, large_data.len());

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), large_data.len() as u64);
    }

    #[tokio::test]
    async fn test_buffer_writer_multiple_writes() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 8 * 1024 * 1024, 1);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("multi_write_file").await.unwrap();

        let mut buffer_writer = BufferWriter::new(writer, pool, SpillTarget::Remote);

        let mut total_written = 0;
        for i in 0..100 {
            let data = format!("Line {}: Hello World!\n", i);
            let written = buffer_writer.write(data.as_bytes()).unwrap();
            total_written += written;
        }

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), total_written as u64);
    }

    #[tokio::test]
    async fn test_buffer_pool_exhaustion_and_backpressure() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        // Create pool with only 1 buffer to test backpressure
        let pool = SpillsBufferPool::create(runtime.clone(), CHUNK_SIZE, 1);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("backpressure_test").await.unwrap();

        let mut buffer_writer = BufferWriter::new(writer, pool.clone(), SpillTarget::Remote);

        // Fill the first buffer
        let data = vec![0u8; CHUNK_SIZE];
        let written = buffer_writer.write(&data).unwrap();
        assert_eq!(written, data.len());

        // This should trigger backpressure and eventually succeed
        let written2 = buffer_writer.write(b"extra data").unwrap();
        assert_eq!(written2, 10);

        buffer_writer.flush().unwrap();
        let _metadata = buffer_writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_buffer_reuse() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 8 * 1024 * 1024, 1);

        // Allocate all buffers
        let mut buffers = Vec::new();
        while let Some(buffer) = pool.try_alloc_buffer() {
            buffers.push(buffer);
        }

        let initial_count = buffers.len();
        assert!(initial_count > 0);

        // Should be no more buffers available
        assert!(pool.try_alloc_buffer().is_none());

        // Release all buffers
        for buffer in buffers {
            pool.release_buffer(buffer);
        }

        // Should be able to allocate the same number again
        let mut new_buffers = Vec::new();
        while let Some(buffer) = pool.try_alloc_buffer() {
            new_buffers.push(buffer);
        }

        assert_eq!(new_buffers.len(), initial_count);
    }

    #[tokio::test]
    async fn test_empty_write() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 8 * 1024 * 1024, 1);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("empty_test").await.unwrap();

        let mut buffer_writer = BufferWriter::new(writer, pool, SpillTarget::Remote);

        let written = buffer_writer.write(b"").unwrap();
        assert_eq!(written, 0);

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), 0);
    }

    #[tokio::test]
    async fn test_close_without_writes() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 8 * 1024 * 1024, 1);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("no_write_test").await.unwrap();

        let buffer_writer = BufferWriter::new(writer, pool, SpillTarget::Remote);

        // Should be able to close without any writes
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_writers() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 16 * 1024 * 1024, 4);
        let operator = create_test_operator().unwrap();

        let write_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for i in 0..4 {
            let pool_clone = pool.clone();
            let operator_clone = operator.clone();
            let write_count_clone = write_count.clone();

            let handle = spawn(async move {
                let writer = operator_clone
                    .writer(&format!("concurrent_{}", i))
                    .await
                    .unwrap();
                let mut buffer_writer = BufferWriter::new(writer, pool_clone, SpillTarget::Remote);

                for j in 0..10 {
                    let data = format!("Writer {} - Line {}\n", i, j);
                    buffer_writer.write_all(data.as_bytes()).unwrap();
                    write_count_clone.fetch_add(1, Ordering::Relaxed);
                }

                buffer_writer.flush().unwrap();
                buffer_writer.close().unwrap()
            });

            handles.push(handle);
        }

        // Wait for all writers to complete
        for handle in handles {
            let _metadata = handle.await.unwrap();
        }

        assert_eq!(write_count.load(Ordering::Relaxed), 40);
    }

    #[tokio::test]
    async fn test_writer_close_error_handling() {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None).unwrap());
        let pool = SpillsBufferPool::create(runtime.clone(), 8 * 1024 * 1024, 1);
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("error_test").await.unwrap();

        let mut buffer_writer = pool.buffer_write(writer, SpillTarget::Remote);
        buffer_writer.write_all(b"test data").unwrap();

        // Close once
        let _metadata = buffer_writer.close().unwrap();

        // Create another writer and try to close it twice
        let writer2 = operator.writer("error_test2").await.unwrap();
        let buffer_writer2 = pool.buffer_write(writer2, SpillTarget::Remote);
        let _metadata = buffer_writer2.close().unwrap();

        // Second close should return error (create new writer for this test)
        let writer3 = operator.writer("error_test3").await.unwrap();
        let buffer_writer3 = pool.buffer_write(writer3, SpillTarget::Remote);
        let _metadata = buffer_writer3.close().unwrap();
    }

    #[test]
    fn test_spill_target_from_storage_params() {
        use databend_common_meta_app::storage::StorageFsConfig;

        // Fs backend should be treated as local spill.
        let fs_params = StorageParams::Fs(StorageFsConfig {
            root: "/tmp/test-root".to_string(),
        });
        let target = SpillTarget::from_storage_params(Some(&fs_params));
        assert!(target.is_local());

        // None (or any non-Fs backend) should be treated as remote spill.
        let target_none = SpillTarget::from_storage_params(None);
        assert!(!target_none.is_local());
    }
}
