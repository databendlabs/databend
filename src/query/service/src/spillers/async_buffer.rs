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

use std::io;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_schema::Schema;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::spawn;
use databend_common_config::SpillConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::infer_table_schema;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storages_parquet::ReadSettings;
use databend_common_storages_parquet::parquet_reader::RowGroupCore;
use databend_common_storages_parquet::parquet_reader::row_group::get_ranges;
use fastrace::Span;
use fastrace::future::FutureExt;
use opendal::Metadata;
use opendal::Operator;
use opendal::Writer;
use parquet::arrow::ArrowWriter;
use parquet::arrow::FieldLevels;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::basic::Compression;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;

use super::record_read_profile;
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

    pub fn from_storage_params(params: Option<&StorageParams>) -> Self {
        match params {
            Some(StorageParams::Fs(_)) => SpillTarget::Local,
            _ => SpillTarget::Remote,
        }
    }
}

pub struct SpillsBufferPool {
    _runtime: Arc<Runtime>,
    working_queue: async_channel::Sender<BufferOperator>,
    available_write_buffers: async_channel::Receiver<BytesMut>,
    available_write_buffers_tx: async_channel::Sender<BytesMut>,
    blocking_nanos: Arc<AtomicU64>,
    blocking_count: Arc<AtomicU64>,
}

impl SpillsBufferPool {
    pub fn init(config: &SpillConfig) -> Result<()> {
        GlobalInstance::set(SpillsBufferPool::create(
            config.buffer_pool_memory as usize,
            config.buffer_pool_workers,
        )?);
        Ok(())
    }

    pub fn instance() -> Arc<SpillsBufferPool> {
        GlobalInstance::get()
    }

    pub fn create(memory: usize, workers: usize) -> Result<Arc<SpillsBufferPool>> {
        let runtime = Arc::new(Runtime::with_worker_threads(
            workers,
            Some("spill-worker".to_owned()),
        )?);

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
            runtime.spawn(
                async_backtrace::location!(String::from("async_buffer")).frame(async move {
                    let mut background = Background::create();
                    while let Ok(op) = working_queue.recv().await {
                        let span = Span::enter_with_parent("Background::recv", op.span());
                        background.recv(op).in_span(span).await;
                    }
                }),
            );
        }

        let blocking_nanos = Arc::new(AtomicU64::new(0));
        let blocking_count = Arc::new(AtomicU64::new(0));

        {
            let blocking_nanos = Arc::clone(&blocking_nanos);
            let blocking_count = Arc::clone(&blocking_count);
            runtime.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    let count = blocking_count.swap(0, Ordering::Relaxed);
                    let nanos = blocking_nanos.swap(0, Ordering::Relaxed);
                    if count > 0 {
                        log::info!(
                            "SpillsBufferPool alloc blocked {} times, total {:.2}ms in last 60s",
                            count,
                            nanos as f64 / 1_000_000.0,
                        );
                    }
                }
            });
        }

        Ok(Arc::new(SpillsBufferPool {
            _runtime: runtime,
            working_queue: working_tx,
            available_write_buffers: buffers_rx,
            available_write_buffers_tx: buffers_tx,
            blocking_nanos,
            blocking_count,
        }))
    }

    pub(crate) fn alloc_buffer(&self) -> std::io::Result<BytesMut> {
        if let Ok(buf) = self.available_write_buffers.try_recv() {
            return Ok(buf);
        }

        let start = Instant::now();
        let result = match self.available_write_buffers.recv_blocking() {
            Ok(buf) => Ok(buf),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "buffer pool is closed",
            )),
        };
        self.blocking_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.blocking_count.fetch_add(1, Ordering::Relaxed);
        result
    }

    pub(crate) fn operator(&self, op: BufferOperator) {
        self.working_queue
            .try_send(op)
            .expect("Buffer pool working queue need unbounded.");
    }

    pub fn buffer_write(self: &Arc<SpillsBufferPool>, writer: Writer) -> BufferWriter {
        let (buffer_tx, buffer_rx) = async_channel::unbounded::<Bytes>();

        let response = BufferOperatorResp::pending();

        self.operator(BufferOperator::WriterTask(BufferWriterTaskOperator {
            writer,
            buffer_rx,
            response: response.clone(),
            available_buffers: self.available_write_buffers_tx.clone(),
            span: Span::enter_with_local_parent("BufferWriterTask"),
        }));

        BufferWriter {
            buffer_tx,
            current_bytes: None,
            buffer_pool: self.clone(),
            response,
        }
    }

    pub fn writer(self: &Arc<Self>, op: Operator, path: String) -> Result<SpillsDataWriter> {
        let writer = self.buffer_writer(op, path)?;
        Ok(SpillsDataWriter::Uninitialize(Some(writer)))
    }

    pub(super) fn buffer_writer(
        self: &Arc<Self>,
        op: Operator,
        path: String,
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

        Ok(self.buffer_write(pending_response.wait_and_take()?))
    }

    pub fn reader(
        self: &Arc<Self>,
        op: Operator,
        path: String,
        data_schema: DataSchemaRef,
        row_groups: Vec<RowGroupMetaData>,
        target: SpillTarget,
        settings: ReadSettings,
    ) -> Result<SpillsDataReader> {
        SpillsDataReader::create(
            path,
            op,
            data_schema,
            row_groups,
            self.clone(),
            target,
            settings,
        )
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
    current_bytes: Option<BytesMut>,
    buffer_pool: Arc<SpillsBufferPool>,
    buffer_tx: async_channel::Sender<Bytes>,
    response: Arc<BufferOperatorResp<io::Result<Metadata>>>,
}

impl BufferWriter {
    pub fn close(mut self) -> io::Result<Metadata> {
        if let Some(b) = self.current_bytes.take() {
            if self.buffer_tx.try_send(b.freeze()).is_err() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
        }

        self.buffer_tx.close();
        self.response.wait_and_take()
    }

    pub(super) fn finish(&mut self) -> std::io::Result<Metadata> {
        if let Some(b) = self.current_bytes.take() {
            if self.buffer_tx.try_send(b.freeze()).is_err() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
        }

        self.buffer_tx.close();
        self.response.wait_and_take()
    }

    fn last_error(&mut self) -> io::Result<()> {
        let locked = self.response.mutex.lock();
        let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

        match locked.take_if(|x| x.is_err()) {
            Some(Err(err)) => Err(err),
            _ => Ok(()),
        }
    }
}

impl io::Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        {
            let locked = self.response.mutex.lock();
            let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

            if let Some(Err(cause)) = locked.take_if(|x| x.is_err()) {
                return Err(cause);
            }
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
                if self.buffer_tx.try_send(current_bytes.freeze()).is_err() {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }

                current_bytes = self.buffer_pool.alloc_buffer()?;
                available_space = current_bytes.capacity() - current_bytes.len();
            }

            let to_write = std::cmp::min(remaining.len(), available_space);
            current_bytes.extend_from_slice(&remaining[..to_write]);
            written += to_write;
            remaining = &remaining[to_write..];
        }

        if current_bytes.capacity() - current_bytes.len() == 0 {
            if self.buffer_tx.try_send(current_bytes.freeze()).is_err() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
        } else {
            self.current_bytes = Some(current_bytes);
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(b) = self.current_bytes.take() {
            if self.buffer_tx.try_send(b.freeze()).is_err() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
        }

        self.last_error()
    }
}

impl Drop for BufferWriter {
    fn drop(&mut self) {
        if let Some(mut b) = self.current_bytes.take() {
            b.clear();
            self.buffer_pool.release_buffer(b);
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
                    .set_dictionary_enabled(false)
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
    receiver: async_channel::Receiver<Result<FetchedRowGroup>>,
    data_schema: DataSchemaRef,
    field_levels: FieldLevels,
    read_bytes: usize,
    target: SpillTarget,
}

impl SpillsDataReader {
    pub fn create(
        location: String,
        operator: Operator,
        data_schema: DataSchemaRef,
        row_groups: Vec<RowGroupMetaData>,
        spills_buffer_pool: Arc<SpillsBufferPool>,
        target: SpillTarget,
        settings: ReadSettings,
    ) -> Result<Self> {
        if row_groups.is_empty() {
            return Err(ErrorCode::Internal(
                "Parquet reader cannot read empty row groups.",
            ));
        }

        let field_levels = parquet_to_arrow_field_levels(
            row_groups[0].schema_descr(),
            ProjectionMask::all(),
            None,
        )?;

        let (tx, rx) = async_channel::bounded(1);
        spills_buffer_pool.operator(BufferOperator::ReaderTask(ReaderTaskOperator {
            span: Span::enter_with_local_parent("ReaderTask"),
            op: operator,
            location,
            row_groups,
            settings,
            sender: tx,
        }));

        Ok(SpillsDataReader {
            receiver: rx,
            data_schema,
            field_levels,
            read_bytes: 0,
            target,
        })
    }

    pub fn read_bytes(&self) -> usize {
        self.read_bytes
    }

    pub fn read(&mut self) -> Result<Option<DataBlock>> {
        let start = Instant::now();

        let fetched = match self.receiver.recv_blocking() {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(None),
        };

        self.read_bytes += fetched.read_bytes;

        let mut rg_core = RowGroupCore::new(fetched.metadata, None);
        rg_core.fetch(&ProjectionMask::all(), None, |_| Ok(fetched.chunks))?;

        let num_rows = rg_core.num_rows();
        let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &rg_core,
            num_rows,
            None,
        )?;
        let batch = reader.next().transpose()?.unwrap();
        debug_assert!(reader.next().is_none());
        record_read_profile(self.target, &start, fetched.read_bytes);
        Ok(Some(DataBlock::from_record_batch(
            &self.data_schema,
            &batch,
        )?))
    }
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

pub struct BufferWriterTaskOperator {
    span: Span,
    writer: Writer,
    buffer_rx: async_channel::Receiver<Bytes>,
    available_buffers: async_channel::Sender<BytesMut>,
    response: Arc<BufferOperatorResp<io::Result<Metadata>>>,
}

pub struct FetchedRowGroup {
    pub metadata: RowGroupMetaData,
    pub chunks: Vec<Bytes>,
    pub read_bytes: usize,
}

pub struct ReaderTaskOperator {
    span: Span,
    op: Operator,
    location: String,
    row_groups: Vec<RowGroupMetaData>,
    settings: ReadSettings,
    sender: async_channel::Sender<Result<FetchedRowGroup>>,
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
    WriterTask(BufferWriterTaskOperator),
    CreateWriter(CreateWriterOperator),
    Fetch(FetchOperator),
    ReaderTask(ReaderTaskOperator),
}

impl BufferOperator {
    fn span(&self) -> &Span {
        match self {
            BufferOperator::WriterTask(op) => &op.span,
            BufferOperator::CreateWriter(op) => &op.span,
            BufferOperator::Fetch(op) => &op.span,
            BufferOperator::ReaderTask(op) => &op.span,
        }
    }
}

pub struct Background;

impl Background {
    pub fn create() -> Background {
        Background
    }

    pub async fn recv(&mut self, op: BufferOperator) {
        match op {
            BufferOperator::WriterTask(op) => {
                spawn(
                    async_backtrace::location!(String::from("writer_task"))
                        .frame(async move { writer_task_loop(op).await }),
                );
            }
            BufferOperator::CreateWriter(op) => {
                spawn(
                    async_backtrace::location!(String::from("create_writer_task")).frame(
                        async move {
                            let writer = op.op.writer(&op.path).await;
                            op.response.done(writer);
                        },
                    ),
                );
            }
            BufferOperator::Fetch(op) => {
                spawn(
                    async_backtrace::location!(String::from("fetch_task")).frame(async move {
                        let res =
                            get_ranges(&op.fetch_ranges, &op.settings, &op.location, &op.op).await;
                        op.response.done(res.map(|(chunks, _)| chunks));
                    }),
                );
            }
            BufferOperator::ReaderTask(op) => {
                spawn(
                    async_backtrace::location!(String::from("reader_task"))
                        .frame(async move { reader_task_loop(op).await }),
                );
            }
        }
    }
}

async fn writer_task_loop(mut op: BufferWriterTaskOperator) {
    let mut has_error = false;
    while let Ok(buf) = op.buffer_rx.recv().await {
        if !buf.is_empty()
            && let Err(e) = op.writer.write(buf.clone()).await
        {
            has_error = true;
            op.buffer_rx.close();
            op.response.done(Err(io::Error::from(e)));
        }

        let mut release_buf = Some(buf);

        while let Some(buf) = release_buf.take() {
            let buf = match buf.try_into_mut() {
                Ok(mut b) if b.capacity() == CHUNK_SIZE => {
                    b.clear();
                    b
                }
                _ => {
                    log::warn!("Failed to recycle buffer, creating new one");
                    BytesMut::with_capacity(CHUNK_SIZE)
                }
            };

            if op.available_buffers.send(buf).await.is_err() {
                op.buffer_rx.close();
                op.response.done(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "buffer pool is closed",
                )));
                return;
            }

            if has_error {
                release_buf = op.buffer_rx.try_recv().ok();
            }
        }

        if has_error {
            return;
        }
    }

    op.response
        .done(op.writer.close().await.map_err(io::Error::from));
}

async fn reader_task_loop(op: ReaderTaskOperator) {
    for row_group_meta in op.row_groups {
        let result = async {
            let rg_core = RowGroupCore::new(row_group_meta.clone(), None);
            let ranges = rg_core.fetch_ranges(&ProjectionMask::all());
            let (chunks, _) = get_ranges(&ranges, &op.settings, &op.location, &op.op).await?;
            let read_bytes = chunks.iter().map(|c| c.len()).sum::<usize>();
            Ok(FetchedRowGroup {
                metadata: row_group_meta,
                chunks,
                read_bytes,
            })
        };

        if op.sender.send(result.await).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_base::runtime::spawn;
    use opendal::Operator;

    use super::*;

    fn create_test_operator() -> std::io::Result<Operator> {
        let builder = opendal::services::Fs::default().root("/tmp");
        Ok(Operator::new(builder)?.finish())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_pool_creation() {
        let pool = SpillsBufferPool::create(16 * 1024 * 1024, 2).unwrap();
        let buffer1 = pool.alloc_buffer();
        assert!(buffer1.is_ok());
        assert_eq!(buffer1.unwrap().capacity(), CHUNK_SIZE);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_writer_basic_write() {
        let pool = SpillsBufferPool::create(8 * 1024 * 1024, 1).unwrap();
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("test_file").await.unwrap();

        let mut buffer_writer = pool.buffer_write(writer);

        let data = b"Hello, World!";
        let written = buffer_writer.write(data).unwrap();
        assert_eq!(written, data.len());

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert!(metadata.content_length() > 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_writer_large_write() {
        let pool = SpillsBufferPool::create(16 * 1024 * 1024, 2).unwrap();
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("large_file").await.unwrap();

        let mut buffer_writer = pool.buffer_write(writer);

        let large_data = vec![0u8; 8 * 1024 * 1024];
        let written = buffer_writer.write(&large_data).unwrap();
        assert_eq!(written, large_data.len());

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), large_data.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_writer_multiple_writes() {
        let pool = SpillsBufferPool::create(8 * 1024 * 1024, 1).unwrap();
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("multi_write_file").await.unwrap();

        let mut buffer_writer = pool.buffer_write(writer);

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_buffer_pool_exhaustion_and_backpressure() {
        let pool = SpillsBufferPool::create(CHUNK_SIZE, 1).unwrap();
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("backpressure_test").await.unwrap();

        let mut buffer_writer = pool.buffer_write(writer);

        let data = vec![0u8; CHUNK_SIZE];
        let written = buffer_writer.write(&data).unwrap();
        assert_eq!(written, data.len());

        let written2 = buffer_writer.write(b"extra data").unwrap();
        assert_eq!(written2, 10);

        buffer_writer.flush().unwrap();
        let _metadata = buffer_writer.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_empty_write() {
        let pool = SpillsBufferPool::create(8 * 1024 * 1024, 1).unwrap();
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("empty_test").await.unwrap();

        let mut buffer_writer = pool.buffer_write(writer);

        let written = buffer_writer.write(b"").unwrap();
        assert_eq!(written, 0);

        buffer_writer.flush().unwrap();
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_without_writes() {
        let pool = SpillsBufferPool::create(8 * 1024 * 1024, 1).unwrap();
        let operator = create_test_operator().unwrap();
        let writer = operator.writer("no_write_test").await.unwrap();

        let buffer_writer = pool.buffer_write(writer);
        let metadata = buffer_writer.close().unwrap();
        assert_eq!(metadata.content_length(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_writers() {
        let pool = SpillsBufferPool::create(16 * 1024 * 1024, 4).unwrap();
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
                let mut buffer_writer = pool_clone.buffer_write(writer);

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

        for handle in handles {
            let _metadata = handle.await.unwrap();
        }

        assert_eq!(write_count.load(Ordering::Relaxed), 40);
    }
}
