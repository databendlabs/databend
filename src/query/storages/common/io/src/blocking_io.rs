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

//! Blocking OpenDAL I/O on the global I/O runtime; callers handle and decode returned buffers.

use std::io;
use std::ops::Range;
use std::time::Duration;
use std::time::Instant;

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::rangemap::RangeMerger;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::*;
use databend_storages_common_blocks::BlockingWrite;
use futures::TryStreamExt;
use opendal::Buffer;
use opendal::Operator;

use crate::ReadSettings;

pub const BLOCKING_WRITE_CHUNK_SIZE: usize = 4 * 1024 * 1024;
pub const BLOCKING_WRITE_MAX_CHUNKS: usize = 2;

/// Worst-case bytes retained by one blocking writer: the current producer
/// buffer, the bounded channel, and one chunk owned by the upload worker.
pub fn blocking_write_retained_bytes(max_chunks: usize) -> usize {
    BLOCKING_WRITE_CHUNK_SIZE.saturating_mul(max_chunks.max(1).saturating_add(2))
}

enum UploadCommand {
    Data(Bytes),
    Finish,
}

/// Create an OpenDAL-backed blocking writer. The upload worker starts on the first non-empty write;
/// closing an otherwise unused writer starts it only long enough to commit the empty file.
pub fn create_blocking_write(
    operator: Operator,
    path: String,
    max_chunks: usize,
) -> OpenDalBlockingWrite {
    OpenDalBlockingWrite::create(operator, path, max_chunks)
}

struct PendingWorker {
    operator: Operator,
    path: String,
    max_chunks: usize,
}

struct ActiveWorker {
    command_tx: async_channel::Sender<UploadCommand>,
    done_rx: async_channel::Receiver<Result<()>>,
}

impl ActiveWorker {
    fn send(&self, command: UploadCommand) -> io::Result<()> {
        self.command_tx
            .send_blocking(command)
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }
}

enum UploadWorkerState {
    Unopened(PendingWorker),
    Open(ActiveWorker),
    Closed,
}

/// An OpenDAL-backed blocking writer that starts its upload worker on the first non-empty write.
pub struct OpenDalBlockingWrite {
    current: Option<BytesMut>,
    bytes_written: u64,
    state: UploadWorkerState,
}

impl OpenDalBlockingWrite {
    fn create(operator: Operator, path: String, max_chunks: usize) -> Self {
        Self {
            current: Some(BytesMut::with_capacity(BLOCKING_WRITE_CHUNK_SIZE)),
            bytes_written: 0,
            state: UploadWorkerState::Unopened(PendingWorker {
                operator,
                path,
                max_chunks,
            }),
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn init_upload_worker(&mut self) -> io::Result<()> {
        let pending = match std::mem::replace(&mut self.state, UploadWorkerState::Closed) {
            UploadWorkerState::Unopened(pending) => pending,
            state @ UploadWorkerState::Open(_) => {
                self.state = state;
                return Ok(());
            }
            UploadWorkerState::Closed => {
                return Err(io::Error::from(io::ErrorKind::BrokenPipe));
            }
        };
        self.state = UploadWorkerState::Open(start_upload_worker(pending));
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        let worker = match std::mem::replace(&mut self.state, UploadWorkerState::Closed) {
            UploadWorkerState::Unopened(pending) => start_upload_worker(pending),
            UploadWorkerState::Open(worker) => worker,
            UploadWorkerState::Closed => {
                return Err(ErrorCode::Internal(
                    "OpenDAL blocking writer is already closed",
                ));
            }
        };

        if let Some(buf) = self.current.take()
            && !buf.is_empty()
        {
            worker.send(UploadCommand::Data(buf.freeze()))?;
        }
        worker.send(UploadCommand::Finish)?;
        worker.command_tx.close();
        match worker.done_rx.recv_blocking() {
            Ok(result) => result,
            Err(_) => Err(upload_task_gone()),
        }
    }

    fn send_current(&mut self, buf: BytesMut) -> io::Result<()> {
        self.init_upload_worker()?;
        let UploadWorkerState::Open(worker) = &self.state else {
            unreachable!("upload worker was not started")
        };
        worker.send(UploadCommand::Data(buf.freeze()))
    }

    #[cfg(test)]
    fn upload_worker_started(&self) -> bool {
        matches!(self.state, UploadWorkerState::Open(_))
    }
}

impl io::Write for OpenDalBlockingWrite {
    fn write(&mut self, mut remaining: &[u8]) -> io::Result<usize> {
        if matches!(self.state, UploadWorkerState::Closed) {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }

        let total = remaining.len();
        if total > 0 {
            self.init_upload_worker()?;
        }
        while !remaining.is_empty() {
            let mut current = self
                .current
                .take()
                .unwrap_or_else(|| BytesMut::with_capacity(BLOCKING_WRITE_CHUNK_SIZE));
            let space = BLOCKING_WRITE_CHUNK_SIZE - current.len();
            let take = space.min(remaining.len());
            current.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];

            if current.len() == BLOCKING_WRITE_CHUNK_SIZE {
                self.send_current(current)?;
            } else {
                self.current = Some(current);
            }
        }

        self.bytes_written += total as u64;
        Ok(total)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BlockingWrite for OpenDalBlockingWrite {
    fn close(&mut self) -> Result<()> {
        OpenDalBlockingWrite::close(self)
    }
}

impl Drop for OpenDalBlockingWrite {
    fn drop(&mut self) {
        if let UploadWorkerState::Open(worker) = &self.state {
            worker.command_tx.close();
        }
    }
}

fn start_upload_worker(pending: PendingWorker) -> ActiveWorker {
    let (command_tx, command_rx) =
        async_channel::bounded::<UploadCommand>(pending.max_chunks.max(1));
    let (done_tx, done_rx) = async_channel::bounded::<Result<()>>(1);

    GlobalIORuntime::instance().spawn(async move {
        let result = match pending.operator.writer(&pending.path).await {
            Ok(writer) => upload_loop(writer, command_rx.clone()).await,
            Err(e) => Err(e.into()),
        };
        command_rx.close();
        let _ = done_tx.send(result).await;
    });

    ActiveWorker {
        command_tx,
        done_rx,
    }
}

fn upload_task_gone() -> ErrorCode {
    ErrorCode::Internal("OpenDAL blocking writer background task exited unexpectedly")
}

async fn upload_loop(
    mut writer: opendal::Writer,
    command_rx: async_channel::Receiver<UploadCommand>,
) -> Result<()> {
    loop {
        match command_rx.recv().await {
            Ok(UploadCommand::Data(chunk)) => {
                if let Err(error) = writer.write(chunk).await {
                    let _ = writer.abort().await;
                    return Err(error.into());
                }
            }
            Ok(UploadCommand::Finish) => {
                if let Err(error) = writer.close().await {
                    let _ = writer.abort().await;
                    return Err(error.into());
                }
                return Ok(());
            }
            Err(_) => {
                writer.abort().await?;
                return Err(ErrorCode::Internal(
                    "OpenDAL blocking writer was dropped without finishing",
                ));
            }
        }
    }
}

struct MergedRangeRead {
    index: usize,
    data: Buffer,
    io_time: Duration,
    #[cfg(test)]
    io_thread: std::thread::ThreadId,
}

#[derive(Debug, PartialEq, Eq)]
struct StreamRangeGroup {
    range: Range<u64>,
    merged_range_indices: Range<usize>,
}

fn group_contiguous_ranges(ranges: &[Range<u64>]) -> Vec<StreamRangeGroup> {
    let Some(first) = ranges.first() else {
        return Vec::new();
    };

    let mut groups = Vec::new();
    let mut group_start = 0;
    let mut group_range = first.clone();
    for (index, range) in ranges.iter().enumerate().skip(1) {
        if group_range.end == range.start {
            group_range.end = range.end;
        } else {
            groups.push(StreamRangeGroup {
                range: group_range,
                merged_range_indices: group_start..index,
            });
            group_start = index;
            group_range = range.clone();
        }
    }
    groups.push(StreamRangeGroup {
        range: group_range,
        merged_range_indices: group_start..ranges.len(),
    });
    groups
}

fn take_transport_bytes(input: &mut Buffer, output: &mut Vec<Bytes>, remaining: &mut usize) {
    while *remaining > 0 && input.has_remaining() {
        let current = input.current();
        let take = current.len().min(*remaining);
        output.push(current.slice(..take));
        input.advance(take);
        *remaining -= take;
    }
}

async fn read_stream_groups(
    op: Operator,
    path: String,
    merged_ranges: Vec<Range<u64>>,
    stream_groups: Vec<StreamRangeGroup>,
    sender: async_channel::Sender<Result<MergedRangeRead>>,
) -> Result<()> {
    for group in stream_groups {
        let reader = op.reader(&path).await?;
        let mut stream = reader.into_stream(group.range.clone()).await?;
        let last_index = group.merged_range_indices.end - 1;
        let mut transport = Buffer::new();
        let mut io_time = Duration::ZERO;

        for index in group.merged_range_indices {
            let range = &merged_ranges[index];
            let mut remaining = usize::try_from(range.end - range.start).map_err(|_| {
                ErrorCode::Internal(format!(
                    "merged range {range:?} length does not fit usize for {path}"
                ))
            })?;
            let expected = remaining;
            let mut output = Vec::new();

            while remaining > 0 {
                take_transport_bytes(&mut transport, &mut output, &mut remaining);
                if remaining == 0 {
                    break;
                }

                let start = Instant::now();
                let next = stream.try_next().await;
                io_time += start.elapsed();
                transport = next?.ok_or_else(|| {
                    ErrorCode::StorageOther(format!(
                        "OpenDAL range stream for {path} ended after {} of {expected} bytes for merged range {range:?}",
                        expected - remaining
                    ))
                })?;
            }

            let data = Buffer::from(output);
            if index == last_index {
                if !transport.is_empty() {
                    return Err(ErrorCode::StorageOther(format!(
                        "OpenDAL range stream for {path} returned more data than requested for {:?}",
                        group.range
                    )));
                }
                let start = Instant::now();
                let trailing = stream.try_next().await;
                io_time += start.elapsed();
                if let Some(trailing) = trailing?
                    && !trailing.is_empty()
                {
                    return Err(ErrorCode::StorageOther(format!(
                        "OpenDAL range stream for {path} returned more data than requested for {:?}",
                        group.range
                    )));
                }
            }

            let read = MergedRangeRead {
                index,
                data,
                io_time: std::mem::take(&mut io_time),
                #[cfg(test)]
                io_thread: std::thread::current().id(),
            };
            if sender.send(Ok(read)).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Reads merged OpenDAL ranges and returns buffers in the original input order.
/// Empty ranges return empty buffers without I/O.
pub struct OperatorRangeReader {
    receiver: async_channel::Receiver<Result<MergedRangeRead>>,
    chunks_by_io: Vec<Vec<(usize, Range<usize>)>>,
    chunks: Vec<Option<Buffer>>,
    next_chunk: usize,
}

impl OperatorRangeReader {
    pub fn create(
        settings: &ReadSettings,
        op: Operator,
        path: String,
        ranges: &[Range<u64>],
        max_prefetch: usize,
    ) -> Result<Self> {
        let range_merger = RangeMerger::from_iter(
            ranges.iter().filter(|range| !range.is_empty()).cloned(),
            settings.max_gap_size,
            settings.max_range_size,
        );
        let merged_ranges = range_merger.ranges();
        let stream_groups = group_contiguous_ranges(&merged_ranges);
        let mut chunks_by_io = vec![Vec::new(); merged_ranges.len()];
        let mut chunks = std::iter::repeat_with(|| None)
            .take(ranges.len())
            .collect::<Vec<_>>();
        for (index, range) in ranges.iter().enumerate() {
            if range.is_empty() {
                chunks[index] = Some(Buffer::new());
                continue;
            }
            let (io_index, merged_range) = range_merger.get(range.clone()).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "range {range:?} not found in merged ranges {merged_ranges:?} for {path}"
                ))
            })?;
            chunks_by_io[io_index].push((
                index,
                (range.start - merged_range.start) as usize
                    ..(range.end - merged_range.start) as usize,
            ));
        }
        for group in &stream_groups {
            metrics_inc_remote_io_seeks_after_merged(1);
            metrics_inc_remote_io_read_bytes_after_merged(group.range.end - group.range.start);
        }

        let (sender, receiver) = async_channel::bounded(max_prefetch.max(1));
        GlobalIORuntime::instance().spawn(async move {
            let result =
                read_stream_groups(op, path, merged_ranges, stream_groups, sender.clone()).await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });

        Ok(Self {
            receiver,
            chunks_by_io,
            chunks,
            next_chunk: 0,
        })
    }

    pub fn read(&mut self) -> Result<Buffer> {
        if self.next_chunk >= self.chunks.len() {
            return Err(ErrorCode::Internal(
                "operator range reader has no remaining ranges",
            ));
        }

        while self.chunks[self.next_chunk].is_none() {
            let read = match self.receiver.recv_blocking() {
                Ok(result) => result?,
                Err(_) => {
                    return Err(ErrorCode::Internal(
                        "operator range reader ended before all ranges were returned",
                    ));
                }
            };
            metrics_inc_remote_io_read_milliseconds(read.io_time.as_millis() as u64);
            let chunk_slices = self.chunks_by_io.get_mut(read.index).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "operator range reader returned unexpected I/O index {}",
                    read.index
                ))
            })?;
            for (index, range) in std::mem::take(chunk_slices) {
                self.chunks[index] = Some(read.data.slice(range));
            }
        }

        let data = self.chunks[self.next_chunk].take().expect("checked above");
        self.next_chunk += 1;
        Ok(data)
    }
}

impl Drop for OperatorRangeReader {
    fn drop(&mut self) {
        self.receiver.close();
    }
}

#[cfg(test)]
pub(crate) fn init_test_runtime() {
    use std::sync::Once;

    use databend_common_base::base::GlobalInstance;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        GlobalInstance::init_production();
        GlobalIORuntime::init(2).unwrap();
    });
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;

    use opendal::OperatorBuilder;
    use opendal::raw::Access;
    use opendal::raw::AccessorInfo;
    use opendal::raw::OpRead;
    use opendal::raw::RpRead;
    use opendal::services::Memory;

    use super::*;

    #[derive(Debug)]
    struct RecordingReadAccessor {
        content: Bytes,
        read_ranges: Mutex<Vec<Range<u64>>>,
        short_response: bool,
    }

    impl RecordingReadAccessor {
        fn new(content: &'static [u8], short_response: bool) -> Arc<Self> {
            Arc::new(Self {
                content: Bytes::from_static(content),
                read_ranges: Mutex::new(Vec::new()),
                short_response,
            })
        }

        fn read_ranges(&self) -> Vec<Range<u64>> {
            self.read_ranges.lock().unwrap().clone()
        }
    }

    impl Access for RecordingReadAccessor {
        type Reader = Buffer;
        type Writer = ();
        type Lister = ();
        type Deleter = ();

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                read: true,
                ..Default::default()
            });
            info.into()
        }

        async fn read(&self, _path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            let range = args.range();
            let start = range.offset();
            let requested = range.size().unwrap_or(self.content.len() as u64 - start);
            let end = start + requested;
            self.read_ranges.lock().unwrap().push(start..end);

            let mut actual_end = end.min(self.content.len() as u64);
            if self.short_response && actual_end > start {
                actual_end -= 1;
            }
            let data = self.content.slice(start as usize..actual_end as usize);
            Ok((RpRead::new(), Buffer::from(data)))
        }
    }

    fn memory_operator() -> Operator {
        Operator::new(Memory::default()).unwrap().finish()
    }

    fn settings() -> ReadSettings {
        ReadSettings {
            max_gap_size: 16,
            max_range_size: 1024,
            parquet_fast_read_bytes: 0,
        }
    }

    #[test]
    fn test_group_contiguous_merged_ranges_without_crossing_gaps() {
        assert_eq!(
            group_contiguous_ranges(&[0..4, 4..8, 8..10, 12..16, 16..20]),
            vec![
                StreamRangeGroup {
                    range: 0..10,
                    merged_range_indices: 0..3,
                },
                StreamRangeGroup {
                    range: 12..20,
                    merged_range_indices: 3..5,
                },
            ]
        );
        assert!(group_contiguous_ranges(&[]).is_empty());
    }

    #[test]
    fn test_take_transport_bytes_spans_transport_buffers() {
        let mut output = Vec::new();
        let mut remaining = 5;
        let mut first = Buffer::from(vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]);
        take_transport_bytes(&mut first, &mut output, &mut remaining);
        assert_eq!(remaining, 1);
        assert!(first.is_empty());

        let mut second = Buffer::from(Bytes::from_static(b"efgh"));
        take_transport_bytes(&mut second, &mut output, &mut remaining);
        assert_eq!(remaining, 0);
        assert_eq!(Buffer::from(output).to_bytes().as_ref(), b"abcde");
        assert_eq!(second.to_bytes().as_ref(), b"fgh");
    }

    #[test]
    fn test_contiguous_merged_ranges_keep_channel_boundaries() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = OperatorBuilder::new(accessor.clone()).finish();
        let split_settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 4,
            parquet_fast_read_bytes: 0,
        };
        let reader = OperatorRangeReader::create(
            &split_settings,
            op,
            "stream-group".to_string(),
            &[0..4, 4..8, 8..10],
            3,
        )
        .unwrap();

        assert_eq!(reader.chunks_by_io.len(), 3);
        let reads = (0..3)
            .map(|_| reader.receiver.recv_blocking().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(reads.iter().map(|read| read.index).collect::<Vec<_>>(), [
            0, 1, 2
        ]);
        assert_eq!(reads[0].data.to_bytes().as_ref(), b"0123");
        assert_eq!(reads[1].data.to_bytes().as_ref(), b"4567");
        assert_eq!(reads[2].data.to_bytes().as_ref(), b"89");
        assert_eq!(accessor.read_ranges(), vec![0..10]);
    }

    #[test]
    fn test_non_contiguous_merged_ranges_use_separate_streams() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = OperatorBuilder::new(accessor.clone()).finish();
        let split_settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 4,
            parquet_fast_read_bytes: 0,
        };
        let mut reader = OperatorRangeReader::create(
            &split_settings,
            op,
            "stream-gaps".to_string(),
            &[0..4, 6..10],
            2,
        )
        .unwrap();

        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"0123");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"6789");
        assert_eq!(accessor.read_ranges(), vec![0..4, 6..10]);
    }

    #[test]
    fn test_short_stream_does_not_emit_incomplete_merged_range() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"01234567", true);
        let op = OperatorBuilder::new(accessor.clone()).finish();
        let split_settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 4,
            parquet_fast_read_bytes: 0,
        };
        let mut reader = OperatorRangeReader::create(
            &split_settings,
            op,
            "short-stream".to_string(),
            &[0..4, 4..8],
            2,
        )
        .unwrap();

        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"0123");
        let error = reader.read().unwrap_err();
        assert!(
            error.message().contains("reader got too little data"),
            "unexpected short-stream error: {error:?}"
        );
        assert_eq!(accessor.read_ranges(), vec![0..8]);
    }

    #[test]
    fn test_writer_starts_upload_worker_lazily() {
        init_test_runtime();
        let op = memory_operator();
        let mut writer = create_blocking_write(op, "lazy-worker".to_string(), 1);

        assert!(!writer.upload_worker_started());
        writer.flush().unwrap();
        assert!(!writer.upload_worker_started());
        assert_eq!(writer.write(&[]).unwrap(), 0);
        assert!(!writer.upload_worker_started());

        writer.write_all(b"x").unwrap();
        assert!(writer.upload_worker_started());
        writer.close().unwrap();
    }

    #[test]
    fn test_close_unopened_writer_commits_empty_file() {
        init_test_runtime();
        let op = memory_operator();
        let path = "empty-blocking-writer".to_string();
        let mut writer = create_blocking_write(op.clone(), path.clone(), 1);

        assert!(!writer.upload_worker_started());
        writer.close().unwrap();
        assert_eq!(writer.bytes_written(), 0);

        let data = GlobalIORuntime::instance()
            .block_on(async { op.read(&path).await.map_err(ErrorCode::from) })
            .unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_writer_and_range_reader_roundtrip() {
        init_test_runtime();
        let op = memory_operator();
        let path = "blocking-io-roundtrip".to_string();
        let mut writer = create_blocking_write(op.clone(), path.clone(), 1);
        writer.write_all(b"0123456789abcdef").unwrap();
        writer.close().unwrap();
        assert_eq!(writer.bytes_written(), 16);
        assert!(writer.write_all(b"after close").is_err());

        let consumer_thread = thread::current().id();
        let unordered_settings = ReadSettings {
            max_gap_size: 1,
            ..settings()
        };
        let byte_ranges = [12..16, 4..4, 0..4, 6..10];
        let mut reader =
            OperatorRangeReader::create(&unordered_settings, op, path, &byte_ranges, 1).unwrap();
        assert_eq!(reader.chunks_by_io.len(), 3);
        let result = byte_ranges
            .iter()
            .map(|_| reader.read().unwrap().to_bytes())
            .collect::<Vec<_>>();
        assert!(reader.read().is_err());
        assert_eq!(thread::current().id(), consumer_thread);
        assert_eq!(result[0].as_ref(), b"cdef");
        assert!(result[1].is_empty());
        assert_eq!(result[2].as_ref(), b"0123");
        assert_eq!(result[3].as_ref(), b"6789");
    }

    #[test]
    fn test_one_io_populates_multiple_chunks() {
        init_test_runtime();
        let op = memory_operator();
        GlobalIORuntime::instance()
            .block_on(async {
                op.write("merged", b"0123456789abcdef".to_vec())
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();
        let mut reader = OperatorRangeReader::create(
            &settings(),
            op,
            "merged".into(),
            &[0..4, 6..10, 12..16],
            1,
        )
        .unwrap();
        assert_eq!(reader.chunks_by_io.len(), 1);
        assert_eq!(reader.chunks_by_io[0].len(), 3);

        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"0123");
        assert!(reader.chunks_by_io[0].is_empty());
        assert!(reader.chunks[1].is_some());
        assert!(reader.chunks[2].is_some());
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"6789");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"cdef");
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_range_reader_propagates_io_error() {
        init_test_runtime();
        let mut reader = OperatorRangeReader::create(
            &settings(),
            memory_operator(),
            "missing".to_string(),
            &[0..1],
            1,
        )
        .unwrap();
        assert!(reader.read().is_err());
    }

    #[test]
    fn test_range_reader_is_bounded_and_can_be_cancelled() {
        init_test_runtime();
        let op = memory_operator();
        GlobalIORuntime::instance()
            .block_on(async {
                op.write("bounded", vec![0_u8; 32])
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();
        let reader = OperatorRangeReader::create(
            &settings(),
            op,
            "bounded".to_string(),
            &(0..32).map(|i| i..i + 1).collect::<Vec<_>>(),
            1,
        )
        .unwrap();
        thread::sleep(Duration::from_millis(20));
        assert!(reader.receiver.len() <= 1);
        drop(reader);
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_io_and_consumer_run_on_different_threads() {
        init_test_runtime();
        let op = memory_operator();
        GlobalIORuntime::instance()
            .block_on(async {
                op.write("threads", b"data".to_vec())
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();
        let consumer = thread::current().id();
        let reader =
            OperatorRangeReader::create(&settings(), op, "threads".into(), &[0..4], 1).unwrap();
        let read = reader.receiver.recv_blocking().unwrap().unwrap();
        assert_ne!(read.io_thread, consumer);
    }
}
