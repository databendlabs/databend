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

use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::rangemap::RangeMerger;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::*;
use opendal::Buffer;
use opendal::Operator;

use crate::ReadSettings;

const CHUNK_SIZE: usize = 4 * 1024 * 1024;

enum UploadCommand {
    Data(Bytes),
    Finish,
}

/// A blocking writer backed by OpenDAL.
pub struct BlockingOperatorWriter {
    current: Option<BytesMut>,
    bytes_written: u64,
    command_tx: async_channel::Sender<UploadCommand>,
    done_rx: async_channel::Receiver<Result<()>>,
    finalized: bool,
}

impl BlockingOperatorWriter {
    pub fn create(op: Operator, path: String, max_chunks: usize) -> Self {
        let (command_tx, command_rx) = async_channel::bounded::<UploadCommand>(max_chunks.max(1));
        let (done_tx, done_rx) = async_channel::bounded::<Result<()>>(1);

        GlobalIORuntime::instance().spawn(async move {
            let result = match op.writer(&path).await {
                Ok(writer) => upload_loop(writer, command_rx.clone()).await,
                Err(e) => Err(e.into()),
            };
            command_rx.close();
            let _ = done_tx.send(result).await;
        });

        Self {
            current: Some(BytesMut::with_capacity(CHUNK_SIZE)),
            bytes_written: 0,
            command_tx,
            done_rx,
            finalized: false,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn close(mut self) -> Result<u64> {
        if let Some(buf) = self.current.take()
            && !buf.is_empty()
        {
            self.command_tx
                .send_blocking(UploadCommand::Data(buf.freeze()))
                .map_err(|_| upload_task_gone())?;
        }
        self.command_tx
            .send_blocking(UploadCommand::Finish)
            .map_err(|_| upload_task_gone())?;
        self.finalized = true;
        self.command_tx.close();
        match self.done_rx.recv_blocking() {
            Ok(result) => result.map(|()| self.bytes_written),
            Err(_) => Err(upload_task_gone()),
        }
    }

    fn send_current(&mut self, buf: BytesMut) -> io::Result<()> {
        self.command_tx
            .send_blocking(UploadCommand::Data(buf.freeze()))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }
}

impl io::Write for BlockingOperatorWriter {
    fn write(&mut self, mut remaining: &[u8]) -> io::Result<usize> {
        if self.finalized {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }

        let total = remaining.len();
        while !remaining.is_empty() {
            let mut current = self
                .current
                .take()
                .unwrap_or_else(|| BytesMut::with_capacity(CHUNK_SIZE));
            let space = CHUNK_SIZE - current.len();
            let take = space.min(remaining.len());
            current.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];

            if current.len() == CHUNK_SIZE {
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

impl Drop for BlockingOperatorWriter {
    fn drop(&mut self) {
        if !self.finalized {
            self.command_tx.close();
        }
    }
}

impl Default for BlockingOperatorWriter {
    fn default() -> Self {
        let (command_tx, command_rx) = async_channel::bounded::<UploadCommand>(1);
        let (_done_tx, done_rx) = async_channel::bounded::<Result<()>>(1);
        command_rx.close();
        Self {
            current: None,
            bytes_written: 0,
            command_tx,
            done_rx,
            finalized: true,
        }
    }
}

fn upload_task_gone() -> ErrorCode {
    ErrorCode::Internal("blocking operator writer background task exited unexpectedly")
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
                    "blocking operator writer was dropped without finishing",
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
        for range in &merged_ranges {
            metrics_inc_remote_io_seeks_after_merged(1);
            metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
        }

        let (sender, receiver) = async_channel::bounded(max_prefetch.max(1));
        GlobalIORuntime::instance().spawn(async move {
            for (index, range) in merged_ranges.into_iter().enumerate() {
                let start = Instant::now();
                let result = op
                    .read_with(&path)
                    .range(range)
                    .await
                    .map(|data| MergedRangeRead {
                        index,
                        data,
                        io_time: start.elapsed(),
                        #[cfg(test)]
                        io_thread: std::thread::current().id(),
                    });
                let failed = result.is_err();
                if sender.send(result.map_err(Into::into)).await.is_err() || failed {
                    break;
                }
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
    use std::thread;
    use std::time::Duration;

    use opendal::services::Memory;

    use super::*;

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
    fn test_writer_and_range_reader_roundtrip() {
        init_test_runtime();
        let op = memory_operator();
        let path = "blocking-io-roundtrip".to_string();
        let mut writer = BlockingOperatorWriter::create(op.clone(), path.clone(), 1);
        writer.write_all(b"0123456789abcdef").unwrap();
        assert_eq!(writer.close().unwrap(), 16);

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
