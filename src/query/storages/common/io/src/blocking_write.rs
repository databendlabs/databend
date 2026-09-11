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

//! Blocking OpenDAL writes: callers produce bytes on a blocking thread while an
//! upload worker on the global I/O runtime streams them to storage.

use std::io;

use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_blocks::BlockingWrite;
use opendal::Operator;

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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::init_test_runtime;

    fn memory_operator() -> Operator {
        Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish()
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
    fn test_writer_roundtrip() {
        init_test_runtime();
        let op = memory_operator();
        let path = "blocking-writer-roundtrip".to_string();
        let mut writer = create_blocking_write(op.clone(), path.clone(), 1);
        writer.write_all(b"0123456789abcdef").unwrap();
        writer.close().unwrap();
        assert_eq!(writer.bytes_written(), 16);
        assert!(writer.write_all(b"after close").is_err());

        let data = GlobalIORuntime::instance()
            .block_on(async { op.read(&path).await.map_err(ErrorCode::from) })
            .unwrap();
        assert_eq!(data.to_bytes().as_ref(), b"0123456789abcdef");
    }
}
