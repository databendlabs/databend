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

//! A sync `io::Write` sink that streams bytes to an opendal object as they are produced, instead
//! of buffering the whole object in memory. Bytes are batched into fixed-size chunks and shipped
//! over a bounded channel to a background task (on the global IO runtime) that awaits the opendal
//! `Writer`. The bounded channel provides backpressure: once `max_chunks` chunks are in flight the
//! producing thread blocks in `write`, capping memory. Drive a low-level `BulkParquetFileWriter`
//! into this sink to upload a payload parquet page-by-page.

use std::io;

use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use opendal::Operator;

const CHUNK_SIZE: usize = 4 * 1024 * 1024;

enum UploadCommand {
    Data(Bytes),
    Finish,
}

/// A sync sink that forwards written bytes to an opendal object on a background task.
///
/// `Default` is implemented so the sink can be moved out of a finished `BulkParquetFileWriter` via
/// `mem::take`; the default is an inert, finalized sink that must never be written to.
pub struct StreamingUploadSink {
    current: Option<BytesMut>,
    bytes_written: u64,
    // Frozen chunks travel to the background upload task. Bounded => backpressure.
    command_tx: async_channel::Sender<UploadCommand>,
    // Resolves once the background task has closed or aborted the opendal writer.
    done_rx: async_channel::Receiver<Result<()>>,
    finalized: bool,
}

impl StreamingUploadSink {
    /// Start a background task that opens `path` on `op` and uploads chunks streamed to it.
    /// `max_chunks` bounds the in-flight chunk count (each `CHUNK_SIZE`), so peak queued memory is
    /// `max_chunks * CHUNK_SIZE`. Any open or upload error surfaces in `write` or [`Self::close`].
    pub fn create(op: Operator, path: String, max_chunks: usize) -> Self {
        let (command_tx, command_rx) = async_channel::bounded::<UploadCommand>(max_chunks.max(1));
        let (done_tx, done_rx) = async_channel::bounded::<Result<()>>(1);

        GlobalIORuntime::instance().spawn(async move {
            let result = match op.writer(&path).await {
                Ok(writer) => upload_loop(writer, command_rx.clone()).await,
                Err(e) => Err(e.into()),
            };
            // Ensure a blocked producer wakes and observes the failed background task.
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

    /// Total bytes accepted by this sink so far (including still-buffered bytes).
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Flush the tail chunk, explicitly finish the stream, and wait until the opendal writer has
    /// committed the object. Dropping the sink without calling this method aborts the upload.
    pub fn close(mut self) -> Result<u64> {
        if let Some(buf) = self.current.take() {
            if !buf.is_empty() {
                self.command_tx
                    .send_blocking(UploadCommand::Data(buf.freeze()))
                    .map_err(|_| upload_task_gone())?;
            }
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

impl io::Write for StreamingUploadSink {
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
                // Full chunk: ship it (blocks if the channel is full => backpressure).
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

impl Drop for StreamingUploadSink {
    fn drop(&mut self) {
        if !self.finalized {
            // Only an explicit `Finish` commits the object. Closing the command channel makes the
            // background task abort after draining the bounded amount of already queued data.
            self.command_tx.close();
        }
    }
}

impl Default for StreamingUploadSink {
    fn default() -> Self {
        // An inert finalized sink, only used as the `mem::take` placeholder after file finish.
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
    ErrorCode::Internal("streaming upload background task exited unexpectedly")
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
                    "streaming upload producer exited without finishing",
                ));
            }
        }
    }
}
