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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;

use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use opendal::Writer;

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
pub struct BufferPool {
    working_queue: async_channel::Sender<BufferOperator>,
    available_write_buffers: async_channel::Receiver<BytesMut>,
    available_write_buffers_tx: async_channel::Sender<BytesMut>,
}

impl BufferPool {
    #[allow(dead_code)]
    pub fn create(executor: Arc<Runtime>, memory: usize, workers: usize) -> Arc<BufferPool> {
        let (working_tx, working_rx) = async_channel::unbounded();
        let (buffers_tx, buffers_rx) = async_channel::unbounded();

        const CHUNK_SIZE: usize = 4 * 1024 * 1024;
        let memory = memory / CHUNK_SIZE * CHUNK_SIZE;

        for _ in 0..memory / CHUNK_SIZE {
            if buffers_tx
                .try_send(BytesMut::with_capacity(CHUNK_SIZE))
                .is_err()
            {
                panic!("Buffer pool available_write_buffers need unbounded.")
            }
        }

        for _ in 0..workers {
            let working_queue = working_rx.clone();
            let available_write_buffers = buffers_tx.clone();
            executor.spawn(async move {
                let mut background = Background::create(available_write_buffers);
                while let Ok(op) = working_queue.recv().await {
                    background.recv(op).await;
                }
            });
        }

        Arc::new(BufferPool {
            working_queue: working_tx,
            available_write_buffers: buffers_rx,
            available_write_buffers_tx: buffers_tx,
        })
    }
    pub fn try_alloc_buffer(&self) -> Option<BytesMut> {
        self.available_write_buffers.try_recv().ok()
    }

    pub fn alloc_buffer(&self) -> std::io::Result<BytesMut> {
        match self.available_write_buffers.recv_blocking() {
            Ok(buf) => Ok(buf),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "buffer pool is closed",
            )),
        }
    }

    pub fn write(&self, op: BufferWriteOperator) {
        if let Err(_) = self.working_queue.try_send(BufferOperator::Write(op)) {
            unreachable!("Buffer pool working queue need unbounded.");
        }
    }

    #[allow(dead_code)]
    pub fn buffer_write(self: &Arc<BufferPool>, writer: Writer) -> BufferWriter {
        BufferWriter::new(writer, self.clone())
    }

    pub fn release_buffer(&self, buffer: BytesMut) {
        if self.available_write_buffers_tx.try_send(buffer).is_err() {
            unreachable!("Buffer pool available_write_buffers need unbounded.");
        }
    }
}

pub struct BufferWriter {
    writer: Option<Writer>,

    current_bytes: Option<BytesMut>,

    buffer_pool: Arc<BufferPool>,
    pending_buffers: VecDeque<Bytes>,
    pending_response: Option<Arc<BufferOperatorResp<BufferWriteResp>>>,
}

impl BufferWriter {
    pub fn new(writer: Writer, buffer_pool: Arc<BufferPool>) -> BufferWriter {
        BufferWriter {
            buffer_pool,
            writer: Some(writer),
            current_bytes: None,
            pending_buffers: VecDeque::new(),
            pending_response: None,
        }
    }

    fn write_buffer(&mut self, wait: bool) -> std::io::Result<()> {
        if let Some(pending_response) = self.pending_response.as_ref() {
            let locked = pending_response.mutex.lock();
            let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

            if let Some(mut response) = locked.take() {
                self.writer = Some(response.writer);

                if let Some(last_error) = response.error.take() {
                    return Err(last_error);
                }
            } else if wait {
                let waited = pending_response.condvar.wait(locked);
                let mut waited = waited.unwrap_or_else(PoisonError::into_inner);
                let mut response = waited.take().unwrap();
                self.writer = Some(response.writer);

                if let Some(last_error) = response.error.take() {
                    return Err(last_error);
                }
            }
        }

        if let Some(writer) = self.writer.take() {
            assert!(self.pending_response.is_none());

            let pending_response = Arc::new(BufferOperatorResp {
                mutex: Mutex::new(None),
                condvar: Default::default(),
            });

            self.pending_response = Some(pending_response.clone());

            self.buffer_pool.write(BufferWriteOperator {
                writer,
                response: pending_response,
                buffers: std::mem::take(&mut self.pending_buffers),
            });
        }

        Ok(())
    }
}

impl std::io::Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
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

    fn flush(&mut self) -> std::io::Result<()> {
        if matches!(&self.current_bytes, Some(current_bytes) if !current_bytes.is_empty()) {
            if let Some(current_bytes) = self.current_bytes.take() {
                self.pending_buffers.push_back(current_bytes.freeze());
            }
        }

        if !self.pending_buffers.is_empty() {
            self.write_buffer(true)?;

            if let Some(pending_response) = self.pending_response.take() {
                let locked = pending_response.mutex.lock();
                let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

                if locked.is_none() {
                    let waited = pending_response.condvar.wait(locked);
                    locked = waited.unwrap_or_else(PoisonError::into_inner);
                }

                let mut response = locked.take().unwrap();
                self.writer = Some(response.writer);

                if let Some(error) = response.error.take() {
                    return Err(error);
                }
            }
        }

        Ok(())
    }
}

impl Drop for BufferWriter {
    fn drop(&mut self) {
        let pending_buffers = std::mem::take(&mut self.pending_buffers);

        for mut pending_buffer in pending_buffers {
            let Ok(mut pending_buffer) = pending_buffer.try_into_mut() else {
                unreachable!("Bytes must be unique ref");
            };

            pending_buffer.clear();
            self.buffer_pool.release_buffer(pending_buffer);
        }

        if let Some(mut current_bytes) = self.current_bytes.take() {
            current_bytes.clear();
            self.buffer_pool.release_buffer(current_bytes);
        }
        // self
    }
}

pub struct BufferWriteResp {
    writer: Writer,
    error: Option<std::io::Error>,
}

pub struct BufferWriteOperator {
    writer: Writer,
    buffers: VecDeque<Bytes>,
    response: Arc<BufferOperatorResp<BufferWriteResp>>,
}

pub struct BufferOperatorResp<T> {
    condvar: Condvar,
    mutex: Mutex<Option<T>>,
}

pub enum BufferOperator {
    Write(BufferWriteOperator),
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
                let bytes = op.buffers.clone();
                let mut error = op
                    .writer
                    .write(op.buffers)
                    .await
                    .map_err(|err| std::io::Error::from(err))
                    .err();
                for bytes in bytes {
                    let Ok(mut bytes) = bytes.try_into_mut() else {
                        unreachable!("Bytes must be unique ref");
                    };

                    bytes.clear();

                    if self.available_buffers.send(bytes).await.is_err() && error.is_none() {
                        error = Some(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "buffer pool is closed",
                        ));
                    }
                }

                let locked = op.response.mutex.lock();
                let mut locked = locked.unwrap_or_else(PoisonError::into_inner);

                *locked = Some(BufferWriteResp {
                    error,
                    writer: op.writer,
                });

                op.response.condvar.notify_one();
            }
        }
    }
}
