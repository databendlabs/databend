// Copyright 2022 Datafuse Labs.
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
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use futures::AsyncRead;
use opendal::raw::input;
use opendal::raw::output;
use opendal::raw::Accessor;
use opendal::raw::Layer;
use opendal::raw::LayeredAccessor;
use opendal::raw::RpRead;
use opendal::raw::RpWrite;
use opendal::OpRead;
use opendal::OpWrite;
use opendal::Result;
use parking_lot::RwLock;

/// StorageMetrics represents the metrics of storage (all bytes metrics are compressed size).
#[derive(Debug, Default)]
pub struct StorageMetrics {
    /// Read bytes.
    read_bytes: AtomicUsize,
    /// Cost(in ms) of read bytes.
    read_bytes_cost_ms: AtomicU64,
    /// Bytes written by data access layer
    write_bytes: AtomicUsize,
    /// Cost(in ms) of write bytes.
    write_bytes_cost_ms: AtomicU64,
    /// Number of partitions scanned, after pruning
    partitions_scanned: AtomicU64,
    /// Number of partitions, before pruning
    partitions_total: AtomicU64,
    /// Status of the operation.
    status: Arc<RwLock<String>>,
}

impl StorageMetrics {
    /// Merge give metrics into one.
    pub fn merge(vs: &[impl AsRef<StorageMetrics>]) -> StorageMetrics {
        StorageMetrics {
            read_bytes: AtomicUsize::new(vs.iter().map(|v| v.as_ref().get_read_bytes()).sum()),
            read_bytes_cost_ms: AtomicU64::new(
                vs.iter().map(|v| v.as_ref().get_read_bytes_cost()).sum(),
            ),
            write_bytes: AtomicUsize::new(vs.iter().map(|v| v.as_ref().get_write_bytes()).sum()),
            write_bytes_cost_ms: AtomicU64::new(
                vs.iter().map(|v| v.as_ref().get_write_bytes_cost()).sum(),
            ),
            partitions_scanned: AtomicU64::new(
                vs.iter().map(|v| v.as_ref().get_partitions_scanned()).sum(),
            ),
            partitions_total: AtomicU64::new(
                vs.iter().map(|v| v.as_ref().get_partitions_total()).sum(),
            ),
            // Get the last one status, mainly used for the single table operation.
            status: Arc::new(RwLock::new(
                vs.iter()
                    .map(|v| v.as_ref().get_status())
                    .collect::<Vec<String>>()
                    .join("|"),
            )),
        }
    }

    pub fn inc_read_bytes(&self, v: usize) {
        if v > 0 {
            self.read_bytes.fetch_add(v, Ordering::Relaxed);
        }
    }

    pub fn get_read_bytes(&self) -> usize {
        self.read_bytes.load(Ordering::Relaxed)
    }

    pub fn inc_read_bytes_cost(&self, ms: u64) {
        if ms > 0 {
            self.read_bytes_cost_ms.fetch_add(ms, Ordering::Relaxed);
        }
    }

    pub fn inc_write_bytes_cost(&self, ms: u64) {
        if ms > 0 {
            self.write_bytes_cost_ms.fetch_add(ms, Ordering::Relaxed);
        }
    }

    pub fn get_read_bytes_cost(&self) -> u64 {
        self.read_bytes_cost_ms.load(Ordering::Relaxed)
    }

    pub fn get_write_bytes_cost(&self) -> u64 {
        self.write_bytes_cost_ms.load(Ordering::Relaxed)
    }

    pub fn inc_write_bytes(&self, v: usize) {
        if v > 0 {
            self.write_bytes.fetch_add(v, Ordering::Relaxed);
        }
    }

    pub fn get_write_bytes(&self) -> usize {
        self.write_bytes.load(Ordering::Relaxed)
    }

    pub fn inc_partitions_scanned(&self, v: u64) {
        if v > 0 {
            self.partitions_scanned.fetch_add(v, Ordering::Relaxed);
        }
    }

    pub fn get_partitions_scanned(&self) -> u64 {
        self.partitions_scanned.load(Ordering::Relaxed)
    }

    pub fn inc_partitions_total(&self, v: u64) {
        if v > 0 {
            self.partitions_total.fetch_add(v, Ordering::Relaxed);
        }
    }

    pub fn get_partitions_total(&self) -> u64 {
        self.partitions_total.load(Ordering::Relaxed)
    }

    pub fn set_status(&self, new: &str) {
        let mut status = self.status.write();
        *status = new.to_string();
    }

    pub fn get_status(&self) -> String {
        let status = self.status.read();
        status.clone()
    }
}

#[derive(Clone, Debug)]
pub struct StorageMetricsLayer {
    metrics: Arc<StorageMetrics>,
}

impl StorageMetricsLayer {
    /// Create a new storage metrics layer.
    pub fn new(metrics: Arc<StorageMetrics>) -> Self {
        StorageMetricsLayer { metrics }
    }
}

impl<A: Accessor> Layer<A> for StorageMetricsLayer {
    type LayeredAccessor = StorageMetricsAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        StorageMetricsAccessor {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StorageMetricsAccessor<A: Accessor> {
    inner: A,
    metrics: Arc<StorageMetrics>,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for StorageMetricsAccessor<A> {
    type Inner = A;
    type Reader = StorageMetricsReader<A::Reader>;
    type BlockingReader = StorageMetricsReader<A::BlockingReader>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, StorageMetricsReader::new(r, self.metrics.clone())))
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        self.inner
            .write(
                path,
                args,
                Box::new(StorageMetricsReader::new(r, self.metrics.clone())),
            )
            .await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, StorageMetricsReader::new(r, self.metrics.clone())))
    }
}

pub struct StorageMetricsReader<R> {
    inner: R,
    metrics: Arc<StorageMetrics>,
    last_pending: Option<Instant>,
}

impl<R> StorageMetricsReader<R> {
    fn new(inner: R, metrics: Arc<StorageMetrics>) -> Self {
        Self {
            inner,
            metrics,
            last_pending: None,
        }
    }
}

impl<R: input::Read> AsyncRead for StorageMetricsReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let start = match self.last_pending {
            None => Instant::now(),
            Some(t) => t,
        };

        let result = Pin::new(&mut self.inner).poll_read(cx, buf);

        match result {
            Poll::Ready(Ok(size)) => {
                self.last_pending = None;
                self.metrics.inc_write_bytes(size);
                self.metrics
                    .inc_write_bytes_cost(start.elapsed().as_millis() as u64);
            }
            Poll::Ready(Err(_)) => {
                self.last_pending = None;
            }
            Poll::Pending => {
                self.last_pending = Some(start);
            }
        };

        result
    }
}

impl<R: output::Read> output::Read for StorageMetricsReader<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let start = match self.last_pending {
            None => Instant::now(),
            Some(t) => t,
        };

        let result = self.inner.poll_read(cx, buf);

        match result {
            Poll::Ready(Ok(size)) => {
                self.last_pending = None;
                self.metrics.inc_read_bytes(size);
                self.metrics
                    .inc_read_bytes_cost(start.elapsed().as_millis() as u64);
            }
            Poll::Ready(Err(_)) => {
                self.last_pending = None;
            }
            Poll::Pending => {
                self.last_pending = Some(start);
            }
        }

        result
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<io::Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<bytes::Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: output::BlockingRead> output::BlockingRead for StorageMetricsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<io::Result<bytes::Bytes>> {
        self.inner.next()
    }
}
