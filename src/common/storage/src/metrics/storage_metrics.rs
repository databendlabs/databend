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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use opendal::raw::oio;
use opendal::raw::oio::Streamer;
use opendal::raw::Accessor;
use opendal::raw::Layer;
use opendal::raw::LayeredAccessor;
use opendal::raw::OpAppend;
use opendal::raw::OpList;
use opendal::raw::OpRead;
use opendal::raw::OpWrite;
use opendal::raw::RpAppend;
use opendal::raw::RpList;
use opendal::raw::RpRead;
use opendal::raw::RpWrite;
use opendal::Result;

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
    type Reader = StorageMetricsWrapper<A::Reader>;
    type BlockingReader = StorageMetricsWrapper<A::BlockingReader>;
    type Writer = StorageMetricsWrapper<A::Writer>;
    type BlockingWriter = StorageMetricsWrapper<A::BlockingWriter>;
    type Pager = A::Pager;
    type BlockingPager = A::BlockingPager;
    type Appender = A::Appender;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[async_backtrace::framed]
    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, StorageMetricsWrapper::new(r, self.metrics.clone())))
    }

    #[async_backtrace::framed]
    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner
            .write(path, args)
            .await
            .map(|(rp, r)| (rp, StorageMetricsWrapper::new(r, self.metrics.clone())))
    }

    #[async_backtrace::framed]
    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner.list(path, args).await
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, StorageMetricsWrapper::new(r, self.metrics.clone())))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner
            .blocking_write(path, args)
            .map(|(rp, r)| (rp, StorageMetricsWrapper::new(r, self.metrics.clone())))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner.blocking_list(path, args)
    }

    async fn append(&self, path: &str, args: OpAppend) -> Result<(RpAppend, Self::Appender)> {
        self.inner.append(path, args).await
    }
}

pub struct StorageMetricsWrapper<R> {
    inner: R,
    metrics: Arc<StorageMetrics>,
    last_pending: Option<Instant>,
}

impl<R> StorageMetricsWrapper<R> {
    fn new(inner: R, metrics: Arc<StorageMetrics>) -> Self {
        Self {
            inner,
            metrics,
            last_pending: None,
        }
    }
}

impl<R: oio::Read> oio::Read for StorageMetricsWrapper<R> {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let start = self.last_pending.unwrap_or(Instant::now());

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

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<bytes::Bytes>>> {
        self.inner.poll_next(cx)
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for StorageMetricsWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        self.inner.next()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for StorageMetricsWrapper<R> {
    #[async_backtrace::framed]
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        let start = Instant::now();

        let result = self.inner.write(bs).await;
        if result.is_ok() {
            self.metrics.inc_write_bytes(size);
            self.metrics
                .inc_write_bytes_cost(start.elapsed().as_millis() as u64);
        }
        result
    }

    #[async_backtrace::framed]
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    #[async_backtrace::framed]
    async fn sink(&mut self, size: u64, s: Streamer) -> Result<()> {
        self.inner.sink(size, s).await
    }

    #[async_backtrace::framed]
    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

#[async_trait]
impl<R: oio::BlockingWrite> oio::BlockingWrite for StorageMetricsWrapper<R> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        let size = bs.len();
        let start = Instant::now();

        let result = self.inner.write(bs);
        if result.is_ok() {
            self.metrics.inc_write_bytes(size);
            self.metrics
                .inc_write_bytes_cost(start.elapsed().as_millis() as u64);
        }
        result
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}
