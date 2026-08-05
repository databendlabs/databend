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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use opendal::Buffer;
use opendal::BytesRange;
use opendal::Capability;
use opendal::Metadata;
use opendal::OperationContext;
use opendal::Result;
use opendal::raw::Layer;
use opendal::raw::OpCopier;
use opendal::raw::OpCopy;
use opendal::raw::OpCreateDir;
use opendal::raw::OpList;
use opendal::raw::OpPresign;
use opendal::raw::OpRead;
use opendal::raw::OpRename;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::RpCreateDir;
use opendal::raw::RpPresign;
use opendal::raw::RpRead;
use opendal::raw::RpRename;
use opendal::raw::RpStat;
use opendal::raw::Service;
use opendal::raw::ServiceInfo;
use opendal::raw::Servicer;
use opendal::raw::oio;

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

impl Layer for StorageMetricsLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(StorageMetricsService {
            inner,
            metrics: self.metrics.clone(),
        })
    }
}

#[derive(Clone)]
struct StorageMetricsService {
    inner: Servicer,
    metrics: Arc<StorageMetrics>,
}

impl Debug for StorageMetricsService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageMetricsService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Service for StorageMetricsService {
    type Reader = StorageMetricsReader;
    type Writer = StorageMetricsWrapper<oio::Writer>;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner
            .read(ctx, path, args)
            .map(|r| StorageMetricsReader::new(r, self.metrics.clone()))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner
            .write(ctx, path, args)
            .map(|w| StorageMetricsWrapper::new(w, self.metrics.clone()))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

struct StorageMetricsReader {
    inner: oio::Reader,
    metrics: Arc<StorageMetrics>,
}

impl StorageMetricsReader {
    fn new(inner: oio::Reader, metrics: Arc<StorageMetrics>) -> Self {
        Self { inner, metrics }
    }
}

impl oio::Read for StorageMetricsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (rp, stream) = self.inner.open(range).await?;
        Ok((
            rp,
            Box::new(StorageMetricsWrapper::new(stream, self.metrics.clone()))
                as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let start = Instant::now();
        self.inner.read(range).await.inspect(|(_, buf)| {
            self.metrics.inc_read_bytes(buf.len());
            self.metrics
                .inc_read_bytes_cost(start.elapsed().as_millis() as u64);
        })
    }
}

pub struct StorageMetricsWrapper<R> {
    inner: R,
    metrics: Arc<StorageMetrics>,
}

impl<R> StorageMetricsWrapper<R> {
    fn new(inner: R, metrics: Arc<StorageMetrics>) -> Self {
        Self { inner, metrics }
    }
}

impl<R: oio::ReadStream> oio::ReadStream for StorageMetricsWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let start = Instant::now();

        self.inner.read().await.inspect(|buf| {
            self.metrics.inc_read_bytes(buf.len());
            self.metrics
                .inc_read_bytes_cost(start.elapsed().as_millis() as u64);
        })
    }
}

impl<R: oio::Write> oio::Write for StorageMetricsWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let start = Instant::now();
        let size = bs.len();

        self.inner.write(bs).await.inspect(|_| {
            self.metrics.inc_write_bytes(size);
            self.metrics
                .inc_write_bytes_cost(start.elapsed().as_millis() as u64);
        })
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
