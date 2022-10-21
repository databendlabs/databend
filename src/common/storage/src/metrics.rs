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

use std::io::Result;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use opendal::io_util::observe_read;
use opendal::io_util::ReadEvent;
use opendal::ops::OpCreate;
use opendal::ops::OpDelete;
use opendal::ops::OpList;
use opendal::ops::OpPresign;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::OpWrite;
use opendal::ops::PresignedRequest;
use opendal::Accessor;
use opendal::AccessorMetadata;
use opendal::BytesReader;
use opendal::Layer;
use opendal::ObjectMetadata;
use opendal::ObjectStreamer;

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
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(StorageMetricsAccessor {
            inner,
            metrics: self.metrics.clone(),
        })
    }
}

#[derive(Clone, Debug)]
struct StorageMetricsAccessor {
    inner: Arc<dyn Accessor>,
    metrics: Arc<StorageMetrics>,
}

#[async_trait]
impl Accessor for StorageMetricsAccessor {
    fn metadata(&self) -> AccessorMetadata {
        self.inner.metadata()
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        self.inner.create(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let metric = self.metrics.clone();

        self.inner.read(path, args).await.map(|r| {
            let mut last_pending = None;
            let r = observe_read(r, move |e| {
                let start = match last_pending {
                    None => Instant::now(),
                    Some(t) => t,
                };
                match e {
                    ReadEvent::Pending => last_pending = Some(start),
                    ReadEvent::Read(n) => {
                        last_pending = None;
                        metric.inc_read_bytes(n);
                    }
                    ReadEvent::Error(_) => last_pending = None,
                    _ => {}
                }
                metric.inc_read_bytes_cost(start.elapsed().as_millis() as u64);
            });

            Box::new(r) as BytesReader
        })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let metric = self.metrics.clone();

        let mut last_pending = None;

        let r = observe_read(r, move |e| {
            let start = match last_pending {
                None => Instant::now(),
                Some(t) => t,
            };
            match e {
                ReadEvent::Pending => last_pending = Some(start),
                ReadEvent::Read(n) => {
                    last_pending = None;
                    metric.inc_write_bytes(n);
                }
                ReadEvent::Error(_) => last_pending = None,
                _ => {}
            }
            metric.inc_write_bytes_cost(start.elapsed().as_millis() as u64);
        });

        self.inner.write(path, args, Box::new(r)).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args).await
    }

    /// TODO: we need to make sure returning object's accessor is correct.
    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        self.inner.list(path, args).await
    }

    fn presign(&self, path: &str, args: OpPresign) -> Result<PresignedRequest> {
        self.inner.presign(path, args)
    }
}
