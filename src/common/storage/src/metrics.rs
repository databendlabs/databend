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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use opendal::raw::observe_read;
use opendal::raw::Accessor;
use opendal::raw::BytesReader;
use opendal::raw::ReadEvent;
use opendal::raw::RpRead;
use opendal::raw::RpWrite;
use opendal::Layer;
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
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, BytesReader)> {
        let metric = self.metrics.clone();

        self.inner.read(path, args).await.map(|(rp, r)| {
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

            (rp, Box::new(r) as BytesReader)
        })
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite> {
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
}
