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
use std::sync::Arc;

use async_trait::async_trait;
use common_infallible::RwLock;
use opendal::error::Result as DalResult;
use opendal::ops::OpDelete;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::OpWrite;
use opendal::readers::CallbackReader;
use opendal::Accessor;
use opendal::Layer;
use opendal::Object;
use opendal::Reader;

use crate::metrics::DalMetrics;

#[derive(Clone, Default)]
pub struct DalContext {
    inner: Option<Arc<dyn Accessor>>,
    metrics: Arc<RwLock<DalMetrics>>,
}

impl DalContext {
    pub fn new(inner: Arc<dyn Accessor>) -> Self {
        DalContext {
            inner: Some(inner),
            metrics: Arc::new(Default::default()),
        }
    }

    /// Increment read bytes.
    pub fn inc_read_bytes(&self, bytes: usize) {
        if bytes > 0 {
            let mut metrics = self.metrics.write();
            metrics.read_bytes += bytes;
        }
    }

    /// Increment write bytes.
    pub fn inc_write_bytes(&self, bytes: usize) {
        if bytes > 0 {
            let mut metrics = self.metrics.write();
            metrics.write_bytes += bytes;
        }
    }

    /// Increment read seek times.
    pub fn inc_read_seeks(&self) {
        let mut metrics = self.metrics.write();
        metrics.read_seeks += 1;
    }

    /// Increment cost for reading bytes.
    pub fn inc_read_byte_cost_ms(&self, cost: usize) {
        if cost > 0 {
            let mut metrics = self.metrics.write();
            metrics.read_byte_cost_ms += cost;
        }
    }

    //// Increment cost for reading seek.
    pub fn inc_read_seek_cost_ms(&self, cost: usize) {
        if cost > 0 {
            let mut metrics = self.metrics.write();
            metrics.read_seek_cost_ms += cost;
        }
    }

    //// Increment numbers of rows written
    pub fn inc_write_rows(&self, rows: usize) {
        if rows > 0 {
            let mut metrics = self.metrics.write();
            metrics.write_rows += rows;
        }
    }

    //// Increment numbers of partitions scanned
    pub fn inc_partitions_scanned(&self, partitions: usize) {
        if partitions > 0 {
            let mut metrics = self.metrics.write();
            metrics.partitions_scanned += partitions;
        }
    }

    //// Increment numbers of partitions (before pruning)
    pub fn inc_partitions_total(&self, partitions: usize) {
        if partitions > 0 {
            let mut metrics = self.metrics.write();
            metrics.partitions_total += partitions;
        }
    }

    pub fn get_metrics(&self) -> DalMetrics {
        self.metrics.read().clone()
    }
}

impl Layer for DalContext {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(DalContext {
            inner: Some(inner),
            metrics: self.metrics.clone(),
        })
    }
}

#[async_trait]
impl Accessor for DalContext {
    async fn read(&self, args: &OpRead) -> DalResult<Reader> {
        let metrics = self.metrics.clone();

        // TODO(xuanwo): Maybe it's better to move into metrics.
        self.inner.as_ref().unwrap().read(args).await.map(|reader| {
            let r = CallbackReader::new(reader, move |n| {
                let mut metrics = metrics.write();
                metrics.read_bytes += n;
            });

            Box::new(r) as Reader
        })
    }
    async fn write(&self, r: Reader, args: &OpWrite) -> DalResult<usize> {
        self.inner.as_ref().unwrap().write(r, args).await.map(|n| {
            self.inc_write_bytes(n);
            n
        })
    }
    async fn stat(&self, args: &OpStat) -> DalResult<Object> {
        self.inner.as_ref().unwrap().stat(args).await
    }
    async fn delete(&self, args: &OpDelete) -> DalResult<()> {
        self.inner.as_ref().unwrap().delete(args).await
    }
}
