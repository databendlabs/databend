// Copyright 2021 Datafuse Labs.
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

use common_infallible::RwLock;

#[derive(Clone, Debug, Default)]
pub struct DalMetrics {
    /// Read bytes.
    pub read_bytes: usize,
    /// Seek times of read.
    pub read_seeks: usize,
    /// Cost(in ms) of read bytes.
    pub read_byte_cost_ms: usize,
    /// Cost(in ms) of seek by reading.
    pub read_seek_cost_ms: usize,
    /// Bytes written by data access layer
    pub write_bytes: usize,
    /// Number of rows written
    pub write_rows: usize,
    /// Number of partitions scanned
    pub partitions_scanned: usize,
}

#[derive(Clone, Debug, Default)]
pub struct DalContext {
    metrics: Arc<RwLock<DalMetrics>>,
}

impl DalContext {
    pub fn create() -> Self {
        DalContext {
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

    pub fn get_metrics(&self) -> DalMetrics {
        self.metrics.read().clone()
    }
}
