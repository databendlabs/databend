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

#[derive(Clone, Debug, Default)]
pub struct DalMetrics {
    /// Read bytes.
    read_bytes: Arc<AtomicUsize>,
    /// Cost(in ms) of read bytes.
    read_bytes_cost_ms: Arc<AtomicU64>,
    /// Bytes written by data access layer
    write_bytes: Arc<AtomicUsize>,
    /// Number of rows written
    write_rows: Arc<AtomicU64>,
    /// Number of partitions scanned, after pruning
    partitions_scanned: Arc<AtomicU64>,
    /// Number of partitions, before pruning
    partitions_total: Arc<AtomicU64>,
}

impl DalMetrics {
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

    pub fn get_read_bytes_cost(&self) -> u64 {
        self.read_bytes_cost_ms.load(Ordering::Relaxed)
    }

    pub fn inc_write_bytes(&self, v: usize) {
        if v > 0 {
            self.write_bytes.fetch_add(v, Ordering::Relaxed);
        }
    }

    pub fn get_write_bytes(&self) -> usize {
        self.write_bytes.load(Ordering::Relaxed)
    }

    // Increment read rows.
    pub fn inc_write_rows(&self, v: u64) {
        if v > 0 {
            self.write_rows.fetch_add(v, Ordering::Relaxed);
        }
    }

    pub fn get_write_rows(&self) -> u64 {
        self.write_rows.load(Ordering::Relaxed)
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
