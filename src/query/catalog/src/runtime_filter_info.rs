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
use std::sync::atomic::Ordering;

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_expression::Expr;

use crate::sbbf::Sbbf;

pub type RuntimeBloomFilter = Sbbf;

#[derive(Clone, Default)]
pub struct RuntimeFilterInfo {
    pub filters: Vec<RuntimeFilterEntry>,
}

impl Debug for RuntimeFilterInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RuntimeFilterInfo {{ filters: [{}] }}",
            self.filters
                .iter()
                .map(|entry| format!("#{}(probe:{})", entry.id, entry.probe_expr.sql_display()))
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

impl RuntimeFilterInfo {
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn is_blooms_empty(&self) -> bool {
        self.filters.iter().all(|entry| entry.bloom.is_none())
    }
}

#[derive(Clone)]
pub struct RuntimeFilterEntry {
    pub id: usize,
    pub probe_expr: Expr<String>,
    pub bloom: Option<RuntimeFilterBloom>,
    pub inlist: Option<Expr<String>>,
    pub min_max: Option<Expr<String>>,
    pub stats: Arc<RuntimeFilterStats>,
    pub build_rows: usize,
    pub build_table_rows: Option<u64>,
    pub enabled: bool,
}

#[derive(Clone)]
pub struct RuntimeFilterBloom {
    pub column_name: String,
    pub filter: RuntimeBloomFilter,
}

#[derive(Default)]
pub struct RuntimeFilterStats {
    bloom_time_ns: AtomicU64,
    bloom_rows_filtered: AtomicU64,
    inlist_min_max_time_ns: AtomicU64,
    min_max_rows_filtered: AtomicU64,
    min_max_partitions_pruned: AtomicU64,
}

impl RuntimeFilterStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_bloom(&self, time_ns: u64, rows_filtered: u64) {
        self.bloom_time_ns.fetch_add(time_ns, Ordering::Relaxed);
        self.bloom_rows_filtered
            .fetch_add(rows_filtered, Ordering::Relaxed);
    }

    pub fn record_inlist_min_max(&self, time_ns: u64, rows_filtered: u64, partitions_pruned: u64) {
        self.inlist_min_max_time_ns
            .fetch_add(time_ns, Ordering::Relaxed);
        self.min_max_rows_filtered
            .fetch_add(rows_filtered, Ordering::Relaxed);
        self.min_max_partitions_pruned
            .fetch_add(partitions_pruned, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RuntimeFilterStatsSnapshot {
        RuntimeFilterStatsSnapshot {
            bloom_time_ns: self.bloom_time_ns.load(Ordering::Relaxed),
            bloom_rows_filtered: self.bloom_rows_filtered.load(Ordering::Relaxed),
            inlist_min_max_time_ns: self.inlist_min_max_time_ns.load(Ordering::Relaxed),
            min_max_rows_filtered: self.min_max_rows_filtered.load(Ordering::Relaxed),
            min_max_partitions_pruned: self.min_max_partitions_pruned.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct RuntimeFilterStatsSnapshot {
    pub bloom_time_ns: u64,
    pub bloom_rows_filtered: u64,
    pub inlist_min_max_time_ns: u64,
    pub min_max_rows_filtered: u64,
    pub min_max_partitions_pruned: u64,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterReport {
    pub filter_id: usize,
    pub has_bloom: bool,
    pub has_inlist: bool,
    pub has_min_max: bool,
    pub stats: RuntimeFilterStatsSnapshot,
}

pub struct RuntimeFilterReady {
    pub runtime_filter_watcher: Sender<Option<()>>,
    /// A dummy receiver to make runtime_filter_watcher channel open.
    pub _runtime_filter_dummy_receiver: Receiver<Option<()>>,
}

impl Default for RuntimeFilterReady {
    fn default() -> Self {
        let (watcher, dummy_receiver) = watch::channel(None);
        Self {
            runtime_filter_watcher: watcher,
            _runtime_filter_dummy_receiver: dummy_receiver,
        }
    }
}
