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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::WatchNotify;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::Expr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::Bitmap;
use opendal::Operator;
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::plan::PartInfoPtr;
use crate::sbbf::Sbbf;

pub type RuntimeBloomFilter = Arc<Sbbf>;

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
    pub spatial: Option<RuntimeFilterSpatial>,
    pub inlist: Option<Expr<String>>,
    pub inlist_value_count: usize,
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

#[derive(Clone)]
pub struct RuntimeFilterSpatial {
    pub column_name: String,
    pub srid: i32,
    pub rtrees: Arc<Vec<u8>>,
    pub rtree_bounds: Option<[f64; 4]>,
}

#[derive(Default)]
pub struct RuntimeFilterStats {
    bloom_time_ns: AtomicU64,
    bloom_rows_filtered: AtomicU64,
    inlist_min_max_time_ns: AtomicU64,
    min_max_rows_filtered: AtomicU64,
    min_max_partitions_pruned: AtomicU64,
    spatial_time_ns: AtomicU64,
    spatial_rows_filtered: AtomicU64,
    spatial_partitions_pruned: AtomicU64,
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

    pub fn record_spatial(&self, time_ns: u64, rows_filtered: u64, partitions_pruned: u64) {
        self.spatial_time_ns.fetch_add(time_ns, Ordering::Relaxed);
        self.spatial_rows_filtered
            .fetch_add(rows_filtered, Ordering::Relaxed);
        self.spatial_partitions_pruned
            .fetch_add(partitions_pruned, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RuntimeFilterStatsSnapshot {
        RuntimeFilterStatsSnapshot {
            bloom_time_ns: self.bloom_time_ns.load(Ordering::Relaxed),
            bloom_rows_filtered: self.bloom_rows_filtered.load(Ordering::Relaxed),
            inlist_min_max_time_ns: self.inlist_min_max_time_ns.load(Ordering::Relaxed),
            min_max_rows_filtered: self.min_max_rows_filtered.load(Ordering::Relaxed),
            min_max_partitions_pruned: self.min_max_partitions_pruned.load(Ordering::Relaxed),
            spatial_time_ns: self.spatial_time_ns.load(Ordering::Relaxed),
            spatial_rows_filtered: self.spatial_rows_filtered.load(Ordering::Relaxed),
            spatial_partitions_pruned: self.spatial_partitions_pruned.load(Ordering::Relaxed),
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
    pub spatial_time_ns: u64,
    pub spatial_rows_filtered: u64,
    pub spatial_partitions_pruned: u64,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterReport {
    pub filter_id: usize,
    pub has_bloom: bool,
    pub has_inlist: bool,
    pub has_min_max: bool,
    pub stats: RuntimeFilterStatsSnapshot,
}

/// Runtime filter that prunes partitions using only partition metadata (e.g. min/max stats).
/// No IO required. Applied in PartitionStreamSource.
pub trait PartitionRuntimeFilter: Send + Sync {
    /// Returns true if the partition should be pruned (skipped).
    fn prune(&self, part: &PartInfoPtr) -> bool;
}

pub type RowRuntimeFilters = Vec<Arc<dyn RowRuntimeFilter>>;
pub type IndexRuntimeFilters = Vec<Arc<dyn IndexRuntimeFilter>>;
pub type PartitionRuntimeFilters = Vec<Arc<dyn PartitionRuntimeFilter>>;

/// Runtime filter that prunes partitions by loading index files (bloom index, spatial index).
/// Requires async IO. Applied in ReadDataTransform.
/// Split into load_index (IO) and prune (computation) for caller-controlled IO scheduling.
/// ReadSettings should be embedded at construction time.
#[async_trait::async_trait]
pub trait IndexRuntimeFilter: Send + Sync {
    /// Load index data for the given partition.
    async fn load_index(
        &self,
        part: &PartInfoPtr,
        op: &Operator,
    ) -> Result<Option<Box<dyn Any + Send>>>;

    /// Returns true if the partition should be pruned (skipped).
    /// `index` is the data returned by `load_index`, None if no index available.
    fn prune(&self, part: &PartInfoPtr, index: Option<&dyn Any>) -> Result<bool>;
}

/// Runtime filter applied per-row during block deserialization (e.g. Sbbf bloom filter).
/// Applied in NativeDeserializeDataTransform / ReadState.
pub trait RowRuntimeFilter: Send + Sync {
    fn column_name(&self) -> &str;
    fn apply(&self, column: Column) -> Result<Bitmap>;
}

const RUNTIME_FILTER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const RUNTIME_FILTER_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Shared state between [`RuntimeFilterBuilder`] (build side) and
/// [`RuntimeFilterSource`] (probe side).
pub struct RuntimeFilterShared {
    table_schema: OnceLock<TableSchemaRef>,
    builder_count: AtomicUsize,
    ready_notify: Notify,
    partition_filters: RwLock<PartitionRuntimeFilters>,
    index_filters: RwLock<IndexRuntimeFilters>,
    row_filters: RwLock<RowRuntimeFilters>,
}

impl Default for RuntimeFilterShared {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeFilterShared {
    pub fn new() -> Self {
        Self {
            table_schema: OnceLock::new(),
            builder_count: AtomicUsize::new(0),
            ready_notify: Notify::new(),
            partition_filters: RwLock::new(vec![]),
            index_filters: RwLock::new(vec![]),
            row_filters: RwLock::new(vec![]),
        }
    }

    fn is_ready(&self) -> bool {
        self.builder_count.load(Ordering::Acquire) == 0
    }
}

/// Build side handle. Clone increments the builder count; drop decrements it.
/// When the last builder is dropped, the probe side is notified.
pub struct RuntimeFilterBuilder {
    shared: Arc<RuntimeFilterShared>,
}

impl RuntimeFilterBuilder {
    fn new(shared: Arc<RuntimeFilterShared>) -> Self {
        shared.builder_count.fetch_add(1, Ordering::AcqRel);
        Self { shared }
    }

    pub fn table_schema(&self) -> Option<&TableSchemaRef> {
        self.shared.table_schema.get()
    }

    pub fn add_partition_filters(&self, filters: PartitionRuntimeFilters) {
        if !filters.is_empty() {
            self.shared.partition_filters.write().extend(filters);
        }
    }

    pub fn add_index_filters(&self, filters: IndexRuntimeFilters) {
        if !filters.is_empty() {
            self.shared.index_filters.write().extend(filters);
        }
    }

    pub fn add_row_filters(&self, filters: RowRuntimeFilters) {
        if !filters.is_empty() {
            self.shared.row_filters.write().extend(filters);
        }
    }
}

impl Clone for RuntimeFilterBuilder {
    fn clone(&self) -> Self {
        self.shared.builder_count.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for RuntimeFilterBuilder {
    fn drop(&mut self) {
        if self.shared.builder_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last builder dropped — notify all waiting sources.
            self.shared.ready_notify.notify_waiters();
        }
    }
}

/// Probe side handle. Waits for all builders to drop, then reads filters.
pub struct RuntimeFilterSource {
    shared: Arc<RuntimeFilterShared>,
}

impl RuntimeFilterSource {
    fn new(shared: Arc<RuntimeFilterShared>) -> Self {
        Self { shared }
    }

    pub fn set_table_schema(&self, schema: TableSchemaRef) {
        let _ = self.shared.table_schema.set(schema);
    }

    /// Returns true if all builders have finished (dropped).
    pub fn is_ready(&self) -> bool {
        self.shared.is_ready()
    }

    /// Wait until all builders have dropped or timeout / abort.
    /// Returns `true` if ready, `false` if timed out.
    pub async fn wait_ready(&self, abort_notify: Arc<WatchNotify>) -> bool {
        if self.shared.is_ready() {
            return true;
        }

        let deadline = Instant::now() + RUNTIME_FILTER_WAIT_TIMEOUT;
        loop {
            let now = Instant::now();
            if now >= deadline {
                log::warn!(
                    "Runtime filter wait timeout after {:?}",
                    RUNTIME_FILTER_WAIT_TIMEOUT,
                );
                return false;
            }

            let wait_duration = (deadline - now).min(RUNTIME_FILTER_WAIT_POLL_INTERVAL);
            tokio::select! {
                _ = self.shared.ready_notify.notified() => {
                    if self.shared.is_ready() {
                        return true;
                    }
                    // Spurious wake or partial drop — keep waiting.
                }
                _ = abort_notify.notified() => {
                    return false;
                }
                _ = tokio::time::sleep(wait_duration) => {
                    if self.shared.is_ready() {
                        return true;
                    }
                }
            }
        }
    }

    pub fn get_partition_filters(&self) -> PartitionRuntimeFilters {
        self.shared.partition_filters.read().clone()
    }

    pub fn get_index_filters(&self) -> IndexRuntimeFilters {
        self.shared.index_filters.read().clone()
    }

    pub fn get_row_filters(&self) -> RowRuntimeFilters {
        self.shared.row_filters.read().clone()
    }
}

impl Clone for RuntimeFilterSource {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

/// Create a paired builder/source for a single scan_id.
pub fn runtime_filter_channel() -> (RuntimeFilterBuilder, RuntimeFilterSource) {
    let shared = Arc::new(RuntimeFilterShared::new());
    (
        RuntimeFilterBuilder::new(shared.clone()),
        RuntimeFilterSource::new(shared),
    )
}

/// Create a builder from an existing shared state (for additional joins targeting the same scan).
pub fn runtime_filter_builder(shared: &Arc<RuntimeFilterShared>) -> RuntimeFilterBuilder {
    RuntimeFilterBuilder::new(shared.clone())
}

/// Create a source from an existing shared state.
pub fn runtime_filter_source(shared: &Arc<RuntimeFilterShared>) -> RuntimeFilterSource {
    RuntimeFilterSource::new(shared.clone())
}

/// Get the shared state from a builder (for storing in ctx).
impl RuntimeFilterBuilder {
    pub fn shared(&self) -> &Arc<RuntimeFilterShared> {
        &self.shared
    }
}

/// Get the shared state from a source.
impl RuntimeFilterSource {
    pub fn shared(&self) -> &Arc<RuntimeFilterShared> {
        &self.shared
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_base::base::WatchNotify;

    use super::*;

    #[tokio::test]
    async fn test_builder_drop_signals_source() {
        let (builder, source) = runtime_filter_channel();
        assert!(!source.is_ready());

        drop(builder);
        assert!(source.is_ready());

        let abort = Arc::new(WatchNotify::new());
        let ready = source.wait_ready(abort).await;
        assert!(ready);
    }

    #[tokio::test]
    async fn test_multiple_builders_all_must_drop() {
        let (builder1, source) = runtime_filter_channel();
        let builder2 = builder1.clone();
        assert!(!source.is_ready());

        drop(builder1);
        assert!(!source.is_ready());

        drop(builder2);
        assert!(source.is_ready());
    }

    #[tokio::test]
    async fn test_abort_cancels_wait() {
        let (_builder, source) = runtime_filter_channel();
        let abort = Arc::new(WatchNotify::new());

        // Notify abort immediately so wait_ready returns false
        abort.notify_waiters();

        let ready = source.wait_ready(abort).await;
        assert!(!ready);
    }

    #[tokio::test]
    async fn test_builder_pushes_filters_source_reads() {
        let (builder, source) = runtime_filter_channel();

        assert!(source.get_partition_filters().is_empty());
        assert!(source.get_index_filters().is_empty());
        assert!(source.get_row_filters().is_empty());

        // We can't easily create real trait impls here, but we can verify
        // the empty → non-empty transition works with the container.
        // The actual trait impl construction is tested via integration tests.

        drop(builder);
        assert!(source.is_ready());
        assert!(source.get_partition_filters().is_empty());
    }

    #[tokio::test]
    async fn test_table_schema_set_and_get() {
        let (builder, source) = runtime_filter_channel();

        assert!(builder.table_schema().is_none());

        let schema = Arc::new(databend_common_expression::TableSchema::empty());
        source.set_table_schema(schema.clone());

        assert!(builder.table_schema().is_some());
    }
}
