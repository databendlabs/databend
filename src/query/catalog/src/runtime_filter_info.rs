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

use std::cmp::Ordering as CmpOrdering;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::sync::watch::Sender;

use crate::sbbf::Sbbf;

pub type RuntimeBloomFilter = Arc<Sbbf>;

/// A monotonic TopN boundary shared by all local scan and PartialTopN processors
/// for one scan. The boundary is absent until a partial candidate set is full.
///
/// A plain `RwLock` is sufficient here: readers touch the boundary once per
/// pruned block/partition, and publishers call [`Self::update`] only when
/// their local boundary tightens, so contention is negligible.
pub struct RuntimeTopNFilter {
    column_id: u32,
    asc: bool,
    nulls_first: bool,
    boundary: RwLock<Option<Scalar>>,
}

impl RuntimeTopNFilter {
    pub fn new(column_id: u32, asc: bool, nulls_first: bool) -> Self {
        Self {
            column_id,
            asc,
            nulls_first,
            boundary: RwLock::new(None),
        }
    }

    pub fn column_id(&self) -> u32 {
        self.column_id
    }

    pub fn asc(&self) -> bool {
        self.asc
    }

    pub fn nulls_first(&self) -> bool {
        self.nulls_first
    }

    pub fn boundary(&self) -> Option<Scalar> {
        self.boundary.read().clone()
    }

    /// Whether `candidate` is strictly tighter than `current`.
    fn tighter(&self, candidate: &Scalar, current: Option<&Scalar>) -> bool {
        match current {
            None => true,
            Some(old) => match candidate.partial_cmp(old) {
                Some(CmpOrdering::Less) => self.asc,
                Some(CmpOrdering::Greater) => !self.asc,
                _ => false,
            },
        }
    }

    /// Publish a per-stream boundary. ASC boundaries can only decrease and DESC
    /// boundaries can only increase, so readers never observe a weaker filter.
    pub fn update(&self, boundary: &Scalar) {
        if matches!(boundary, Scalar::Null) {
            return;
        }

        // Fast path: skip the write lock when another stream has already
        // published an equal or tighter boundary.
        if !self.tighter(boundary, self.boundary.read().as_ref()) {
            return;
        }

        let mut current = self.boundary.write();
        // Re-check under the write lock: a concurrent update may have won.
        if self.tighter(boundary, current.as_ref()) {
            *current = Some(boundary.clone());
        }
    }

    /// Return true only when every row in a block is strictly worse than the
    /// current boundary. Equal values are retained for tie safety.
    ///
    /// `min`/`max` describe the non-null values of the block and `null_count`
    /// its null rows; boundaries are never null, so null rows rank by
    /// `nulls_first` alone.
    pub fn should_prune(&self, min: &Scalar, max: &Scalar, null_count: u64) -> bool {
        let boundary = self.boundary.read();
        let Some(boundary) = boundary.as_ref() else {
            return false;
        };

        // Under NULLS FIRST null rows sort before the boundary and stay
        // candidates forever.
        if self.nulls_first && null_count > 0 {
            return false;
        }

        // Blocks without non-null values have null min/max statistics. Under
        // NULLS LAST every such row sorts after the non-null boundary.
        if matches!(min, Scalar::Null) || matches!(max, Scalar::Null) {
            return !self.nulls_first && null_count > 0;
        }

        if self.asc {
            min.partial_cmp(boundary) == Some(CmpOrdering::Greater)
        } else {
            max.partial_cmp(boundary) == Some(CmpOrdering::Less)
        }
    }
}

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
    statistics_column_names: Vec<String>,
}

impl RuntimeFilterReady {
    pub fn with_statistics_column_names(
        column_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let (watcher, dummy_receiver) = watch::channel(None);
        let statistics_column_names = column_names
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        Self {
            runtime_filter_watcher: watcher,
            _runtime_filter_dummy_receiver: dummy_receiver,
            statistics_column_names,
        }
    }

    pub fn for_statistics_probe_exprs<'a>(
        enable_statistics_pruning: bool,
        probe_exprs: impl IntoIterator<Item = &'a Expr<String>>,
    ) -> Self {
        if !enable_statistics_pruning {
            return Self::default();
        }

        let statistics_column_names = probe_exprs
            .into_iter()
            .flat_map(|expr| expr.column_refs().into_keys())
            .collect::<BTreeSet<_>>();
        Self::with_statistics_column_names(statistics_column_names)
    }

    pub fn has_statistics_pruning(&self) -> bool {
        !self.statistics_column_names.is_empty()
    }

    pub fn statistics_column_names(&self) -> &[String] {
        &self.statistics_column_names
    }
}

impl Default for RuntimeFilterReady {
    fn default() -> Self {
        Self::with_statistics_column_names(Vec::<String>::new())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Expr;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn int64(value: i64) -> Scalar {
        Scalar::Number(NumberScalar::Int64(value))
    }

    #[test]
    fn runtime_top_n_filter_is_monotonic_and_tie_safe() {
        let asc = RuntimeTopNFilter::new(7, true, false);
        assert!(!asc.should_prune(&int64(11), &int64(20), 0));

        asc.update(&int64(10));
        assert_eq!(asc.boundary(), Some(int64(10)));
        assert!(asc.should_prune(&int64(11), &int64(20), 0));
        assert!(!asc.should_prune(&int64(10), &int64(20), 0));

        // A weaker local boundary must not loosen the shared filter.
        asc.update(&int64(12));
        assert_eq!(asc.boundary(), Some(int64(10)));
        asc.update(&int64(8));
        assert_eq!(asc.boundary(), Some(int64(8)));

        let desc = RuntimeTopNFilter::new(7, false, false);
        desc.update(&int64(10));
        assert!(desc.should_prune(&int64(1), &int64(9), 0));
        assert!(!desc.should_prune(&int64(1), &int64(10), 0));
        desc.update(&int64(8));
        assert_eq!(desc.boundary(), Some(int64(10)));
        desc.update(&int64(12));
        assert_eq!(desc.boundary(), Some(int64(12)));

        desc.update(&Scalar::Null);
        assert_eq!(desc.boundary(), Some(int64(12)));
    }

    #[test]
    fn runtime_top_n_filter_ranks_nulls_by_ordering() {
        let nulls_last = RuntimeTopNFilter::new(1, true, false);
        nulls_last.update(&int64(10));
        // Nulls sort after the boundary, so null rows are prunable too.
        assert!(nulls_last.should_prune(&int64(11), &int64(20), 5));
        // All-null blocks sort entirely after the boundary.
        assert!(nulls_last.should_prune(&Scalar::Null, &Scalar::Null, 7));
        assert!(!nulls_last.should_prune(&int64(9), &int64(20), 5));

        let nulls_first = RuntimeTopNFilter::new(1, true, true);
        nulls_first.update(&int64(10));
        // Null rows are always candidates under NULLS FIRST.
        assert!(!nulls_first.should_prune(&int64(11), &int64(20), 1));
        assert!(nulls_first.should_prune(&int64(11), &int64(20), 0));
        assert!(!nulls_first.should_prune(&Scalar::Null, &Scalar::Null, 7));
    }

    #[test]
    fn runtime_top_n_filter_concurrent_updates_keep_tightest() {
        let filter = Arc::new(RuntimeTopNFilter::new(1, true, false));
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let filter = filter.clone();
                std::thread::spawn(move || {
                    // Race tightening publishes against lock-free reads.
                    for v in (0..500).rev() {
                        filter.update(&int64(v * 4 + t));
                        let _ = filter.should_prune(&int64(1), &int64(2), 0);
                        let _ = filter.boundary();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        // The global minimum wins regardless of interleaving.
        assert_eq!(filter.boundary(), Some(int64(0)));
        assert!(filter.should_prune(&int64(1), &int64(5), 0));
        assert!(!filter.should_prune(&int64(0), &int64(5), 0));
    }

    #[test]
    fn runtime_filter_ready_tracks_statistics_probe_columns() {
        let probe_expr = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "probe_col".to_string(),
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: "probe_col".to_string(),
        });

        let ready =
            RuntimeFilterReady::for_statistics_probe_exprs(true, [&probe_expr, &probe_expr]);
        assert_eq!(ready.statistics_column_names(), ["probe_col"]);
        assert!(ready.has_statistics_pruning());

        let ready = RuntimeFilterReady::for_statistics_probe_exprs(false, [&probe_expr]);
        assert!(ready.statistics_column_names().is_empty());
        assert!(!ready.has_statistics_pruning());
    }
}
