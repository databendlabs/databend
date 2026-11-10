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

//! Metrics of what the cache holds and why entries leave it, read from the cache manager when
//! metrics are scraped. A reclaim, which runs inside allocations, possibly while the allocating
//! thread holds a metrics lock, touches no metric: evictions are counted in plain atomics. A
//! lookup counts its access, hit or miss through `CacheLookupCounters`, an atomic addition
//! unless the counters were cleared (`TRUNCATE system.metrics`); then it takes the family's
//! lock once, and an allocation under it may reclaim, which takes no metric lock. The collector
//! reads in-cache counts under the shard read locks; a reclaim on another thread only tries
//! those locks and skips the busy shards.
//!
//! The collector runs under the global metrics registry's lock, so it must not touch a metric:
//! the first use of one registers it, which takes that lock again. It reads
//! `CacheManager::composition`, which leaves out the access, hit and miss counts; those are
//! exported by their own families.
//!
//! Gauges, per kind in memory and on disk, labelled by `cache_name` and `pool` (`meta`, `index`,
//! `data`, or `disk`): `cache_items`, `cache_resident_bytes` (charged, including values evicted
//! but still held by queries), `cache_in_cache_bytes` (still in the cache), `cache_capacity` (in
//! the kind's `unit`, absent when unlimited) and `cache_enabled`. Per memory pool, shared backend
//! only: `cache_pool_items`, `cache_pool_resident_bytes`, `cache_pool_in_cache_bytes` and
//! `cache_pool_capacity_bytes`. Per disk pool: `cache_disk_pool_items`,
//! `cache_disk_pool_used_bytes` and `cache_disk_pool_capacity_bytes`; for the disk cache as a
//! whole, `cache_disk_queue_bytes` and the counter `cache_disk_dropped` by `reason`
//! (`queue_full`, `rejected`, `io_error`).
//!
//! Counters per kind on disk, since the process started (see `disk_kind_families`):
//! `cache_disk_hit_bytes`, `cache_disk_queued_hit_count`, `cache_disk_read_microseconds`,
//! `cache_disk_storage_read_bytes`, `cache_disk_write_count`, `cache_disk_write_bytes`,
//! `cache_disk_drop_count` by `reason`, `cache_disk_eviction_count` and
//! `cache_disk_eviction_bytes` by `reason` (`capacity`: room for a write or a lowered
//! capacity; `disk_pressure`: the file system over `cache.disk.watermark_ratio`), and
//! `cache_disk_invalidation_count` and `cache_disk_invalidation_bytes`.
//!
//! Counters, shared backend only, since the process started:
//! - `cache_eviction_count` and `cache_eviction_bytes` per kind, by `reason`: `capacity` (room in
//!   the pool, a lowered cap or limit) or `memory_pressure` (reclaimed for queries). Explicit
//!   evictions and replacements are not counted.
//! - `cache_insert_rejected_count` per kind, by `reason`: `oversized`, `kind_limit` or
//!   `pool_full_pinned`.
//! - `cache_memory_reclaim_count`, `cache_memory_reclaim_bytes` (charges released) and
//!   `cache_memory_reclaim_milliseconds` (spent).
//!
//! Every family reaches both the Prometheus scrape and `system.metrics` (a `SampleCollector`),
//! from one read; `TRUNCATE system.metrics` does not reset them.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Once;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_base::runtime::metrics::MetricSample;
use databend_common_base::runtime::metrics::MetricValue;
use databend_common_base::runtime::metrics::SampleCollector;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;

use crate::CacheKind;
use crate::CacheManager;
use crate::DiskCache;
use crate::disk::disk_pool::Eviction;
use crate::disk::disk_writer::DropReason;
use crate::manager::CacheRow;

/// Collects the metrics of one manager, or of the global one.
pub struct CacheMetricsCollector {
    manager: Option<Arc<CacheManager>>,
}

impl Debug for CacheMetricsCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let source = if self.manager.is_some() {
            "manager"
        } else {
            "global"
        };
        f.debug_struct("CacheMetricsCollector")
            .field("source", &source)
            .finish()
    }
}

impl CacheMetricsCollector {
    /// Registers the collector of the global manager, once per process, for both the scrape and
    /// `system.metrics`.
    pub(crate) fn register_global() {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            GLOBAL_METRICS_REGISTRY
                .register_sample_collector(Arc::new(CacheMetricsCollector { manager: None }));
        });
    }

    /// The collector of `manager`, for registries other than the global one.
    pub fn of(manager: Arc<CacheManager>) -> Self {
        CacheMetricsCollector {
            manager: Some(manager),
        }
    }

    fn manager(&self) -> Option<Arc<CacheManager>> {
        self.manager
            .clone()
            .or_else(GlobalInstance::try_get::<Arc<CacheManager>>)
    }
}

type Labels = Vec<(&'static str, String)>;

fn with(labels: &Labels, name: &'static str, value: &str) -> Labels {
    let mut labels = labels.clone();
    labels.push((name, value.to_string()));
    labels
}

struct Family {
    name: &'static str,
    help: &'static str,
    metric_type: MetricType,
    samples: Vec<(Labels, u64)>,
}

impl Family {
    fn gauge(name: &'static str, help: &'static str) -> Self {
        Family {
            name,
            help,
            metric_type: MetricType::Gauge,
            samples: Vec::new(),
        }
    }

    fn counter(name: &'static str, help: &'static str) -> Self {
        Family {
            metric_type: MetricType::Counter,
            ..Self::gauge(name, help)
        }
    }

    fn push(&mut self, labels: Labels, value: u64) {
        self.samples.push((labels, value));
    }

    fn encode(&self, encoder: &mut DescriptorEncoder) -> Result<(), std::fmt::Error> {
        if self.samples.is_empty() {
            return Ok(());
        }
        let mut family = encoder.encode_descriptor(self.name, self.help, None, self.metric_type)?;
        let encode = |value: u64, sample: MetricEncoder| match self.metric_type {
            MetricType::Counter => ConstCounter::new(value).encode(sample),
            _ => ConstGauge::new(value as i64).encode(sample),
        };
        if let [(labels, value)] = self.samples.as_slice()
            && labels.is_empty()
        {
            return encode(*value, family);
        }
        for (labels, value) in &self.samples {
            encode(*value, family.encode_family(labels)?)?;
        }
        Ok(())
    }

    /// The samples of `system.metrics`, named as the scrape names them: counters with the
    /// `_total` suffix the encoder appends.
    fn sample(&self, samples: &mut Vec<MetricSample>) {
        let counter = matches!(self.metric_type, MetricType::Counter);
        let name = if counter {
            format!("{}_total", self.name)
        } else {
            self.name.to_string()
        };
        for (labels, value) in &self.samples {
            let value = *value as f64;
            samples.push(MetricSample {
                name: name.clone(),
                labels: labels
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.clone()))
                    .collect::<HashMap<_, _>>(),
                value: if counter {
                    MetricValue::Counter(value)
                } else {
                    MetricValue::Gauge(value)
                },
            });
        }
    }
}

fn row_labels(row: &CacheRow) -> Labels {
    let pool = row.pool.map_or("disk", |pool| pool.name());
    vec![("cache_name", row.name.clone()), ("pool", pool.to_string())]
}

struct Families {
    items: Family,
    resident: Family,
    in_cache: Family,
    capacity: Family,
    enabled: Family,
    pool_items: Family,
    pool_resident: Family,
    pool_in_cache: Family,
    pool_capacity: Family,
    evicted: Family,
    evicted_bytes: Family,
    rejected: Family,
}

impl Families {
    fn new() -> Self {
        Families {
            items: Family::gauge("cache_items", "Entries in the cache."),
            resident: Family::gauge(
                "cache_resident_bytes",
                "Bytes charged, including values evicted but still held by queries.",
            ),
            in_cache: Family::gauge("cache_in_cache_bytes", "Bytes still in the cache."),
            capacity: Family::gauge(
                "cache_capacity",
                "Capacity in the unit of the unit label; absent when unlimited.",
            ),
            enabled: Family::gauge("cache_enabled", "1 when the cache is on."),
            pool_items: Family::gauge("cache_pool_items", "Entries in the pool."),
            pool_resident: Family::gauge(
                "cache_pool_resident_bytes",
                "Bytes charged to the pool, including values evicted but still held by queries.",
            ),
            pool_in_cache: Family::gauge("cache_pool_in_cache_bytes", "Bytes still in the pool."),
            pool_capacity: Family::gauge("cache_pool_capacity_bytes", "Byte capacity of the pool."),
            evicted: Family::counter("cache_eviction_count", "Entries evicted, by reason."),
            evicted_bytes: Family::counter(
                "cache_eviction_bytes",
                "Bytes of entries evicted, by reason.",
            ),
            rejected: Family::counter("cache_insert_rejected_count", "Values refused, by reason."),
        }
    }

    fn add_pool(&mut self, row: &CacheRow) {
        let labels: Labels = vec![("pool", row.pool.map_or("", |p| p.name()).to_string())];
        self.pool_items.push(labels.clone(), row.num_items);
        self.pool_resident.push(labels.clone(), row.size);
        self.pool_in_cache.push(labels.clone(), row.in_cache_size);
        if let Some(cap) = row.capacity {
            self.pool_capacity.push(labels, cap);
        }
    }

    fn add_kind(&mut self, row: &CacheRow) {
        let labels = row_labels(row);
        self.items.push(labels.clone(), row.num_items);
        self.resident.push(labels.clone(), row.size);
        self.in_cache.push(labels.clone(), row.in_cache_size);
        self.enabled.push(labels.clone(), u64::from(row.enabled));
        if let Some(cap) = row.capacity {
            let unit = row.unit.to_string();
            self.capacity.push(with(&labels, "unit", &unit), cap);
        }
        let Some(events) = row.events else {
            return;
        };
        for (reason, count, bytes) in [
            (
                "capacity",
                events.evicted_for_capacity,
                events.evicted_for_capacity_bytes,
            ),
            (
                "memory_pressure",
                events.evicted_for_pressure,
                events.evicted_for_pressure_bytes,
            ),
        ] {
            let labels = with(&labels, "reason", reason);
            self.evicted.push(labels.clone(), count);
            self.evicted_bytes.push(labels, bytes);
        }
        for (reason, count) in [
            ("oversized", events.rejected_oversized),
            ("kind_limit", events.rejected_kind_limit),
            ("pool_full_pinned", events.rejected_pool_full_pinned),
        ] {
            self.rejected.push(with(&labels, "reason", reason), count);
        }
    }

    fn into_vec(self) -> Vec<Family> {
        vec![
            self.items,
            self.resident,
            self.in_cache,
            self.capacity,
            self.enabled,
            self.pool_items,
            self.pool_resident,
            self.pool_in_cache,
            self.pool_capacity,
            self.evicted,
            self.evicted_bytes,
            self.rejected,
        ]
    }
}

/// Counters per kind on disk, labelled like its gauges (`cache_name` `disk_cache_<...>`, `pool`
/// `disk`) and, where given, by `reason`. With `cache_hit_count`, `cache_miss_count` and
/// `cache_miss_bytes` of the same names, they tell what each kind serves, misses, writes and
/// loses, and how much.
fn disk_kind_families(disk: &DiskCache) -> Vec<Family> {
    let counter = Family::counter;
    let mut hit_bytes = counter("cache_disk_hit_bytes", "Bytes served by the disk cache.");
    let mut queued_hits = counter(
        "cache_disk_queued_hit_count",
        "Lookups served from entries still waiting to be written.",
    );
    let mut read_micros = counter(
        "cache_disk_read_microseconds",
        "Time spent reading disk cache files.",
    );
    let mut storage_read = counter(
        "cache_disk_storage_read_bytes",
        "Bytes read from storage on disk cache misses.",
    );
    let mut written = counter(
        "cache_disk_write_count",
        "Entries written to the disk cache.",
    );
    let mut written_bytes = counter("cache_disk_write_bytes", "Bytes written to the disk cache.");
    let mut dropped = counter(
        "cache_disk_drop_count",
        "Entries not written to the disk cache, by reason.",
    );
    let mut evicted = counter(
        "cache_disk_eviction_count",
        "Entries evicted from the disk cache, by reason.",
    );
    let mut evicted_bytes = counter(
        "cache_disk_eviction_bytes",
        "Bytes evicted from the disk cache, by reason.",
    );
    let mut invalidated = counter(
        "cache_disk_invalidation_count",
        "Entries dropped because their object was rewritten or deleted.",
    );
    let mut invalidated_bytes = counter(
        "cache_disk_invalidation_bytes",
        "Bytes dropped because their object was rewritten or deleted.",
    );
    for kind in CacheKind::all().filter(|kind| kind.on_disk()) {
        let stats = disk.kind_stats(kind);
        let labels: Labels = vec![
            ("cache_name", disk.metric_name(kind).to_string()),
            ("pool", "disk".to_string()),
        ];
        hit_bytes.push(labels.clone(), stats.hit_bytes);
        queued_hits.push(labels.clone(), stats.queued_hits);
        read_micros.push(labels.clone(), stats.read_micros);
        storage_read.push(labels.clone(), stats.storage_read_bytes);
        written.push(labels.clone(), stats.events.written);
        written_bytes.push(labels.clone(), stats.events.written_bytes);
        invalidated.push(labels.clone(), stats.events.invalidated);
        invalidated_bytes.push(labels.clone(), stats.events.invalidated_bytes);
        for reason in DropReason::ALL {
            dropped.push(
                with(&labels, "reason", reason.name()),
                stats.dropped[reason as usize],
            );
        }
        for reason in Eviction::ALL {
            let i = reason as usize;
            let reason = with(&labels, "reason", reason.name());
            evicted.push(reason.clone(), stats.events.evicted[i]);
            evicted_bytes.push(reason, stats.events.evicted_bytes[i]);
        }
    }
    vec![
        hit_bytes,
        queued_hits,
        read_micros,
        storage_read,
        written,
        written_bytes,
        dropped,
        evicted,
        evicted_bytes,
        invalidated,
        invalidated_bytes,
    ]
}

impl CacheMetricsCollector {
    /// The families of one read, shared by the scrape and `system.metrics` so the two cannot
    /// disagree.
    fn families(&self) -> Vec<Family> {
        let Some(manager) = self.manager() else {
            return Vec::new();
        };
        let mut families = Families::new();
        let mut disk_pools = Family::gauge(
            "cache_disk_pool_used_bytes",
            "Bytes of the entries in a disk cache pool.",
        );
        let mut disk_pool_items =
            Family::gauge("cache_disk_pool_items", "Entries in a disk cache pool.");
        let mut disk_pool_capacity = Family::gauge(
            "cache_disk_pool_capacity_bytes",
            "Byte capacity of a disk cache pool.",
        );
        for row in manager.composition() {
            match (row.kind, row.disk) {
                (Some(_), _) => families.add_kind(&row),
                (None, false) => families.add_pool(&row),
                (None, true) => {
                    let labels: Labels =
                        vec![("pool", row.pool.map_or("", |p| p.name()).to_string())];
                    disk_pool_items.push(labels.clone(), row.num_items);
                    disk_pools.push(labels.clone(), row.size);
                    if let Some(cap) = row.capacity {
                        disk_pool_capacity.push(labels, cap);
                    }
                }
            }
        }
        let mut families = families.into_vec();
        families.extend([disk_pool_items, disk_pools, disk_pool_capacity]);
        if let Some(disk) = manager.disk() {
            let stats = disk.stats();
            let mut queued = Family::gauge(
                "cache_disk_queue_bytes",
                "Bytes queued to be written to the disk cache.",
            );
            queued.push(Labels::new(), stats.queued_bytes);
            let mut dropped = Family::counter(
                "cache_disk_dropped",
                "Entries not written to the disk cache, by reason.",
            );
            for (reason, count) in [
                ("queue_full", stats.dropped_queue_full),
                ("rejected", stats.dropped_rejected),
                ("io_error", stats.dropped_io_error),
            ] {
                dropped.push(vec![("reason", reason.to_string())], count);
            }
            families.extend([queued, dropped]);
            families.extend(disk_kind_families(&disk));
        }
        if let Some(stats) = manager.reclaim_stats() {
            for (name, help, value) in [
                (
                    "cache_memory_reclaim_count",
                    "Reclaims of cache memory for queries under memory pressure.",
                    stats.count,
                ),
                (
                    "cache_memory_reclaim_bytes",
                    "Estimated bytes of value charges released by reclaims, not physical memory.",
                    stats.freed_bytes,
                ),
                (
                    "cache_memory_reclaim_milliseconds",
                    "Time spent in reclaims.",
                    stats.millis,
                ),
            ] {
                let mut family = Family::counter(name, help);
                family.push(Labels::new(), value);
                families.push(family);
            }
        }
        families
    }
}

impl Collector for CacheMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        for family in self.families() {
            family.encode(&mut encoder)?;
        }
        Ok(())
    }
}

impl SampleCollector for CacheMetricsCollector {
    fn sample(&self, samples: &mut Vec<MetricSample>) {
        for family in self.families() {
            family.sample(samples);
        }
    }
}
