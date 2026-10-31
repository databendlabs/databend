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
//! metrics are scraped. The cache paths only bump plain atomics, which also keeps reclaim safe:
//! it runs inside allocations, possibly while the allocating thread holds a metrics lock.
//!
//! Gauges, per kind and per disk tier, labelled by `cache_name` and `pool` (`meta`, `index`,
//! `data`, or `disk`): `cache_items`, `cache_resident_bytes` (charged, including values evicted
//! but still held by queries), `cache_in_cache_bytes` (still in the cache), `cache_capacity` (in
//! the kind's `unit`, absent when unlimited) and `cache_enabled`. Per pool, shared backend only:
//! `cache_pool_items`, `cache_pool_resident_bytes`, `cache_pool_in_cache_bytes` and
//! `cache_pool_capacity_bytes`.
//!
//! Counters, shared backend only, since the process started:
//! - `cache_eviction_count` and `cache_eviction_bytes` per kind, by `reason`: `capacity` (room in
//!   the pool, a lowered cap or limit) or `memory_pressure` (reclaimed for queries). Explicit
//!   evictions and replacements are not counted.
//! - `cache_admission_deferred_count` per kind: loaded values not cached yet, because the key
//!   had not been read often enough.
//! - `cache_insert_rejected_count` per kind, by `reason`: `oversized`, `kind_limit` or
//!   `pool_full_pinned`.
//! - `cache_memory_reclaim_count`, `cache_memory_reclaim_bytes` (freed) and
//!   `cache_memory_reclaim_milliseconds` (spent).

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Once;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;

use crate::CacheManager;
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
    /// Registers the collector of the global manager, once per process.
    pub(crate) fn register_global() {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            GLOBAL_METRICS_REGISTRY
                .register_collector(Box::new(CacheMetricsCollector { manager: None }));
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
    deferred: Family,
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
            deferred: Family::counter(
                "cache_admission_deferred_count",
                "Loaded values not cached yet: the key had not been read often enough.",
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
        self.deferred
            .push(labels.clone(), events.admission_deferred);
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
            self.deferred,
            self.rejected,
        ]
    }
}

impl Collector for CacheMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let Some(manager) = self.manager() else {
            return Ok(());
        };
        let mut families = Families::new();
        for row in manager.rows() {
            match row.kind {
                Some(_) => families.add_kind(&row),
                None => families.add_pool(&row),
            }
        }
        let mut families = families.into_vec();
        if let Some(stats) = manager.reclaim_stats() {
            for (name, help, value) in [
                (
                    "cache_memory_reclaim_count",
                    "Reclaims of cache memory for queries under memory pressure.",
                    stats.count,
                ),
                (
                    "cache_memory_reclaim_bytes",
                    "Bytes freed by reclaims.",
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
        for family in families {
            family.encode(&mut encoder)?;
        }
        Ok(())
    }
}
