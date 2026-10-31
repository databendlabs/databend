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

//! Gauges of what the cache holds, read from the cache manager when metrics are scraped, so
//! keeping them costs nothing on the cache paths.
//!
//! Per kind and per disk tier, labelled by `cache_name` and `pool` (`meta`, `index`, `data`, or
//! `disk`): `cache_items`, `cache_resident_bytes` (charged, including values evicted but still
//! held by queries), `cache_in_cache_bytes` (still in the cache), `cache_capacity` (in the
//! kind's `unit`, absent when unlimited) and `cache_enabled`. Per pool, shared backend only:
//! `cache_pool_items`, `cache_pool_resident_bytes`, `cache_pool_in_cache_bytes` and
//! `cache_pool_capacity_bytes`.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Once;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::gauge::ConstGauge;

use crate::CacheManager;
use crate::manager::CacheRow;

/// Collects the gauges of one manager, or of the global one.
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

struct GaugeFamily {
    name: &'static str,
    help: &'static str,
    samples: Vec<(Labels, u64)>,
}

impl GaugeFamily {
    fn new(name: &'static str, help: &'static str) -> Self {
        GaugeFamily {
            name,
            help,
            samples: Vec::new(),
        }
    }

    fn encode(&self, encoder: &mut DescriptorEncoder) -> Result<(), std::fmt::Error> {
        if self.samples.is_empty() {
            return Ok(());
        }
        let mut family =
            encoder.encode_descriptor(self.name, self.help, None, MetricType::Gauge)?;
        for (labels, value) in &self.samples {
            ConstGauge::new(*value as i64).encode(family.encode_family(labels)?)?;
        }
        Ok(())
    }
}

fn row_labels(row: &CacheRow) -> Labels {
    let pool = row.pool.map_or("disk", |pool| pool.name());
    vec![("cache_name", row.name.clone()), ("pool", pool.to_string())]
}

impl Collector for CacheMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let Some(manager) = self.manager() else {
            return Ok(());
        };
        let mut items = GaugeFamily::new("cache_items", "Entries in the cache.");
        let mut resident = GaugeFamily::new(
            "cache_resident_bytes",
            "Bytes charged, including values evicted but still held by queries.",
        );
        let mut in_cache = GaugeFamily::new("cache_in_cache_bytes", "Bytes still in the cache.");
        let mut capacity = GaugeFamily::new(
            "cache_capacity",
            "Capacity in the unit of the unit label; absent when unlimited.",
        );
        let mut enabled = GaugeFamily::new("cache_enabled", "1 when the cache is on.");
        let mut pool_items = GaugeFamily::new("cache_pool_items", "Entries in the pool.");
        let mut pool_resident = GaugeFamily::new(
            "cache_pool_resident_bytes",
            "Bytes charged to the pool, including values evicted but still held by queries.",
        );
        let mut pool_in_cache =
            GaugeFamily::new("cache_pool_in_cache_bytes", "Bytes still in the pool.");
        let mut pool_capacity =
            GaugeFamily::new("cache_pool_capacity_bytes", "Byte capacity of the pool.");

        for row in manager.rows() {
            if row.kind.is_none() {
                let labels: Labels = vec![("pool", row.pool.map_or("", |p| p.name()).to_string())];
                pool_items.samples.push((labels.clone(), row.num_items));
                pool_resident.samples.push((labels.clone(), row.size));
                pool_in_cache
                    .samples
                    .push((labels.clone(), row.in_cache_size));
                if let Some(cap) = row.capacity {
                    pool_capacity.samples.push((labels, cap));
                }
                continue;
            }
            let labels = row_labels(&row);
            items.samples.push((labels.clone(), row.num_items));
            resident.samples.push((labels.clone(), row.size));
            in_cache.samples.push((labels.clone(), row.in_cache_size));
            enabled
                .samples
                .push((labels.clone(), u64::from(row.enabled)));
            if let Some(cap) = row.capacity {
                let mut labels = labels;
                labels.push(("unit", row.unit.to_string()));
                capacity.samples.push((labels, cap));
            }
        }
        for family in [
            items,
            resident,
            in_cache,
            capacity,
            enabled,
            pool_items,
            pool_resident,
            pool_in_cache,
            pool_capacity,
        ] {
            family.encode(&mut encoder)?;
        }
        Ok(())
    }
}
