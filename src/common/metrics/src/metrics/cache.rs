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

use std::sync::LazyLock;

use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyCounterMember;
use databend_common_base::runtime::metrics::FamilyHistogram;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family_in_milliseconds;
use prometheus_client::encoding::EncodeLabelSet;

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheLabels {
    cache_name: String,
}

static CACHE_ACCESS_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_access_count"));
static CACHE_MISS_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_miss_count"));
static CACHE_MISS_BYTES: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_miss_bytes"));
static CACHE_MISS_LOAD_MILLISECOND: LazyLock<FamilyHistogram<CacheLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds("cache_miss_load_millisecond"));
static CACHE_HIT_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_hit_count"));
static CACHE_POPULATION_PENDING_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_population_pending_count"));
static CACHE_POPULATION_OVERFLOW_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_population_overflow_count"));
static CACHE_WRITE_SKIPPED_BYTES: LazyLock<FamilyCounter<CacheWriteSkipLabels>> =
    LazyLock::new(|| register_counter_family("cache_write_skipped_bytes"));

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheWriteSkipLabels {
    cache_name: String,
    reason: String,
}

/// The lookup counters of one cache, looked up once: counting a lookup is then an atomic
/// addition per counter, with no label to build and find. For caches counted on every lookup;
/// counts the same metrics as the `metrics_inc_cache_*` functions.
pub struct CacheLookupCounters {
    access: FamilyCounterMember<CacheLabels>,
    hit: FamilyCounterMember<CacheLabels>,
    miss: FamilyCounterMember<CacheLabels>,
    miss_bytes: FamilyCounterMember<CacheLabels>,
}

impl CacheLookupCounters {
    pub fn new(cache_name: &str) -> Self {
        let member = |family: &FamilyCounter<CacheLabels>| {
            family.member(CacheLabels {
                cache_name: cache_name.to_string(),
            })
        };
        CacheLookupCounters {
            access: member(&CACHE_ACCESS_COUNT),
            hit: member(&CACHE_HIT_COUNT),
            miss: member(&CACHE_MISS_COUNT),
            miss_bytes: member(&CACHE_MISS_BYTES),
        }
    }

    pub fn access(&self) {
        self.access.inc_by(1);
    }

    pub fn hit(&self) {
        self.hit.inc_by(1);
    }

    pub fn miss(&self) {
        self.miss.inc_by(1);
    }

    pub fn miss_bytes(&self, bytes: u64) {
        self.miss_bytes.inc_by(bytes);
    }
}

pub fn get_cache_access_count(cache_name: &str) -> u64 {
    get_metric_count_by_name(&CACHE_ACCESS_COUNT, cache_name)
}

pub fn get_cache_hit_count(cache_name: &str) -> u64 {
    get_metric_count_by_name(&CACHE_HIT_COUNT, cache_name)
}

pub fn get_cache_miss_count(cache_name: &str) -> u64 {
    get_metric_count_by_name(&CACHE_MISS_COUNT, cache_name)
}

fn get_metric_count_by_name(
    metric: &LazyLock<FamilyCounter<CacheLabels>>,
    cache_name: &str,
) -> u64 {
    metric
        .get(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .map(|v| v.get())
        .unwrap_or_default()
}

pub fn metrics_inc_cache_access_count(c: u64, cache_name: &str) {
    CACHE_ACCESS_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_miss_count(c: u64, cache_name: &str) {
    CACHE_MISS_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_miss_bytes(c: u64, cache_name: &str) {
    CACHE_MISS_BYTES
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

// When cache miss, load time cost.
pub fn metrics_inc_cache_miss_load_millisecond(c: u64, cache_name: &str) {
    CACHE_MISS_LOAD_MILLISECOND
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .observe(c as f64);
}

pub fn metrics_inc_cache_hit_count(c: u64, cache_name: &str) {
    CACHE_HIT_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_population_pending_count(c: i64, cache_name: &str) {
    CACHE_POPULATION_PENDING_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}

/// Bytes a read decoded but did not put in `cache_name`, by the reason it was held back.
pub fn metrics_inc_cache_write_skipped_bytes(c: u64, cache_name: &str, reason: &str) {
    CACHE_WRITE_SKIPPED_BYTES
        .get_or_create(&CacheWriteSkipLabels {
            cache_name: cache_name.to_string(),
            reason: reason.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_population_overflow_count(c: i64, cache_name: &str) {
    CACHE_POPULATION_OVERFLOW_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counters count into the metrics the name-based functions read and update.
    #[test]
    fn test_lookup_counters_count_the_named_metrics() {
        let name = "test_lookup_counters_cache";
        let counters = CacheLookupCounters::new(name);
        counters.access();
        counters.access();
        counters.hit();
        counters.miss();
        metrics_inc_cache_hit_count(1, name);
        assert_eq!(get_cache_access_count(name), 2);
        assert_eq!(get_cache_hit_count(name), 2);
        assert_eq!(get_cache_miss_count(name), 1);
    }
}
