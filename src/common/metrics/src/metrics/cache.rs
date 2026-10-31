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

use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyHistogram;
use databend_common_base::runtime::metrics::Histogram;
use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family_in_milliseconds;
use databend_common_base::runtime::metrics::register_histogram_in_milliseconds;
use prometheus_client::encoding::EncodeLabelSet;

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheLabels {
    cache_name: String,
}

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheReasonLabels {
    cache_name: String,
    reason: String,
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
static CACHE_EVICTION_COUNT: LazyLock<FamilyCounter<CacheReasonLabels>> =
    LazyLock::new(|| register_counter_family("cache_eviction_count"));
static CACHE_EVICTION_BYTES: LazyLock<FamilyCounter<CacheReasonLabels>> =
    LazyLock::new(|| register_counter_family("cache_eviction_bytes"));
static CACHE_ADMISSION_DEFERRED_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_admission_deferred_count"));
static CACHE_INSERT_REJECTED_COUNT: LazyLock<FamilyCounter<CacheReasonLabels>> =
    LazyLock::new(|| register_counter_family("cache_insert_rejected_count"));
static CACHE_MEMORY_RECLAIM_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("cache_memory_reclaim_count"));
static CACHE_MEMORY_RECLAIM_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("cache_memory_reclaim_bytes"));
static CACHE_MEMORY_RECLAIM_MILLISECOND: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("cache_memory_reclaim_millisecond"));

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

pub fn metrics_inc_cache_population_overflow_count(c: i64, cache_name: &str) {
    CACHE_POPULATION_OVERFLOW_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}

fn reason_labels(cache_name: &str, reason: &str) -> CacheReasonLabels {
    CacheReasonLabels {
        cache_name: cache_name.to_string(),
        reason: reason.to_string(),
    }
}

/// Entries of `cache_name` evicted for `reason`: `capacity` (room for other entries, or a
/// lowered limit) or `memory_pressure` (reclaimed for queries).
pub fn metrics_inc_cache_eviction(cache_name: &str, reason: &str, count: u64, bytes: u64) {
    let labels = reason_labels(cache_name, reason);
    CACHE_EVICTION_COUNT.get_or_create(&labels).inc_by(count);
    CACHE_EVICTION_BYTES.get_or_create(&labels).inc_by(bytes);
}

pub fn get_cache_eviction_count(cache_name: &str, reason: &str) -> u64 {
    CACHE_EVICTION_COUNT
        .get(&reason_labels(cache_name, reason))
        .map(|v| v.get())
        .unwrap_or_default()
}

pub fn get_cache_eviction_bytes(cache_name: &str, reason: &str) -> u64 {
    CACHE_EVICTION_BYTES
        .get(&reason_labels(cache_name, reason))
        .map(|v| v.get())
        .unwrap_or_default()
}

/// A loaded value not cached yet: the key has not been read often enough to be admitted.
pub fn metrics_inc_cache_admission_deferred_count(cache_name: &str) {
    CACHE_ADMISSION_DEFERRED_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc();
}

pub fn get_cache_admission_deferred_count(cache_name: &str) -> u64 {
    get_metric_count_by_name(&CACHE_ADMISSION_DEFERRED_COUNT, cache_name)
}

/// A value refused by the cache, for `reason`.
pub fn metrics_inc_cache_insert_rejected_count(cache_name: &str, reason: &str) {
    CACHE_INSERT_REJECTED_COUNT
        .get_or_create(&reason_labels(cache_name, reason))
        .inc();
}

pub fn get_cache_insert_rejected_count(cache_name: &str, reason: &str) -> u64 {
    CACHE_INSERT_REJECTED_COUNT
        .get(&reason_labels(cache_name, reason))
        .map(|v| v.get())
        .unwrap_or_default()
}

/// One reclaim of cache memory for queries under memory pressure.
pub fn metrics_inc_cache_memory_reclaim(freed_bytes: u64, millis: u64) {
    CACHE_MEMORY_RECLAIM_COUNT.inc();
    CACHE_MEMORY_RECLAIM_BYTES.inc_by(freed_bytes);
    CACHE_MEMORY_RECLAIM_MILLISECOND.observe(millis as f64);
}

pub fn get_cache_memory_reclaim_count() -> u64 {
    CACHE_MEMORY_RECLAIM_COUNT.get()
}

pub fn get_cache_memory_reclaim_bytes() -> u64 {
    CACHE_MEMORY_RECLAIM_BYTES.get()
}
