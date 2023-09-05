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

use common_metrics::register_counter_family;
use common_metrics::register_histogram_in_milliseconds;
use common_metrics::Counter;
use common_metrics::Family;
use lazy_static::lazy_static;
use metrics::increment_gauge;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::histogram::Histogram;

fn key_str(cache_name: &str, action: &str) -> String {
    format!("cache_{cache_name}_{action}")
}

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheLabels {
    cache_name: String,
}

lazy_static! {
    static ref CACHE_ACCESS_COUNT: Family<CacheLabels, Counter> =
        register_counter_family("cache_access_count");
    static ref CACHE_MISS_COUNT: Family<CacheLabels, Counter> =
        register_counter_family("cache_miss_count");
    static ref CACHE_MISS_LOAD_MILLISECOND: Family<CacheLabels, Histogram> =
        register_histogram_in_milliseconds("cache_miss_load_millisecond");
    static ref CACHE_HIT_COUNT: Family<CacheLabels, Counter> =
        register_counter_family("cache_hit_count");
    static ref CACHE_POPULATION_PENDING_COUNT: Family<CacheLabels, Counter> =
        register_counter_family("cache_population_pending_count");
    static ref CACHE_POPULATION_OVERFLOW_COUNT: Family<CacheLabels, Counter> =
        register_counter_family("cache_population_overflow_count");
}

pub fn metrics_inc_cache_access_count(c: u64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "access_count"), c as f64);
    CACHE_ACCESS_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_miss_count(c: u64, cache_name: &str) {
    // increment_gauge!(key!("memory_miss_count"), c as f64);
    increment_gauge!(key_str(cache_name, "miss_count"), c as f64);
    CACHE_MISS_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

// When cache miss, load time cost.
pub fn metrics_inc_cache_miss_load_millisecond(c: u64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "miss_load_millisecond"), c as f64);
    CACHE_MISS_LOAD_MILLISECOND
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .observe(c as f64);
}

pub fn metrics_inc_cache_hit_count(c: u64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "hit_count"), c as f64);
    CACHE_HIT_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_population_pending_count(c: i64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "population_pending_count"), c as f64);
    CACHE_POPULATION_PENDING_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}

pub fn metrics_inc_cache_population_overflow_count(c: i64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "population_overflow_count"), c as f64);
    CACHE_POPULATION_OVERFLOW_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}
