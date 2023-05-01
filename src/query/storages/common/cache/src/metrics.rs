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

use metrics::increment_gauge;

fn key_str(cache_name: &str, action: &str) -> String {
    format!("cache_{cache_name}_{action}")
}

pub fn metrics_inc_cache_access_count(c: u64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "access_count"), c as f64);
}

pub fn metrics_inc_cache_miss_count(c: u64, cache_name: &str) {
    // increment_gauge!(key!("memory_miss_count"), c as f64);
    increment_gauge!(key_str(cache_name, "miss_count"), c as f64);
}

// When cache miss, load time cost.
pub fn metrics_inc_cache_miss_load_millisecond(c: u64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "miss_load_millisecond"), c as f64);
}

pub fn metrics_inc_cache_hit_count(c: u64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "hit_count"), c as f64);
}

pub fn metrics_inc_cache_population_pending_count(c: i64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "population_pending_count"), c as f64);
}

pub fn metrics_inc_cache_population_overflow_count(c: i64, cache_name: &str) {
    increment_gauge!(key_str(cache_name, "population_overflow_count"), c as f64);
}
