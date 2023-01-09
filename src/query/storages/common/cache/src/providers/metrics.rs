// Copyright 2022 Datafuse Labs.
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

use metrics::gauge;
use metrics::increment_gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("cache_", $key)
    };
}

/// ByPass Cache Metrics
pub fn metrics_inc_bypass_writes(c: u64) {
    increment_gauge!(key!("bypass_writes"), c as f64);
}

pub fn metrics_inc_bypass_write_milliseconds(c: u64) {
    increment_gauge!(key!("bypass_write_milliseconds"), c as f64);
}

pub fn metrics_inc_bypass_reads(c: u64) {
    increment_gauge!(key!("bypass_reads"), c as f64);
}

pub fn metrics_inc_bypass_read_milliseconds(c: u64) {
    increment_gauge!(key!("bypass_read_milliseconds"), c as f64);
}

pub fn metrics_inc_bypass_removes(c: u64) {
    increment_gauge!(key!("bypass_removes"), c as f64);
}

pub fn metrics_inc_bypass_remove_milliseconds(c: u64) {
    increment_gauge!(key!("bypass_remove_milliseconds"), c as f64);
}

/// Memory Item Cache Metrics
pub fn metrics_inc_memory_item_hits(c: u64) {
    increment_gauge!(key!("memory_item_hits"), c as f64);
}

pub fn metrics_inc_memory_item_misses(c: u64) {
    increment_gauge!(key!("memory_item_misses"), c as f64);
}

pub fn metrics_inc_memory_item_load_milliseconds(c: u64) {
    increment_gauge!(key!("memory_item_load_milliseconds"), c as f64);
}

pub fn metrics_inc_memory_item_writes(c: u64) {
    increment_gauge!(key!("memory_item_writes"), c as f64);
}

pub fn metrics_inc_memory_item_write_milliseconds(c: u64) {
    increment_gauge!(key!("memory_item_write_milliseconds"), c as f64);
}

pub fn metrics_inc_memory_item_removes(c: u64) {
    increment_gauge!(key!("memory_item_removes"), c as f64);
}

pub fn metrics_inc_memory_item_remove_milliseconds(c: u64) {
    increment_gauge!(key!("memory_item_remove_milliseconds"), c as f64);
}

/// Memory Bytes Cache Metrics
pub fn metrics_inc_memory_bytes_hits(c: u64) {
    increment_gauge!(key!("memory_bytes_hits"), c as f64);
}

pub fn metrics_inc_memory_bytes_misses(c: u64) {
    increment_gauge!(key!("memory_bytes_misses"), c as f64);
}

pub fn metrics_inc_memory_bytes_load_milliseconds(c: u64) {
    increment_gauge!(key!("memory_bytes_load_milliseconds"), c as f64);
}

pub fn metrics_inc_memory_bytes_writes(c: u64) {
    increment_gauge!(key!("memory_bytes_writes"), c as f64);
}

pub fn metrics_inc_memory_bytes_write_milliseconds(c: u64) {
    increment_gauge!(key!("memory_bytes_write_milliseconds"), c as f64);
}

pub fn metrics_inc_memory_bytes_removes(c: u64) {
    increment_gauge!(key!("memory_bytes_removes"), c as f64);
}

pub fn metrics_inc_memory_bytes_remove_milliseconds(c: u64) {
    increment_gauge!(key!("memory_bytes_remove_milliseconds"), c as f64);
}

pub fn metrics_reset() {
    let c = 0 as f64;

    // Bypass metrics.
    gauge!(key!("bypass_writes"), c);
    gauge!(key!("bypass_write_milliseconds"), c);
    gauge!(key!("bypass_reads"), c);
    gauge!(key!("bypass_read_milliseconds"), c);
    gauge!(key!("bypass_removes"), c);
    gauge!(key!("bypass_remove_milliseconds"), c);

    // Memory item metrics.
    gauge!(key!("memory_item_hits"), c);
    gauge!(key!("memory_item_misses"), c);
    gauge!(key!("memory_item_load_milliseconds"), c);
    gauge!(key!("memory_item_writes"), c);
    gauge!(key!("memory_item_write_milliseconds"), c);
    gauge!(key!("memory_item_removes"), c);
    gauge!(key!("memory_item_remove_milliseconds"), c);

    // Memory bytes metrics.
    gauge!(key!("memory_bytes_hits"), c);
    gauge!(key!("memory_bytes_misses"), c);
    gauge!(key!("memory_bytes_load_milliseconds"), c);
    gauge!(key!("memory_bytes_writes"), c);
    gauge!(key!("memory_bytes_write_milliseconds"), c);
    gauge!(key!("memory_bytes_removes"), c);
    gauge!(key!("memory_bytes_remove_milliseconds"), c);
}
