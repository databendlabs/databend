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

use crate::register_counter;
use crate::register_counter_family;
use crate::register_histogram_family_in_milliseconds;
use crate::Counter;
use crate::Family;
use crate::Histogram;
use crate::VecLabels;

pub static AGGREGATE_PARTIAL_CELL_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_aggregate_partial_cell_count"));

pub static AGGREGATE_PARTIAL_SPILL_CELL_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_aggregate_partial_spill_cell_count"));
pub static AGGREGATE_PARTIAL_HASHTABLE_ALLOCATED_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_aggregate_partial_hashtable_allocated_bytes"));
pub static AGGREGATE_PARTIAL_HASHTABLE_EXCHANGE_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_aggregate_partial_hashtable_exchange_rows"));

pub static SPILL_COUNT: LazyLock<Family<VecLabels, Counter>> =
    LazyLock::new(|| register_counter_family("transform_spill_count"));
pub static SPILL_WRITE_COUNT: LazyLock<Family<VecLabels, Counter>> =
    LazyLock::new(|| register_counter_family("transform_spill_write_count"));
pub static SPILL_WRITE_BYTES: LazyLock<Family<VecLabels, Counter>> =
    LazyLock::new(|| register_counter_family("transform_spill_write_bytes"));
pub static SPILL_WRITE_MILLISECONDS: LazyLock<Family<VecLabels, Histogram>> = LazyLock::new(|| {
    register_histogram_family_in_milliseconds("transform_spill_write_milliseconds")
});
pub static SPILL_READ_COUNT: LazyLock<Family<VecLabels, Counter>> =
    LazyLock::new(|| register_counter_family("transform_spill_read_count"));
pub static SPILL_READ_BYTES: LazyLock<Family<VecLabels, Counter>> =
    LazyLock::new(|| register_counter_family("transform_spill_read_bytes"));
pub static SPILL_READ_MILLISECONDS: LazyLock<Family<VecLabels, Histogram>> = LazyLock::new(|| {
    register_histogram_family_in_milliseconds("transform_spill_read_milliseconds")
});
pub static SPILL_DATA_DESERIALIZE_MILLISECONDS: LazyLock<Family<VecLabels, Histogram>> =
    LazyLock::new(|| {
        register_histogram_family_in_milliseconds("transform_spill_data_deserialize_milliseconds")
    });
pub static SPILL_DATA_SERIALIZE_MILLISECONDS: LazyLock<Family<VecLabels, Histogram>> =
    LazyLock::new(|| {
        register_histogram_family_in_milliseconds("transform_spill_data_serialize_milliseconds")
    });

// Cluster exchange metrics.
pub static EXCHANGE_WRITE_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_exchange_write_count"));
pub static EXCHANGE_WRITE_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_exchange_write_bytes"));
pub static EXCHANGE_READ_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_exchange_read_count"));
pub static EXCHANGE_READ_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("transform_exchange_read_bytes"));

pub fn metrics_inc_aggregate_partial_spill_count() {
    let labels = &vec![("spill", "aggregate_partial_spill".to_string())];
    SPILL_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_aggregate_partial_spill_cell_count(c: u64) {
    AGGREGATE_PARTIAL_SPILL_CELL_COUNT.inc_by(c);
}

pub fn metrics_inc_aggregate_partial_hashtable_exchange_rows(c: u64) {
    AGGREGATE_PARTIAL_HASHTABLE_EXCHANGE_ROWS.inc_by(c);
}

pub fn metrics_inc_aggregate_partial_hashtable_allocated_bytes(c: u64) {
    AGGREGATE_PARTIAL_HASHTABLE_ALLOCATED_BYTES.inc_by(c);
}

pub fn metrics_inc_group_by_partial_spill_count() {
    let labels = &vec![("spill", "group_by_partial_spill".to_string())];
    SPILL_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_group_by_partial_spill_cell_count(c: u64) {
    AGGREGATE_PARTIAL_SPILL_CELL_COUNT.inc_by(c);
}

pub fn metrics_inc_group_by_partial_hashtable_allocated_bytes(c: u64) {
    AGGREGATE_PARTIAL_HASHTABLE_ALLOCATED_BYTES.inc_by(c);
}

pub fn metrics_inc_group_by_spill_write_count() {
    let labels = &vec![("spill", "group_by_spill".to_string())];
    SPILL_WRITE_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_group_by_spill_write_bytes(c: u64) {
    let labels = &vec![("spill", "group_by_spill".to_string())];
    SPILL_WRITE_BYTES.get_or_create(labels).inc_by(c);
}

pub fn metrics_inc_group_by_spill_write_milliseconds(c: u64) {
    let labels = &vec![("spill", "group_by_spill".to_string())];
    SPILL_WRITE_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64)
}

pub fn metrics_inc_aggregate_spill_write_count() {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_WRITE_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_aggregate_spill_write_bytes(c: u64) {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_WRITE_BYTES.get_or_create(labels).inc_by(c);
}

pub fn metrics_inc_aggregate_spill_write_milliseconds(c: u64) {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_WRITE_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64);
}

pub fn metrics_inc_aggregate_spill_read_count() {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_READ_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_aggregate_spill_read_bytes(c: u64) {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_READ_BYTES.get_or_create(labels).inc_by(c);
}

pub fn metrics_inc_aggregate_spill_read_milliseconds(c: u64) {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_READ_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64);
}

pub fn metrics_inc_aggregate_spill_data_serialize_milliseconds(c: u64) {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_DATA_SERIALIZE_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64);
}

pub fn metrics_inc_aggregate_spill_data_deserialize_milliseconds(c: u64) {
    let labels = &vec![("spill", "aggregate_spill".to_string())];
    SPILL_DATA_DESERIALIZE_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64);
}

// Cluster exchange metrics.
pub fn metrics_inc_exchange_write_count(v: usize) {
    EXCHANGE_WRITE_COUNT.inc_by(v as u64);
}

pub fn metrics_inc_exchange_write_bytes(c: usize) {
    EXCHANGE_WRITE_BYTES.inc_by(c as u64);
}

pub fn metrics_inc_exchange_read_count(v: usize) {
    EXCHANGE_READ_COUNT.inc_by(v as u64);
}

pub fn metrics_inc_exchange_read_bytes(c: usize) {
    EXCHANGE_READ_BYTES.inc_by(c as u64);
}

// Sort spill metrics
pub fn metrics_inc_sort_spill_count() {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_sort_spill_write_count() {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_WRITE_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_sort_spill_write_bytes(c: u64) {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_WRITE_BYTES.get_or_create(labels).inc_by(c);
}

pub fn metrics_inc_sort_spill_write_milliseconds(c: u64) {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_WRITE_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64)
}

pub fn metrics_inc_sort_spill_read_count() {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_READ_COUNT.get_or_create(labels).inc();
}

pub fn metrics_inc_sort_spill_read_bytes(c: u64) {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_READ_BYTES.get_or_create(labels).inc_by(c);
}

pub fn metrics_inc_sort_spill_read_milliseconds(c: u64) {
    let labels = &vec![("spill", "sort_spill".to_string())];
    SPILL_READ_MILLISECONDS
        .get_or_create(labels)
        .observe(c as f64);
}
