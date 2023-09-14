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

use common_metrics::register_counter;
use common_metrics::register_histogram_in_milliseconds;
use common_metrics::Counter;
use common_metrics::Histogram;
use lazy_static::lazy_static;
use metrics::increment_gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("transform_", $key)
    };
}

lazy_static! {
    static ref AGGREGATE_PARTIAL_SPILL_COUNT: Counter =
        register_counter(key!("aggregate_partial_spill_count"));
    static ref AGGREGATE_PARTIAL_SPILL_CELL_COUNT: Counter =
        register_counter(key!("aggregate_partial_spill_cell_count"));
    static ref AGGREGATE_PARTIAL_HASHTABLE_ALLOCATED_BYTES: Counter =
        register_counter(key!("aggregate_partial_hashtable_allocated_bytes"));
    static ref GROUP_BY_SPILL_WRITE_COUNT: Counter =
        register_counter(key!("group_by_spill_write_count"));
    static ref GROUP_BY_SPILL_WRITE_BYTES: Counter =
        register_counter(key!("group_by_spill_write_bytes"));
    static ref AGGREGATE_SPILL_WRITE_COUNT: Counter =
        register_counter(key!("aggregate_spill_write_count"));
    static ref GROUP_BY_SPILL_WRITE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("group_by_spill_write_milliseconds"));
    static ref AGGREGATE_SPILL_READ_COUNT: Counter =
        register_counter(key!("aggregate_spill_read_count"));
    static ref AGGREGATE_SPILL_READ_BYTES: Counter =
        register_counter(key!("aggregate_spill_read_bytes"));
    static ref AGGREGATE_SPILL_READ_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("aggregate_spill_read_milliseconds"));
    static ref AGGREGATE_SPILL_DATA_DESERIALIZE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("aggregate_spill_data_deserialize_milliseconds"));
    static ref AGGREGATE_SPILL_DATA_SERIALIZE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("aggregate_spill_data_serialize_milliseconds"));
}

pub fn metrics_inc_aggregate_partial_spill_count() {
    increment_gauge!(key!("aggregate_partial_spill_count"), 1_f64);
    AGGREGATE_PARTIAL_SPILL_COUNT.inc();
}

pub fn metrics_inc_aggregate_partial_spill_cell_count(c: u64) {
    increment_gauge!(key!("aggregate_partial_spill_cell_count"), c as f64);
    AGGREGATE_PARTIAL_SPILL_CELL_COUNT.inc_by(c);
}

pub fn metrics_inc_aggregate_partial_hashtable_allocated_bytes(c: u64) {
    increment_gauge!(
        key!("aggregate_partial_hashtable_allocated_bytes"),
        c as f64
    );
    AGGREGATE_PARTIAL_HASHTABLE_ALLOCATED_BYTES.inc_by(c)
}

pub fn metrics_inc_group_by_spill_write_count() {
    increment_gauge!(key!("group_by_spill_write_count"), 1_f64);
    GROUP_BY_SPILL_WRITE_COUNT.inc();
}

pub fn metrics_inc_group_by_spill_write_bytes(c: u64) {
    increment_gauge!(key!("group_by_spill_write_bytes"), c as f64);
    GROUP_BY_SPILL_WRITE_BYTES.inc_by(c);
}

pub fn metrics_inc_group_by_spill_write_milliseconds(c: u64) {
    increment_gauge!(key!("group_by_spill_write_milliseconds"), c as f64);
    GROUP_BY_SPILL_WRITE_MILLISECONDS.observe(c as f64)
}

pub fn metrics_inc_aggregate_spill_write_count() {
    increment_gauge!(key!("aggregate_spill_write_count"), 1_f64);
    AGGREGATE_SPILL_WRITE_COUNT.inc();
}

pub fn metrics_inc_aggregate_spill_write_bytes(c: u64) {
    increment_gauge!(key!("aggregate_spill_write_bytes"), c as f64);
}

pub fn metrics_inc_aggregate_spill_write_milliseconds(c: u64) {
    increment_gauge!(key!("aggregate_spill_write_milliseconds"), c as f64);
}

pub fn metrics_inc_aggregate_spill_read_count() {
    increment_gauge!(key!("aggregate_spill_read_count"), 1_f64);
}

pub fn metrics_inc_aggregate_spill_read_bytes(c: u64) {
    increment_gauge!(key!("aggregate_spill_read_bytes"), c as f64);
}

pub fn metrics_inc_aggregate_spill_read_milliseconds(c: u64) {
    increment_gauge!(key!("aggregate_spill_read_milliseconds"), c as f64);
}

pub fn metrics_inc_aggregate_spill_data_serialize_milliseconds(c: u64) {
    increment_gauge!(
        key!("aggregate_spill_data_serialize_milliseconds"),
        c as f64
    );
}

pub fn metrics_inc_aggregate_spill_data_deserialize_milliseconds(c: u64) {
    increment_gauge!(
        key!("aggregate_spill_data_deserialize_milliseconds"),
        c as f64
    );
}
