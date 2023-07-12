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

macro_rules! key {
    ($key: literal) => {
        concat!("transform_", $key)
    };
}

pub fn metrics_inc_aggregate_partial_spill_count() {
    increment_gauge!(key!("aggregate_partial_spill_count"), 1_f64);
}

pub fn metrics_inc_aggregate_partial_spill_cell_count(c: u64) {
    increment_gauge!(key!("aggregate_partial_spill_cell_count"), c as f64);
}

pub fn metrics_inc_aggregate_partial_hashtable_allocated_bytes(c: u64) {
    increment_gauge!(
        key!("aggregate_partial_hashtable_allocated_bytes"),
        c as f64
    );
}

pub fn metrics_inc_group_by_spill_write_count() {
    increment_gauge!(key!("group_by_spill_write_count"), 1_f64);
}

pub fn metrics_inc_group_by_spill_write_bytes(c: u64) {
    increment_gauge!(key!("group_by_spill_write_bytes"), c as f64);
}

pub fn metrics_inc_group_by_spill_write_milliseconds(c: u64) {
    increment_gauge!(key!("group_by_spill_write_milliseconds"), c as f64);
}

pub fn metrics_inc_aggregate_spill_write_count() {
    increment_gauge!(key!("aggregate_spill_write_count"), 1_f64);
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
