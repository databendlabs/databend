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
        concat!("query_", $key)
    };
}

/// COPY
pub fn metrics_inc_copy_purged_files_counter(c: u32) {
    increment_gauge!(key!("copy_purge_files_counter"), c as f64);
}

pub fn metrics_inc_copy_purged_files_cost_milliseconds(c: u32) {
    increment_gauge!(key!("copy_purged_files_cost_milliseconds"), c as f64);
}

pub fn metrics_inc_copy_commit_data_counter(c: u32) {
    increment_gauge!(key!("copy_commit_data_counter"), c as f64);
}

pub fn metrics_inc_copy_commit_data_cost_milliseconds(c: u32) {
    increment_gauge!(key!("copy_commit_data_cost_milliseconds"), c as f64);
}

pub fn metrics_inc_copy_append_data_counter(c: u32) {
    increment_gauge!(key!("copy_append_data_counter"), c as f64);
}

pub fn metrics_inc_copy_append_data_cost_milliseconds(c: u32) {
    increment_gauge!(key!("copy_append_data_cost_milliseconds"), c as f64);
}

pub fn metrics_inc_copy_read_file_counter(c: u32) {
    increment_gauge!(key!("copy_read_stage_file_counter"), c as f64);
}

pub fn metrics_inc_copy_read_file_cost_milliseconds(c: u32) {
    increment_gauge!(key!("copy_read_file_cost_milliseconds"), c as f64);
}
