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
        concat!("query_", $key)
    };
}

lazy_static! {
    static ref COPY_PURGE_FILE_COUNTER: Counter = register_counter("copy_purge_file_counter");
    static ref COPY_PURGE_FILE_COST_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_purge_file_cost_milliseconds");
    static ref COPY_READ_PART_COUNTER: Counter = register_counter("copy_read_part_counter");
    static ref COPY_READ_SIZE_BYTES: Counter = register_counter("copy_read_size_bytes");
    static ref COPY_READ_PART_COST_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_read_part_cost_milliseconds");
    static ref FILTER_OUT_COPIED_FILES_REQUEST_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("filter_out_copied_files_request_milliseconds");
    static ref FILTER_OUT_COPIED_FILES_ENTIRE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("filter_out_copied_files_entire_milliseconds");
    static ref COLLECT_FILES_GET_ALL_SOURCE_FILES_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("collect_files_get_all_source_files_milliseconds");
}

/// COPY
pub fn metrics_inc_copy_purge_files_counter(c: u32) {
    increment_gauge!(key!("copy_purge_file_counter"), c as f64);
    COPY_PURGE_FILE_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_copy_purge_files_cost_milliseconds(c: u32) {
    increment_gauge!(key!("copy_purge_file_cost_milliseconds"), c as f64);
    COPY_PURGE_FILE_COST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_copy_read_part_counter() {
    increment_gauge!(key!("copy_read_part_counter"), 1.0);
    COPY_READ_PART_COUNTER.inc();
}

pub fn metrics_inc_copy_read_size_bytes(c: u64) {
    increment_gauge!(key!("copy_read_size_bytes"), c as f64);
    COPY_READ_SIZE_BYTES.inc_by(c);
}

pub fn metrics_inc_copy_read_part_cost_milliseconds(c: u64) {
    increment_gauge!(key!("copy_read_part_cost_milliseconds"), c as f64);
    COPY_READ_PART_COST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_filter_out_copied_files_request_milliseconds(c: u64) {
    increment_gauge!(
        key!("filter_out_copied_files_request_milliseconds"),
        c as f64
    );
    FILTER_OUT_COPIED_FILES_REQUEST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_filter_out_copied_files_entire_milliseconds(c: u64) {
    increment_gauge!(
        key!("filter_out_copied_files_entire_milliseconds"),
        c as f64
    );
    FILTER_OUT_COPIED_FILES_ENTIRE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_collect_files_get_all_source_files_milliseconds(c: u64) {
    increment_gauge!(
        key!("collect_files_get_all_source_files_milliseconds"),
        c as f64
    );
    COLLECT_FILES_GET_ALL_SOURCE_FILES_MILLISECONDS.observe(c as f64);
}
