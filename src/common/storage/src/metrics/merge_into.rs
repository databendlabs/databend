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

macro_rules! key {
    ($key: literal) => {
        concat!("query_", $key)
    };
}

lazy_static! {
    static ref MERGE_INTO_REPLACE_BLOCKS_COUNTER: Counter =
        register_counter(key!("merge_into_replace_blocks_counter"));
    static ref MERGE_INTO_DELETED_BLOCKS_COUNTER: Counter =
        register_counter(key!("merge_into_deleted_blocks_counter"));
    static ref MERGE_INTO_APPEND_BLOCKS_COUNTER: Counter =
        register_counter(key!("merge_into_append_blocks_counter"));
    static ref MERGE_INTO_MATCHED_ROWS: Counter = register_counter(key!("merge_into_matched_rows"));
    static ref MERGE_INTO_UNMATCHED_ROWS: Counter =
        register_counter(key!("merge_into_unmatched_rows"));
    static ref MERGE_INTO_ACCUMULATE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("merge_into_accumulate_milliseconds"));
    static ref MERGE_INTO_APPLY_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("merge_into_apply_milliseconds"));
    static ref MERGE_INTO_SPLIT_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("merge_into_split_milliseconds"));
    static ref MERGE_INTO_NOT_MATCHED_OPERATION_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("merge_into_not_matched_operation_milliseconds"));
    static ref MERGE_INTO_MATCHED_OPERATION_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("merge_into_matched_operation_milliseconds"));
}

pub fn metrics_inc_merge_into_replace_blocks_counter(c: u32) {
    MERGE_INTO_REPLACE_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_deleted_blocks_counter(c: u32) {
    MERGE_INTO_DELETED_BLOCKS_COUNTER.inc_by(c as u64);
}

// used to record append new blocks in matched split and not match insert.
pub fn metrics_inc_merge_into_append_blocks_counter(c: u32) {
    MERGE_INTO_APPEND_BLOCKS_COUNTER.inc_by(c as u64);
}

// matched_rows and not unmatched_rows is used in the join phase of merge_source.
pub fn metrics_inc_merge_into_matched_rows(c: u32) {
    MERGE_INTO_MATCHED_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_unmatched_rows(c: u32) {
    MERGE_INTO_UNMATCHED_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_accumulate_milliseconds(c: u64) {
    MERGE_INTO_ACCUMULATE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_merge_into_apply_milliseconds(c: u64) {
    MERGE_INTO_APPLY_MILLISECONDS.observe(c as f64);
}

// join result data split time
pub fn metrics_inc_merge_into_split_milliseconds(c: u64) {
    MERGE_INTO_SPLIT_MILLISECONDS.observe(c as f64);
}

// after merge_source_split, record the time of not macthed clauses (processor_merge_into_not_matched)
pub fn merge_into_not_matched_operation_milliseconds(c: u64) {
    MERGE_INTO_NOT_MATCHED_OPERATION_MILLISECONDS.observe(c as f64);
}

// after merge_source_split, record the time of macthed clauses (processor_merge_into_matched_and_split)
pub fn merge_into_matched_operation_milliseconds(c: u64) {
    MERGE_INTO_MATCHED_OPERATION_MILLISECONDS.observe(c as f64);
}
