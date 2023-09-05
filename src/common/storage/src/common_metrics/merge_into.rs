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
use lazy_static::lazy_static;
use prometheus_client::metrics::counter::Counter;
use common_metrics::registry::register_counter;


macro_rules! key {
    ($key: literal) => {
        concat!("query_", $key)
    };
}

lazy_static! {
    static ref MERGE_INTO_REPLACE_BLOCKS_COUNTER: Counter = register_counter(key!("merge_into_replace_blocks_counter"));
    static ref MERGE_INTO_APPEND_BLOCKS_COUNTER: Counter = register_counter(key!("merge_into_append_blocks_counter"));
    static ref MERGE_INTO_MATCHED_ROWS: Counter = register_counter(key!("merge_into_matched_rows"));
    static ref MERGE_INTO_UNMATCHED_ROWS: Counter = register_counter(key!("merge_into_unmatched_rows"));
}

pub fn metrics_inc_merge_into_replace_blocks_counter(c: u32) {
    increment_gauge!(key!("merge_into_replace_blocks_counter"), c as f64);
    MERGE_INTO_REPLACE_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_append_blocks_counter(c: u32) {
    increment_gauge!(key!("merge_into_append_blocks_counter"), c as f64);
    MERGE_INTO_APPEND_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_matched_rows(c: u32) {
    increment_gauge!(key!("merge_into_matched_rows"), c as f64);
    MERGE_INTO_MATCHED_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_unmatched_rows(c: u32) {
    increment_gauge!(key!("merge_into_unmatched_rows"), c as f64);
    MERGE_INTO_UNMATCHED_ROWS.inc_by(c as u64);
}
