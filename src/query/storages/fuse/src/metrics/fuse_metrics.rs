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

//! Defines meta-service metric.
//!
//! The metric key is built in form of `<namespace>_<sub_system>_<field>`.
//!
//! The `namespace` is `fuse`.

use metrics::counter;
use metrics::gauge;
use metrics::increment_gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("fuse_", $key)
    };
}

pub fn metrics_set_segments_memory_usage(size: f64) {
    gauge!(key!("compact_segments_memory_usage"), size);
}

pub fn metrics_set_selected_blocks_memory_usage(size: f64) {
    gauge!(key!("compact_selected_blocks_memory_usage"), size);
}

pub fn metrics_inc_commit_mutation_unresolvable_conflict() {
    counter!(key!("commit_mutation_unresolvable_conflict"), 1);
}

pub fn metrics_inc_commit_mutation_resolvable_conflict() {
    counter!(key!("commit_mutation_resolvable_conflict"), 1);
}

pub fn metrics_inc_commit_mutation_retry() {
    counter!(key!("commit_mutation_retry"), 1);
}

pub fn metrics_inc_commit_mutation_success() {
    counter!(key!("commit_mutation_success"), 1);
}

pub fn metrics_inc_commit_mutation_aborts() {
    counter!(key!("commit_mutation_aborts"), 1);
}

pub fn metrics_inc_remote_io_seeks(c: u64) {
    increment_gauge!(key!("remote_io_seeks"), c as f64);
}

pub fn metrics_inc_remote_io_seeks_after_merged(c: u64) {
    increment_gauge!(key!("remote_io_seeks_after_merged"), c as f64);
}

pub fn metrics_inc_remote_io_read_bytes(c: u64) {
    increment_gauge!(key!("remote_io_read_bytes"), c as f64);
}

pub fn metrics_inc_remote_io_read_bytes_after_merged(c: u64) {
    increment_gauge!(key!("remote_io_read_bytes_after_merged"), c as f64);
}

pub fn metrics_inc_remote_io_read_milliseconds(c: u64) {
    increment_gauge!(key!("remote_io_read_milliseconds"), c as f64);
}

pub fn metrics_inc_remote_io_copy_milliseconds(c: u64) {
    increment_gauge!(key!("remote_io_copy_milliseconds"), c as f64);
}

pub fn metrics_inc_remote_io_read_parts(c: u64) {
    increment_gauge!(key!("remote_io_read_parts"), c as f64);
}

pub fn metrics_reset() {
    let c = 0 as f64;
    gauge!(key!("remote_io_seeks"), c);
    gauge!(key!("remote_io_seeks_after_merged"), c);
    gauge!(key!("remote_io_read_bytes"), c);
    gauge!(key!("remote_io_read_bytes_after_merged"), c);
    gauge!(key!("remote_io_read_milliseconds"), c);
    gauge!(key!("remote_io_copy_milliseconds"), c);
    gauge!(key!("remote_io_read_parts"), c);
}
