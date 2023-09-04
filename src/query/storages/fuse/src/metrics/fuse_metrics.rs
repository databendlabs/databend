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

use common_metrics::histogram::Histogram;
use metrics::counter;
use metrics::increment_gauge;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("fuse_", $key)
    };
}

#[derive(Debug, Default)]
struct FuseMetrics {
    commit_mutation_unresolvable_conflict: Counter,
    commit_mutation_latest_snapshot_append_only: Counter,
    commit_mutation_modified_segment_exists_in_latest: Counter,
    commit_mutation_retry: Counter,
    commit_mutation_success: Counter,
    commit_copied_files: Counter,
    commit_milliseconds: Histogram,
    commit_aborts: Counter,
    remote_io_seeks: Counter,
    remote_io_seeks_after_merged: Counter,
    remote_io_read_bytes: Counter,
    remote_io_read_bytes_after_merged: Counter,
    remote_io_read_parts: Counter,
    remote_io_read_milliseconds: Histogram,
    remote_io_deserialize_milliseconds: Histogram,
    block_write_nums: Counter,
    block_write_bytes: Counter,
    block_write_milliseconds: Histogram,
    block_index_write_nums: Counter,
    block_index_write_bytes: Counter,
    block_index_write_milliseconds: Histogram,
    block_index_read_bytes: Counter,
    compact_block_read_nums: Counter,
    compact_block_read_bytes: Counter,
    compact_block_read_milliseconds: Histogram,
    segments_range_pruning_before: Counter,
    segments_range_pruning_after: Counter,
    bytes_segment_range_pruning_before: Counter,
    bytes_segment_range_pruning_after: Counter,
    blocks_range_pruning_before: Counter,
    blocks_range_pruning_after: Counter,
    bytes_block_range_pruning_before: Counter,
    bytes_block_range_pruning_after: Counter,
    blocks_bloom_pruning_before: Counter,
    blocks_bloom_pruning_after: Counter,
    bytes_block_bloom_pruning_before: Counter,
    bytes_block_bloom_pruning_after: Counter,
    pruning_prewhere_nums: Gauge,
    pruning_milliseconds: Histogram,
    deletion_block_range_pruned_nums: Counter,
    deletion_segment_range_pruned_whole_segment_nums: Counter,
    deletion_block_range_pruned_whole_block_nums: Counter,
    replace_into_block_number_after_pruning: Gauge,
    replace_into_segment_number_after_pruning: Counter,
    replace_into_block_number_totally_loaded: Counter,
    replace_into_row_number_write: Counter,
    replace_into_row_number_totally_loaded: Counter,
    replace_into_block_number_whole_block_deletion: Counter,
    replace_into_block_number_zero_row_deleted: Counter,
    replace_into_row_number_source_block: Counter,
    replace_into_row_number_after_table_level_pruning: Gauge,
    replace_into_partition_number: Counter,
    replace_into_time_process_input_block_ms: Histogram,
    replace_into_number_apply_deletion: Counter,
    replace_into_time_accumulated_merge_action_ms: Histogram,
    replace_into_time_apply_deletion_ms: Histogram,
    replace_into_block_number_bloom_pruned: Counter,
    replace_into_block_number_source: Counter,
}

impl FuseMetrics {
    fn init() -> Self {
        let metrics = Self::default();
    }
}

pub fn metrics_inc_commit_mutation_unresolvable_conflict() {
    counter!(key!("commit_mutation_unresolvable_conflict"), 1);
}

pub fn metrics_inc_commit_mutation_latest_snapshot_append_only() {
    counter!(key!("commit_mutation_latest_snapshot_append_only"), 1);
}

pub fn metrics_inc_commit_mutation_modified_segment_exists_in_latest() {
    counter!(key!("modified_segment_exists_in_latest"), 1);
}

pub fn metrics_inc_commit_mutation_retry() {
    counter!(key!("commit_mutation_retry"), 1);
}

pub fn metrics_inc_commit_mutation_success() {
    counter!(key!("commit_mutation_success"), 1);
}

pub fn metrics_inc_commit_copied_files(n: u64) {
    counter!(key!("commit_copied_files"), n);
}

pub fn metrics_inc_commit_milliseconds(c: u128) {
    increment_gauge!(key!("commit_milliseconds"), c as f64);
}

pub fn metrics_inc_commit_aborts() {
    counter!(key!("commit_aborts"), 1);
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

pub fn metrics_inc_remote_io_read_parts(c: u64) {
    increment_gauge!(key!("remote_io_read_parts"), c as f64);
}

pub fn metrics_inc_remote_io_read_milliseconds(c: u64) {
    increment_gauge!(key!("remote_io_read_milliseconds"), c as f64);
}

pub fn metrics_inc_remote_io_deserialize_milliseconds(c: u64) {
    increment_gauge!(key!("remote_io_deserialize_milliseconds"), c as f64);
}

/// Block metrics.
pub fn metrics_inc_block_write_nums(c: u64) {
    increment_gauge!(key!("block_write_nums"), c as f64);
}

pub fn metrics_inc_block_write_bytes(c: u64) {
    increment_gauge!(key!("block_write_bytes"), c as f64);
}

pub fn metrics_inc_block_write_milliseconds(c: u64) {
    increment_gauge!(key!("block_write_milliseconds"), c as f64);
}

pub fn metrics_inc_block_index_write_nums(c: u64) {
    increment_gauge!(key!("block_index_write_nums"), c as f64);
}

pub fn metrics_inc_block_index_write_bytes(c: u64) {
    increment_gauge!(key!("block_index_write_bytes"), c as f64);
}

pub fn metrics_inc_block_index_write_milliseconds(c: u64) {
    increment_gauge!(key!("block_index_write_milliseconds"), c as f64);
}

pub fn metrics_inc_block_index_read_bytes(c: u64) {
    increment_gauge!(key!("block_index_read_bytes"), c as f64);
}

/// Compact metrics.
pub fn metrics_inc_compact_block_read_nums(c: u64) {
    increment_gauge!(key!("compact_block_read_nums"), c as f64);
}

pub fn metrics_inc_compact_block_read_bytes(c: u64) {
    increment_gauge!(key!("compact_block_read_bytes"), c as f64);
}

pub fn metrics_inc_compact_block_read_milliseconds(c: u64) {
    increment_gauge!(key!("compact_block_read_milliseconds"), c as f64);
}

/// Pruning metrics.
pub fn metrics_inc_segments_range_pruning_before(c: u64) {
    increment_gauge!(key!("segments_range_pruning_before"), c as f64);
}

pub fn metrics_inc_segments_range_pruning_after(c: u64) {
    increment_gauge!(key!("segments_range_pruning_after"), c as f64);
}

pub fn metrics_inc_bytes_segment_range_pruning_before(c: u64) {
    increment_gauge!(key!("bytes_segment_range_pruning_before"), c as f64);
}

pub fn metrics_inc_bytes_segment_range_pruning_after(c: u64) {
    increment_gauge!(key!("bytes_segment_range_pruning_after"), c as f64);
}

pub fn metrics_inc_blocks_range_pruning_before(c: u64) {
    increment_gauge!(key!("blocks_range_pruning_before"), c as f64);
}

pub fn metrics_inc_blocks_range_pruning_after(c: u64) {
    increment_gauge!(key!("blocks_range_pruning_after"), c as f64);
}

pub fn metrics_inc_bytes_block_range_pruning_before(c: u64) {
    increment_gauge!(key!("bytes_block_range_pruning_before"), c as f64);
}

pub fn metrics_inc_bytes_block_range_pruning_after(c: u64) {
    increment_gauge!(key!("bytes_block_range_pruning_after"), c as f64);
}

pub fn metrics_inc_blocks_bloom_pruning_before(c: u64) {
    increment_gauge!(key!("blocks_bloom_pruning_before"), c as f64);
}

pub fn metrics_inc_blocks_bloom_pruning_after(c: u64) {
    increment_gauge!(key!("blocks_bloom_pruning_after"), c as f64);
}

pub fn metrics_inc_bytes_block_bloom_pruning_before(c: u64) {
    increment_gauge!(key!("bytes_block_bloom_pruning_before"), c as f64);
}

pub fn metrics_inc_bytes_block_bloom_pruning_after(c: u64) {
    increment_gauge!(key!("bytes_block_bloom_pruning_after"), c as f64);
}

pub fn metrics_inc_pruning_prewhere_nums(c: u64) {
    increment_gauge!(key!("pruning_prewhere_nums"), c as f64);
}

pub fn metrics_inc_pruning_milliseconds(c: u64) {
    increment_gauge!(key!("pruning_milliseconds"), c as f64);
}

pub fn metrics_inc_deletion_block_range_pruned_nums(c: u64) {
    increment_gauge!(key!("deletion_block_range_pruned_nums"), c as f64);
}

pub fn metrics_inc_deletion_segment_range_purned_whole_segment_nums(c: u64) {
    increment_gauge!(
        key!("deletion_segment_range_pruned_whole_segment_nums"),
        c as f64
    );
}

pub fn metrics_inc_deletion_block_range_pruned_whole_block_nums(c: u64) {
    increment_gauge!(
        key!("deletion_block_range_pruned_whole_block_nums"),
        c as f64
    );
}

pub fn metrics_inc_replace_block_number_after_pruning(c: u64) {
    increment_gauge!(key!("replace_into_block_number_after_pruning"), c as f64);
}

pub fn metrics_inc_replace_segment_number_after_pruning(c: u64) {
    increment_gauge!(key!("replace_into_segment_number_after_pruning"), c as f64);
}

pub fn metrics_inc_replace_row_number_after_pruning(c: u64) {
    increment_gauge!(key!("replace_into_row_number_after_pruning"), c as f64);
}

pub fn metrics_inc_replace_block_number_totally_loaded(c: u64) {
    increment_gauge!(key!("replace_into_block_number_totally_loaded"), c as f64);
}

pub fn metrics_inc_replace_row_number_write(c: u64) {
    increment_gauge!(key!("replace_into_row_number_write"), c as f64);
}
pub fn metrics_inc_replace_block_number_write(c: u64) {
    increment_gauge!(key!("replace_into_block_number_write"), c as f64);
}

pub fn metrics_inc_replace_row_number_totally_loaded(c: u64) {
    increment_gauge!(key!("replace_into_row_number_totally_loaded"), c as f64);
}

pub fn metrics_inc_replace_whole_block_deletion(c: u64) {
    increment_gauge!(
        key!("replace_into_block_number_whole_block_deletion"),
        c as f64
    );
}

pub fn metrics_inc_replace_block_of_zero_row_deleted(c: u64) {
    increment_gauge!(key!("replace_into_block_number_zero_row_deleted"), c as f64);
}

pub fn metrics_inc_replace_original_row_number(c: u64) {
    increment_gauge!(key!("replace_into_row_number_source_block"), c as f64);
}

pub fn metrics_inc_replace_row_number_after_table_level_pruning(c: u64) {
    increment_gauge!(
        key!("replace_into_row_number_after_table_level_pruning"),
        c as f64
    );
}

pub fn metrics_inc_replace_partition_number(c: u64) {
    increment_gauge!(key!("replace_into_partition_number"), c as f64);
}

// time used in processing the input block
pub fn metrics_inc_replace_process_input_block_time_ms(c: u64) {
    increment_gauge!(key!("replace_into_time_process_input_block_ms"), c as f64);
}

// the number of accumulate_merge_action operation invoked
pub fn metrics_inc_replace_number_accumulated_merge_action() {
    increment_gauge!(key!("replace_into_number_accumulate_merge_action"), 1_f64);
}

// the number of apply_deletion operation applied
pub fn metrics_inc_replace_number_apply_deletion() {
    increment_gauge!(key!("replace_into_number_apply_deletion"), 1_f64);
}

// time used in executing the accumulated_merge_action operation
pub fn metrics_inc_replace_accumulated_merge_action_time_ms(c: u64) {
    increment_gauge!(
        key!("replace_into_time_accumulated_merge_action_ms"),
        c as f64
    );
}

// time used in executing the apply_deletion operation
pub fn metrics_inc_replace_apply_deletion_time_ms(c: u64) {
    increment_gauge!(key!("replace_into_time_apply_deletion_ms"), c as f64);
}

// number of blocks that pruned by bloom filter
pub fn metrics_inc_replace_block_number_bloom_pruned(c: u64) {
    increment_gauge!(key!("replace_into_block_number_bloom_pruned"), c as f64);
}
// number of blocks from upstream  source
pub fn metrics_inc_replace_block_number_input(c: u64) {
    increment_gauge!(key!("replace_into_block_number_source"), c as f64);
}
