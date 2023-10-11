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
        concat!("fuse_", $key)
    };
}

lazy_static! {
    static ref COMMIT_MUTATION_UNRESOLVABLE_CONFLICT: Counter =
        register_counter(key!("commit_mutation_unresolvable_conflict"));
    static ref COMMIT_MUTATION_LATEST_SNAPSHOT_APPEND_ONLY: Counter =
        register_counter(key!("commit_mutation_latest_snapshot_append_only"));
    static ref COMMIT_MUTATION_MODIFIED_SEGMENT_EXISTS_IN_LATEST: Counter =
        register_counter(key!("commit_mutation_modified_segment_exists_in_latest"));
    static ref COMMIT_MUTATION_RETRY: Counter = register_counter(key!("commit_mutation_retry"));
    static ref COMMIT_MUTATION_SUCCESS: Counter = register_counter(key!("commit_mutation_success"));
    static ref COMMIT_COPIED_FILES: Counter = register_counter(key!("commit_copied_files"));
    static ref COMMIT_MILLISECONDS: Counter = register_counter(key!("commit_milliseconds"));
    static ref COMMIT_ABORTS: Counter = register_counter(key!("commit_aborts"));
    static ref REMOTE_IO_SEEKS: Counter = register_counter(key!("remote_io_seeks"));
    static ref REMOTE_IO_SEEKS_AFTER_MERGED: Counter =
        register_counter(key!("remote_io_seeks_after_merged"));
    static ref REMOTE_IO_READ_BYTES: Counter = register_counter(key!("remote_io_read_bytes"));
    static ref REMOTE_IO_READ_BYTES_AFTER_MERGED: Counter =
        register_counter(key!("remote_io_read_bytes_after_merged"));
    static ref REMOTE_IO_READ_PARTS: Counter = register_counter(key!("remote_io_read_parts"));
    static ref REMOTE_IO_READ_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("remote_io_read_milliseconds"));
    static ref REMOTE_IO_DESERIALIZE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("remote_io_deserialize_milliseconds"));
    static ref BLOCK_WRITE_NUMS: Counter = register_counter(key!("block_write_nums"));
    static ref BLOCK_WRITE_BYTES: Counter = register_counter(key!("block_write_bytes"));
    static ref BLOCK_WRITE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("block_write_millioseconds"));
    static ref BLOCK_INDEX_WRITE_NUMS: Counter = register_counter(key!("block_index_write_nums"));
    static ref BLOCK_INDEX_WRITE_BYTES: Counter = register_counter(key!("block_index_write_bytes"));
    static ref BLOCK_INDEX_WRITE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("block_index_write_milliseconds"));
    static ref BLOCK_INDEX_READ_BYTES: Counter = register_counter(key!("block_index_read_bytes"));
    static ref COMPACT_BLOCK_READ_NUMS: Counter = register_counter(key!("compact_block_read_nums"));
    static ref COMPACT_BLOCK_READ_BYTES: Counter =
        register_counter(key!("compact_block_read_bytes"));
    static ref COMPACT_BLOCK_READ_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("compact_block_read_milliseconds"));
    static ref COMPACT_BLOCK_BUILD_TASK_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("compact_block_build_task_milliseconds"));
    static ref SEGMENTS_RANGE_PRUNING_BEFORE: Counter =
        register_counter(key!("segments_range_pruning_before"));
    static ref SEGMENTS_RANGE_PRUNING_AFTER: Counter =
        register_counter(key!("segments_range_pruning_after"));
    static ref BYTES_SEGMENT_RANGE_PRUNING_BEFORE: Counter =
        register_counter(key!("bytes_segment_range_pruning_before"));
    static ref BYTES_SEGMENT_RANGE_PRUNING_AFTER: Counter =
        register_counter(key!("bytes_segment_range_pruning_after"));
    static ref BLOCKS_RANGE_PRUNING_BEFORE: Counter =
        register_counter(key!("blocks_range_pruning_before"));
    static ref BLOCKS_RANGE_PRUNING_AFTER: Counter =
        register_counter(key!("blocks_range_pruning_after"));
    static ref BYTES_BLOCK_RANGE_PRUNING_BEFORE: Counter =
        register_counter(key!("bytes_block_range_pruning_before"));
    static ref BYTES_BLOCK_RANGE_PRUNING_AFTER: Counter =
        register_counter(key!("bytes_block_range_pruning_after"));
    static ref BLOCKS_BLOOM_PRUNING_BEFORE: Counter =
        register_counter(key!("blocks_bloom_pruning_before"));
    static ref BLOCKS_BLOOM_PRUNING_AFTER: Counter =
        register_counter(key!("blocks_bloom_pruning_after"));
    static ref BYTES_BLOCK_BLOOM_PRUNING_BEFORE: Counter =
        register_counter(key!("bytes_block_bloom_pruning_before"));
    static ref BYTES_BLOCK_BLOOM_PRUNING_AFTER: Counter =
        register_counter(key!("bytes_block_bloom_pruning_after"));
    static ref PRUNING_PREWHERE_NUMS: Counter = register_counter(key!("pruning_prewhere_nums"));
    static ref PRUNING_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds(key!("pruning_milliseconds"));
    static ref DELETION_BLOCK_RANGE_PRUNED_NUMS: Counter =
        register_counter(key!("deletion_block_range_pruned_nums"));
    static ref DELETION_SEGMENT_RANGE_PRUNED_WHOLE_SEGMENT_NUMS: Counter =
        register_counter(key!("deletion_segment_range_pruned_whole_segment_nums"));
    static ref DELETION_BLOCK_RANGE_PRUNED_WHOLE_BLOCK_NUMS: Counter =
        register_counter(key!("deletion_block_range_pruned_whole_block_nums"));
    static ref REPLACE_INTO_BLOCK_NUMBER_AFTER_PRUNING: Counter =
        register_counter(key!("replace_into_block_number_after_pruning"));
    static ref REPLACE_INTO_SEGMENT_NUMBER_AFTER_PRUNING: Counter =
        register_counter(key!("replace_into_segment_number_after_pruning"));
    static ref REPLACE_INTO_ROW_NUMBER_AFTER_PRUNING: Counter =
        register_counter(key!("replace_into_row_number_after_pruning"));
    static ref REPLACE_INTO_BLOCK_NUMBER_TOTALLY_LOADED: Counter =
        register_counter(key!("replace_into_block_number_totally_loaded"));
    static ref REPLACE_INTO_ROW_NUMBER_WRITE: Counter =
        register_counter(key!("replace_into_row_number_write"));
    static ref REPLACE_INTO_BLOCK_NUMBER_WRITE: Counter =
        register_counter(key!("replace_into_block_number_write"));
    static ref REPLACE_INTO_ROW_NUMBER_TOTALLY_LOADED: Counter =
        register_counter(key!("replace_into_row_number_totally_loaded"));
    static ref REPLACE_INTO_BLOCK_NUMBER_WHOLE_BLOCK_DELETION: Counter =
        register_counter(key!("replace_into_block_number_whole_block_deletion"));
    static ref REPLACE_INTO_BLOCK_NUMBER_ZERO_ROW_DELETED: Counter =
        register_counter(key!("replace_into_block_number_zero_row_deleted"));
    static ref REPLACE_INTO_ROW_NUMBER_SOURCE_BLOCK: Counter =
        register_counter(key!("replace_into_row_number_source_block"));
    static ref REPLACE_INTO_ROW_NUMBER_AFTER_TABLE_LEVEL_PRUNING: Counter =
        register_counter(key!("replace_into_row_number_after_table_level_pruning"));
    static ref REPLACE_INTO_PARTITION_NUMBER: Counter =
        register_counter(key!("replace_into_partition_number"));
    static ref REPLACE_INTO_TIME_PROCESS_INPUT_BLOCK_MS: Histogram =
        register_histogram_in_milliseconds(key!("replace_into_time_process_input_block_ms"));
    static ref REPLACE_INTO_NUMBER_ACCUMULATED_MERGE_ACTION: Counter =
        register_counter(key!("replace_into_number_accumulated_merge_action"));
    static ref REPLACE_INTO_NUMBER_APPLY_DELETION: Counter =
        register_counter(key!("replace_into_number_apply_deletion"));
    static ref REPLACE_INTO_TIME_ACCUMULATED_MERGE_ACTION_MS: Histogram =
        register_histogram_in_milliseconds(key!("replace_into_time_accumulated_merge_action_ms"),);
    static ref REPLACE_INTO_TIME_APPLY_DELETION_MS: Histogram =
        register_histogram_in_milliseconds(key!("replace_into_time_apply_deletion_ms"),);
    static ref REPLACE_INTO_BLOCK_NUMBER_BLOOM_PRUNED: Counter =
        register_counter(key!("replace_into_block_number_bloom_pruned"));
    static ref REPLACE_INTO_BLOCK_NUMBER_SOURCE: Counter =
        register_counter(key!("replace_into_block_number_source"));
    static ref REPLACE_INTO_REPLACED_BLOCKS_ROWS: Counter =
        register_counter(key!("replace_into_replaced_blocks_rows"));
    static ref REPLACE_INTO_DELETED_BLOCKS_ROWS: Counter =
        register_counter(key!("replace_into_deleted_blocks_rows"));
    static ref REPLACE_INTO_APPEND_BLOCKS_ROWS: Counter =
        register_counter(key!("replace_into_append_blocks_rows"));
}

pub fn metrics_inc_commit_mutation_unresolvable_conflict() {
    COMMIT_MUTATION_UNRESOLVABLE_CONFLICT.inc();
}

pub fn metrics_inc_commit_mutation_latest_snapshot_append_only() {
    COMMIT_MUTATION_LATEST_SNAPSHOT_APPEND_ONLY.inc();
}

pub fn metrics_inc_commit_mutation_modified_segment_exists_in_latest() {
    COMMIT_MUTATION_MODIFIED_SEGMENT_EXISTS_IN_LATEST.inc();
}

pub fn metrics_inc_commit_mutation_retry() {
    COMMIT_MUTATION_RETRY.inc();
}

pub fn metrics_inc_commit_mutation_success() {
    COMMIT_MUTATION_SUCCESS.inc();
}

pub fn metrics_inc_commit_copied_files(n: u64) {
    COMMIT_COPIED_FILES.inc_by(n);
}

pub fn metrics_inc_commit_milliseconds(c: u128) {
    COMMIT_MILLISECONDS.inc_by(c as u64);
}

pub fn metrics_inc_commit_aborts() {
    COMMIT_ABORTS.inc();
}

pub fn metrics_inc_remote_io_seeks(c: u64) {
    REMOTE_IO_SEEKS.inc_by(c);
}

pub fn metrics_inc_remote_io_seeks_after_merged(c: u64) {
    REMOTE_IO_SEEKS_AFTER_MERGED.inc_by(c);
}

pub fn metrics_inc_remote_io_read_bytes(c: u64) {
    REMOTE_IO_READ_BYTES.inc_by(c);
}

pub fn metrics_inc_remote_io_read_bytes_after_merged(c: u64) {
    REMOTE_IO_READ_BYTES_AFTER_MERGED.inc_by(c);
}

pub fn metrics_inc_remote_io_read_parts(c: u64) {
    REMOTE_IO_READ_PARTS.inc_by(c);
}

pub fn metrics_inc_remote_io_read_milliseconds(c: u64) {
    REMOTE_IO_READ_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_remote_io_deserialize_milliseconds(c: u64) {
    REMOTE_IO_DESERIALIZE_MILLISECONDS.observe(c as f64);
}

/// Block metrics.
pub fn metrics_inc_block_write_nums(c: u64) {
    BLOCK_WRITE_NUMS.inc_by(c);
}

pub fn metrics_inc_block_write_bytes(c: u64) {
    BLOCK_WRITE_BYTES.inc_by(c);
}

pub fn metrics_inc_block_write_milliseconds(c: u64) {
    BLOCK_WRITE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_block_index_write_nums(c: u64) {
    BLOCK_INDEX_WRITE_NUMS.inc_by(c);
}

pub fn metrics_inc_block_index_write_bytes(c: u64) {
    BLOCK_INDEX_WRITE_BYTES.inc_by(c);
}

pub fn metrics_inc_block_index_write_milliseconds(c: u64) {
    BLOCK_INDEX_WRITE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_block_index_read_bytes(c: u64) {
    BLOCK_INDEX_READ_BYTES.inc_by(c);
}

/// Compact metrics.
pub fn metrics_inc_compact_block_read_nums(c: u64) {
    COMPACT_BLOCK_READ_NUMS.inc_by(c);
}

pub fn metrics_inc_compact_block_read_bytes(c: u64) {
    COMPACT_BLOCK_READ_BYTES.inc_by(c);
}

pub fn metrics_inc_compact_block_read_milliseconds(c: u64) {
    COMPACT_BLOCK_READ_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_compact_block_build_task_milliseconds(c: u64) {
    COMPACT_BLOCK_BUILD_TASK_MILLISECONDS.observe(c as f64);
}

/// Pruning metrics.
pub fn metrics_inc_segments_range_pruning_before(c: u64) {
    SEGMENTS_RANGE_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_segments_range_pruning_after(c: u64) {
    SEGMENTS_RANGE_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_bytes_segment_range_pruning_before(c: u64) {
    BYTES_SEGMENT_RANGE_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_bytes_segment_range_pruning_after(c: u64) {
    BYTES_SEGMENT_RANGE_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_blocks_range_pruning_before(c: u64) {
    BLOCKS_RANGE_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_blocks_range_pruning_after(c: u64) {
    BLOCKS_RANGE_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_bytes_block_range_pruning_before(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_bytes_block_range_pruning_after(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_blocks_bloom_pruning_before(c: u64) {
    BLOCKS_BLOOM_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_blocks_bloom_pruning_after(c: u64) {
    BLOCKS_BLOOM_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_bytes_block_bloom_pruning_before(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_bytes_block_bloom_pruning_after(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_pruning_prewhere_nums(c: u64) {
    PRUNING_PREWHERE_NUMS.inc_by(c);
}

pub fn metrics_inc_pruning_milliseconds(c: u64) {
    PRUNING_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_deletion_block_range_pruned_nums(c: u64) {
    DELETION_BLOCK_RANGE_PRUNED_NUMS.inc_by(c);
}

pub fn metrics_inc_deletion_segment_range_purned_whole_segment_nums(c: u64) {
    DELETION_SEGMENT_RANGE_PRUNED_WHOLE_SEGMENT_NUMS.inc_by(c);
}

pub fn metrics_inc_deletion_block_range_pruned_whole_block_nums(c: u64) {
    DELETION_BLOCK_RANGE_PRUNED_WHOLE_BLOCK_NUMS.inc_by(c);
}

pub fn metrics_inc_replace_block_number_after_pruning(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_AFTER_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_segment_number_after_pruning(c: u64) {
    REPLACE_INTO_SEGMENT_NUMBER_AFTER_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_row_number_after_pruning(c: u64) {
    REPLACE_INTO_ROW_NUMBER_AFTER_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_block_number_totally_loaded(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_TOTALLY_LOADED.inc_by(c);
}

pub fn metrics_inc_replace_row_number_write(c: u64) {
    REPLACE_INTO_ROW_NUMBER_WRITE.inc_by(c);
}

pub fn metrics_inc_replace_block_number_write(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_WRITE.inc_by(c);
}

pub fn metrics_inc_replace_row_number_totally_loaded(c: u64) {
    REPLACE_INTO_ROW_NUMBER_TOTALLY_LOADED.inc_by(c);
}

pub fn metrics_inc_replace_whole_block_deletion(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_WHOLE_BLOCK_DELETION.inc_by(c);
}

pub fn metrics_inc_replace_block_of_zero_row_deleted(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_ZERO_ROW_DELETED.inc_by(c);
}

pub fn metrics_inc_replace_original_row_number(c: u64) {
    REPLACE_INTO_ROW_NUMBER_SOURCE_BLOCK.inc_by(c);
}

pub fn metrics_inc_replace_row_number_after_table_level_pruning(c: u64) {
    REPLACE_INTO_ROW_NUMBER_AFTER_TABLE_LEVEL_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_partition_number(c: u64) {
    REPLACE_INTO_PARTITION_NUMBER.inc_by(c);
}

// time used in processing the input block
pub fn metrics_inc_replace_process_input_block_time_ms(c: u64) {
    REPLACE_INTO_TIME_PROCESS_INPUT_BLOCK_MS.observe(c as f64);
}

// the number of accumulate_merge_action operation invoked
pub fn metrics_inc_replace_number_accumulated_merge_action() {
    REPLACE_INTO_NUMBER_ACCUMULATED_MERGE_ACTION.inc();
}

// the number of apply_deletion operation applied
pub fn metrics_inc_replace_number_apply_deletion() {
    REPLACE_INTO_NUMBER_APPLY_DELETION.inc();
}

// time used in executing the accumulated_merge_action operation
pub fn metrics_inc_replace_accumulated_merge_action_time_ms(c: u64) {
    REPLACE_INTO_TIME_ACCUMULATED_MERGE_ACTION_MS.observe(c as f64)
}

// time used in executing the apply_deletion operation
pub fn metrics_inc_replace_apply_deletion_time_ms(c: u64) {
    REPLACE_INTO_TIME_APPLY_DELETION_MS.observe(c as f64);
}

// number of blocks that pruned by bloom filter
pub fn metrics_inc_replace_block_number_bloom_pruned(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_BLOOM_PRUNED.inc_by(c);
}
// number of blocks from upstream  source
pub fn metrics_inc_replace_block_number_input(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_SOURCE.inc_by(c);
}

// rows of blocks that are replaced
pub fn metrics_inc_replace_replaced_blocks_rows(c: u64) {
    REPLACE_INTO_REPLACED_BLOCKS_ROWS.inc_by(c);
}

// rows of blocks that are deleted
pub fn metrics_inc_replace_deleted_blocks_rows(c: u64) {
    REPLACE_INTO_DELETED_BLOCKS_ROWS.inc_by(c);
}

// rows of blocks that are appended
pub fn metrics_inc_replace_append_blocks_rows(c: u64) {
    REPLACE_INTO_APPEND_BLOCKS_ROWS.inc_by(c);
}
