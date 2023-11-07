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

use lazy_static::lazy_static;

use crate::register_counter;
use crate::register_histogram_in_milliseconds;
use crate::Counter;
use crate::Histogram;

lazy_static! {
    // Common metrics.
    static ref OMIT_FILTER_ROWGROUPS: Counter = register_counter("omit_filter_rowgroups");
    static ref OMIT_FILTER_ROWS: Counter = register_counter("omit_filter_rows");

    // COPY metrics.
     static ref COPY_PURGE_FILE_COUNTER: Counter = register_counter("copy_purge_file_counter");
    static ref COPY_PURGE_FILE_COST_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_purge_file_cost_milliseconds");
    static ref COPY_READ_PART_COUNTER: Counter = register_counter("copy_read_part_counter");
    static ref COPY_READ_SIZE_BYTES: Counter = register_counter("copy_read_size_bytes");
    static ref COPY_READ_PART_COST_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_read_part_cost_milliseconds");
    static ref FILTER_OUT_COPIED_FILES_REQUEST_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_filter_out_copied_files_request_milliseconds");
    static ref FILTER_OUT_COPIED_FILES_ENTIRE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_filter_out_copied_files_entire_milliseconds");
    static ref COLLECT_FILES_GET_ALL_SOURCE_FILES_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("copy_collect_files_get_all_source_files_milliseconds");


    // Merge into metrics.
    static ref MERGE_INTO_REPLACE_BLOCKS_COUNTER: Counter =
        register_counter("merge_into_replace_blocks_counter");
    static ref MERGE_INTO_REPLACE_BLOCKS_ROWS_COUNTER: Counter =
        register_counter("merge_into_replace_blocks_rows_counter");
    static ref MERGE_INTO_DELETED_BLOCKS_COUNTER: Counter =
        register_counter("merge_into_deleted_blocks_counter");
    static ref MERGE_INTO_DELETED_BLOCKS_ROWS_COUNTER: Counter =
        register_counter("merge_into_deleted_blocks_rows_counter");
    static ref MERGE_INTO_APPEND_BLOCKS_COUNTER: Counter =
        register_counter("merge_into_append_blocks_counter");
    static ref MERGE_INTO_DISTRIBUTED_HASHTABLE_FETCH_ROWNUMBER: Counter =
        register_counter("merge_into_distributed_hashtable_fetch_row_number");
    static ref MERGE_INTO_DISTRIBUTED_HASHTABLE_EMPTY_BLOCK: Counter =
        register_counter("merge_into_distributed_hashtable_empty_block");
    static ref MERGE_INTO_DISTRIBUTED_GENERATE_ROW_NUMBERS: Counter =
        register_counter("merge_into_distributed_generate_row_numbers");
    static ref MERGE_INTO_DISTRIBUTED_INIT_UNIQUE_NUMBER: Counter =
        register_counter("merge_into_distributed_init_unique_number");
    static ref MERGE_INTO_DISTRIBUTED_NEW_SET_LEN: Counter =
        register_counter("merge_into_distributed_new_set_len");
    static ref MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_EMPTY_NULL_BLOCK: Counter =
        register_counter("merge_into_distributed_hashtable_push_empty_null_block");
    static ref MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK: Counter =
        register_counter("merge_into_distributed_hashtable_push_null_block");
    static ref MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK_ROWS: Counter =
        register_counter("merge_into_distributed_hashtable_push_null_block_rows");
    static ref MERGE_INTO_APPEND_BLOCKS_ROWS_COUNTER: Counter =
        register_counter("merge_into_append_blocks_rows_counter");
    static ref MERGE_INTO_MATCHED_ROWS: Counter = register_counter("merge_into_matched_rows");
    static ref MERGE_INTO_UNMATCHED_ROWS: Counter = register_counter("merge_into_unmatched_rows");
    static ref MERGE_INTO_DISTRIBUTED_DEDUPLICATE_ROWNUMBER: Counter =
        register_counter("merge_into_distributed_deduplicate_row_number");
    static ref MERGE_INTO_DISTRIBUTED_EMPTY_ROWNUMBER: Counter =
        register_counter("merge_into_distributed_empty_row_number");
    static ref MERGE_INTO_DISTRIBUTED_APPLY_ROWNUMBER: Counter =
        register_counter("merge_into_distributed_apply_row_number");
    static ref MERGE_INTO_ACCUMULATE_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("merge_into_accumulate_milliseconds");
    static ref MERGE_INTO_APPLY_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("merge_into_apply_milliseconds");
    static ref MERGE_INTO_SPLIT_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("merge_into_split_milliseconds");
    static ref MERGE_INTO_NOT_MATCHED_OPERATION_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("merge_into_not_matched_operation_milliseconds");
    static ref MERGE_INTO_MATCHED_OPERATION_MILLISECONDS: Histogram =
        register_histogram_in_milliseconds("merge_into_matched_operation_milliseconds");
}

/// Common metrics.
pub fn metrics_inc_omit_filter_rowgroups(c: u64) {
    OMIT_FILTER_ROWGROUPS.inc_by(c);
}

pub fn metrics_inc_omit_filter_rows(c: u64) {
    OMIT_FILTER_ROWS.inc_by(c);
}

/// COPY
pub fn metrics_inc_copy_purge_files_counter(c: u32) {
    COPY_PURGE_FILE_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_copy_purge_files_cost_milliseconds(c: u32) {
    COPY_PURGE_FILE_COST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_copy_read_part_counter() {
    COPY_READ_PART_COUNTER.inc();
}

pub fn metrics_inc_copy_read_size_bytes(c: u64) {
    COPY_READ_SIZE_BYTES.inc_by(c);
}

pub fn metrics_inc_copy_read_part_cost_milliseconds(c: u64) {
    COPY_READ_PART_COST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_filter_out_copied_files_request_milliseconds(c: u64) {
    FILTER_OUT_COPIED_FILES_REQUEST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_filter_out_copied_files_entire_milliseconds(c: u64) {
    FILTER_OUT_COPIED_FILES_ENTIRE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_collect_files_get_all_source_files_milliseconds(c: u64) {
    COLLECT_FILES_GET_ALL_SOURCE_FILES_MILLISECONDS.observe(c as f64);
}

/// Merge into metrics.
pub fn metrics_inc_merge_into_replace_blocks_counter(c: u32) {
    MERGE_INTO_REPLACE_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_replace_blocks_rows_counter(c: u32) {
    MERGE_INTO_REPLACE_BLOCKS_ROWS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_deleted_blocks_counter(c: u32) {
    MERGE_INTO_DELETED_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_deleted_blocks_rows_counter(c: u32) {
    MERGE_INTO_DELETED_BLOCKS_ROWS_COUNTER.inc_by(c as u64);
}

// used to record append new blocks in matched split and not match insert.
pub fn metrics_inc_merge_into_append_blocks_counter(c: u32) {
    MERGE_INTO_APPEND_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn merge_into_distributed_deduplicate_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_DEDUPLICATE_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_empty_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_EMPTY_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_apply_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_APPLY_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_fetch_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_FETCH_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_empty_block(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_EMPTY_BLOCK.inc_by(c as u64);
}

pub fn merge_into_distributed_generate_row_numbers(c: u32) {
    MERGE_INTO_DISTRIBUTED_GENERATE_ROW_NUMBERS.inc_by(c as u64);
}

pub fn merge_into_distributed_init_unique_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_INIT_UNIQUE_NUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_new_set_len(c: u32) {
    MERGE_INTO_DISTRIBUTED_NEW_SET_LEN.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_push_empty_null_block(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_EMPTY_NULL_BLOCK.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_push_null_block(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_push_null_block_rows(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_append_blocks_rows_counter(c: u32) {
    MERGE_INTO_APPEND_BLOCKS_ROWS_COUNTER.inc_by(c as u64);
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
