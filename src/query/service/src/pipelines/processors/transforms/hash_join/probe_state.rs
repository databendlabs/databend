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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::KeysState;
use databend_common_expression::filter::FilterExecutor;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::sql::plans::JoinType;

#[derive(Debug)]
pub struct ProcessState {
    pub input: DataBlock,
    // used in left mark join now
    pub probe_has_null: bool,
    pub keys_state: KeysState,
    pub next_idx: usize,
}

/// ProbeState used for probe phase of hash join.
/// We may need some reusable state for probe phase.
pub struct ProbeState {
    pub(crate) process_state: Option<ProcessState>,

    pub(crate) max_block_size: usize,
    // The `mutable_indexes` is used to call `take` or `gather`.
    pub(crate) mutable_indexes: MutableIndexes,
    // The `generation_state` is used to generate probe side `DataBlock`.
    pub(crate) generation_state: ProbeBlockGenerationState,
    // The `hashes` store hashes of keys and will be converted in-place to pointers for memory reuse.
    pub(crate) hashes: Vec<u64>,
    // The `func_ctx` is used to handle `other_predicate`.
    pub(crate) func_ctx: FunctionContext,
    // The `row_state` is used to record whether a row in probe input is matched.
    pub(crate) row_state: Option<Vec<usize>>,
    // The row_state_indexes[idx] = i records the row_state[i] has been increased 1 by the idx,
    // if idx is filtered by other conditions, we will set row_state[idx] = row_state[idx] - 1.
    // Safe to unwrap.
    pub(crate) row_state_indexes: Option<Vec<usize>>,
    // The `probe_unmatched_indexes` is used to store unmatched keys index of input.
    pub(crate) probe_unmatched_indexes: Option<Vec<u32>>,
    // The `markers` is used for right mark join.
    pub(crate) markers: Option<Vec<u8>>,
    // If hash join other condition is not empty.
    pub(crate) with_conjunction: bool,

    // Early filtering.
    // 1.The `selection` is used to store the indexes of input which matched by hash.
    pub(crate) selection: Vec<u32>,
    // 2.The indexes of [0, selection_count) in `selection` are valid.
    pub(crate) selection_count: usize,
    // 3.Statistics for **adaptive** early filtering, the `num_keys` indicates the number of valid keys
    // in probe side, the `num_keys_hash_matched` indicates the number of keys which matched by hash.
    pub(crate) num_keys: u64,
    pub(crate) num_keys_hash_matched: u64,
    // 4.Whether to probe with selection.
    pub(crate) probe_with_selection: bool,
    // 5.If join type is LEFT / LEFT SINGLE / LEFT ANTI / FULL, we use it to store unmatched indexes
    // count during early filtering.
    pub(crate) probe_unmatched_indexes_count: usize,

    pub(crate) filter_executor: Option<FilterExecutor>,

    // Marks the hash table as used by RowPtr, will not be reused
    pub(crate) used_once: Option<MutableBitmap>,
}

impl ProbeState {
    pub fn clear(&mut self) {
        // Reuse hashes vec.
        unsafe { self.hashes.set_len(0) };
    }

    pub fn create(
        max_block_size: usize,
        join_type: &JoinType,
        with_conjunction: bool,
        func_ctx: FunctionContext,
        other_predicate: Option<Expr>,
    ) -> Self {
        let (row_state, row_state_indexes) = match &join_type {
            JoinType::Left | JoinType::LeftAny | JoinType::LeftSingle | JoinType::Full => {
                if with_conjunction {
                    (Some(vec![0; max_block_size]), Some(vec![0; max_block_size]))
                } else {
                    (Some(vec![0; max_block_size]), None)
                }
            }
            _ => (None, None),
        };
        let probe_unmatched_indexes = if matches!(
            &join_type,
            JoinType::Left
                | JoinType::LeftAny
                | JoinType::LeftSingle
                | JoinType::Full
                | JoinType::LeftAnti
        ) && !with_conjunction
        {
            Some(vec![0; max_block_size])
        } else {
            None
        };
        let markers = if matches!(&join_type, JoinType::RightMark) {
            Some(vec![MARKER_KIND_FALSE; max_block_size])
        } else {
            None
        };
        let filter_executor = if !matches!(
            &join_type,
            JoinType::LeftMark | JoinType::RightMark | JoinType::Cross
        ) && let Some(predicate) = other_predicate
        {
            let filter_executor = FilterExecutor::new(
                predicate,
                func_ctx.clone(),
                max_block_size,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            );
            Some(filter_executor)
        } else {
            None
        };
        ProbeState {
            process_state: None,
            max_block_size,
            mutable_indexes: MutableIndexes::new(max_block_size),
            generation_state: ProbeBlockGenerationState::new(max_block_size),
            selection: vec![0; max_block_size],
            hashes: vec![0; max_block_size],
            selection_count: 0,
            probe_with_selection: false,
            num_keys: 1,
            num_keys_hash_matched: 1,
            func_ctx,
            row_state,
            row_state_indexes,
            probe_unmatched_indexes,
            markers,
            probe_unmatched_indexes_count: 0,
            with_conjunction,
            filter_executor,
            used_once: None,
        }
    }

    // Reset some states which changed during probe.
    // Only be called when spill is enabled.
    pub fn reset(&mut self) {
        self.num_keys = 1;
        self.num_keys_hash_matched = 1;
    }

    pub(crate) fn check_used(
        used_once: &mut Option<MutableBitmap>,
        max_block_size: usize,
        build_indexes_ptr: *mut RowPtr,
        matched_idx: usize,
    ) -> bool {
        if let Some(used_once) = used_once {
            // used_once indicates the row position of the same key in build keys,
            // and the complete row is spliced out by row ptr to get whether the corresponding position has been used.
            let ptr = unsafe { &*build_indexes_ptr.add(matched_idx) };
            let row = ptr.chunk_index as usize * max_block_size + ptr.row_index as usize;
            if used_once.get(row) {
                return true;
            }
            used_once.set(row, true);
        }
        false
    }
}

pub struct MutableIndexes {
    pub(crate) probe_indexes: Vec<u32>,
    pub(crate) build_indexes: Vec<RowPtr>,
}

impl MutableIndexes {
    fn new(size: usize) -> Self {
        Self {
            probe_indexes: vec![0; size],
            build_indexes: vec![
                RowPtr {
                    chunk_index: 0,
                    row_index: 0,
                };
                size
            ],
        }
    }
}

pub struct ProbeBlockGenerationState {
    // The is_probe_projected means whether we need to output probe columns.
    pub(crate) is_probe_projected: bool,
    // When we need a bitmap that is all true, we can directly slice it to reduce memory usage.
    pub(crate) true_validity: Bitmap,
}

impl ProbeBlockGenerationState {
    fn new(size: usize) -> Self {
        Self {
            is_probe_projected: false,
            true_validity: Bitmap::new_constant(true, size),
        }
    }
}
