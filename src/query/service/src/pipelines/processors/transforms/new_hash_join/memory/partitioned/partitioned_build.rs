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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::types::DataType;
use databend_common_expression::with_hash_method;

use super::compact_hash_table::CompactJoinHashTable;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;

pub const CHUNK_BITS: usize = 16;
pub const CHUNK_SIZE: usize = 1 << CHUNK_BITS; // 65536
const CHUNK_MASK: usize = CHUNK_SIZE - 1;

/// Convert a 1-based flat index to RowPtr (chunk_index, row_offset).
#[inline(always)]
pub fn flat_to_row_ptr(flat_index: usize) -> RowPtr {
    let zero_based = flat_index - 1;
    RowPtr {
        chunk_index: (zero_based >> CHUNK_BITS) as u32,
        row_index: (zero_based & CHUNK_MASK) as u32,
    }
}

/// Per-thread build state for partitioned hash join.
pub struct PartitionedBuild {
    /// Build blocks, each strictly CHUNK_SIZE rows (last may be shorter).
    pub chunks: Vec<DataBlock>,
    /// Per-chunk build key states for key comparison during probe.
    pub build_keys_states: Vec<KeysState>,
    /// Compact hash table (u32 row indices, 1-based).
    pub hash_table: CompactJoinHashTable<u32>,
    /// Build columns in ColumnVec format for fast gather.
    pub columns: Vec<ColumnVec>,
    /// Column types for build side.
    pub column_types: Vec<DataType>,
    /// Total build rows.
    pub num_rows: usize,
    /// Hash method for key extraction.
    pub method: HashMethodKind,
    /// Join descriptor.
    pub desc: Arc<HashJoinDesc>,
    /// Function context.
    pub function_ctx: FunctionContext,
    /// Accumulator for fixed-size chunks.
    squash_buffer: Vec<DataBlock>,
    squash_rows: usize,
}

impl PartitionedBuild {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
    ) -> Self {
        PartitionedBuild {
            chunks: Vec::new(),
            build_keys_states: Vec::new(),
            hash_table: CompactJoinHashTable::new(0),
            columns: Vec::new(),
            column_types: Vec::new(),
            num_rows: 0,
            method,
            desc,
            function_ctx,
            squash_buffer: Vec::new(),
            squash_rows: 0,
        }
    }

    /// Push a build block. None signals end of input.
    pub fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        match data {
            Some(block) if !block.is_empty() => {
                self.squash_rows += block.num_rows();
                self.squash_buffer.push(block);
                while self.squash_rows >= CHUNK_SIZE {
                    self.flush_one_chunk()?;
                }
            }
            _ => {
                if !self.squash_buffer.is_empty() {
                    let block = DataBlock::concat(&std::mem::take(&mut self.squash_buffer))?;
                    if !block.is_empty() {
                        self.num_rows += block.num_rows();
                        self.chunks.push(block);
                    }
                    self.squash_rows = 0;
                }
            }
        }
        Ok(())
    }

    /// Finalize build: extract keys, compute hashes, build compact hash table, extract ColumnVec.
    pub fn final_build(&mut self) -> Result<()> {
        if self.num_rows == 0 {
            return Ok(());
        }

        let mut all_keys_states = Vec::with_capacity(self.chunks.len());
        let mut all_hashes = Vec::with_capacity(self.num_rows);

        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                for chunk in &self.chunks {
                    let keys_entries = self.desc.build_key(chunk, &self.function_ctx)?;
                    let mut keys_block = DataBlock::new(keys_entries, chunk.num_rows());
                    self.desc.remove_keys_nullable(&mut keys_block);
                    let keys = ProjectedBlock::from(keys_block.columns());
                    let keys_state = method.build_keys_state(keys, chunk.num_rows())?;
                    method.build_keys_hashes(&keys_state, &mut all_hashes);
                    all_keys_states.push(keys_state);
                }
            }
        });

        // Build compact hash table (1-indexed)
        let mut bucket_nums = vec![0usize; self.num_rows + 1];
        self.hash_table = CompactJoinHashTable::new(self.num_rows);
        let bucket_mask = self.hash_table.bucket_mask();
        for (i, h) in all_hashes.iter().enumerate() {
            bucket_nums[i + 1] = (*h as usize) & bucket_mask;
        }
        self.hash_table.build(&bucket_nums);

        // Project build columns and extract ColumnVec
        if let Some(first_chunk) = self.chunks.first() {
            let first_projected = first_chunk.clone().project(&self.desc.build_projection);
            self.column_types = (0..first_projected.num_columns())
                .map(|offset| first_projected.get_by_offset(offset).data_type())
                .collect();

            let num_cols = first_projected.num_columns();
            let mut columns = Vec::with_capacity(num_cols);
            for offset in 0..num_cols {
                let full_columns: Vec<Column> = self
                    .chunks
                    .iter()
                    .map(|chunk| {
                        chunk
                            .clone()
                            .project(&self.desc.build_projection)
                            .get_by_offset(offset)
                            .to_column()
                    })
                    .collect();
                columns.push(Column::take_downcast_column_vec(&full_columns));
            }
            self.columns = columns;
        }

        self.build_keys_states = all_keys_states;
        Ok(())
    }

    fn flush_one_chunk(&mut self) -> Result<()> {
        let concat = DataBlock::concat(&std::mem::take(&mut self.squash_buffer))?;
        let chunk = concat.slice(0..CHUNK_SIZE).maybe_gc();
        let remain_rows = concat.num_rows() - CHUNK_SIZE;
        if remain_rows > 0 {
            let remain = concat.slice(CHUNK_SIZE..concat.num_rows()).maybe_gc();
            self.squash_buffer.push(remain);
        }
        self.squash_rows = remain_rows;
        self.num_rows += chunk.num_rows();
        self.chunks.push(chunk);
        Ok(())
    }

    pub fn reset(&mut self) {
        self.chunks.clear();
        self.build_keys_states.clear();
        self.hash_table = CompactJoinHashTable::new(0);
        self.columns.clear();
        self.column_types.clear();
        self.num_rows = 0;
        self.squash_buffer.clear();
        self.squash_rows = 0;
    }

    /// Probe the hash table with a data block. Returns matched pairs and unmatched probe indices.
    /// For each probe row, walks the hash chain and compares keys.
    pub fn probe(
        &self,
        data: &DataBlock,
    ) -> Result<(Vec<u64>, Vec<RowPtr>, Vec<u64>)> {
        if self.num_rows == 0 {
            let unmatched: Vec<u64> = (0..data.num_rows() as u64).collect();
            return Ok((vec![], vec![], unmatched));
        }

        let probe_keys_entries = self.desc.probe_key(data, &self.function_ctx)?;
        let mut probe_keys_block = DataBlock::new(probe_keys_entries, data.num_rows());
        self.desc.remove_keys_nullable(&mut probe_keys_block);

        let mut matched_probe = Vec::new();
        let mut matched_build = Vec::new();
        let mut has_match = vec![false; data.num_rows()];

        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                let keys = ProjectedBlock::from(probe_keys_block.columns());
                let probe_ks = method.build_keys_state(keys, data.num_rows())?;
                let mut probe_hashes = Vec::with_capacity(data.num_rows());
                method.build_keys_hashes(&probe_ks, &mut probe_hashes);

                let probe_acc = method.build_keys_accessor(probe_ks)?;
                let build_accs: Vec<_> = self
                    .build_keys_states
                    .iter()
                    .map(|ks| method.build_keys_accessor(ks.clone()))
                    .collect::<Result<Vec<_>>>()?;

                let bucket_mask = self.hash_table.bucket_mask();
                for probe_idx in 0..data.num_rows() {
                    let bucket = (probe_hashes[probe_idx] as usize) & bucket_mask;
                    let mut build_idx = self.hash_table.first_index(bucket);
                    while build_idx != 0 {
                        let bi = build_idx as usize;
                        let chunk_idx = (bi - 1) >> CHUNK_BITS;
                        let offset = (bi - 1) & CHUNK_MASK;
                        let build_key = unsafe { build_accs[chunk_idx].key_unchecked(offset) };
                        let probe_key = unsafe { probe_acc.key_unchecked(probe_idx) };
                        if build_key == probe_key {
                            has_match[probe_idx] = true;
                            matched_probe.push(probe_idx as u64);
                            matched_build.push(flat_to_row_ptr(bi));
                        }
                        build_idx = self.hash_table.next_index(build_idx);
                    }
                }
            }
        });

        let unmatched: Vec<u64> = has_match
            .iter()
            .enumerate()
            .filter(|(_, m)| !**m)
            .map(|(i, _)| i as u64)
            .collect();

        Ok((matched_probe, matched_build, unmatched))
    }

    /// Probe and mark visited build rows (for right join types).
    pub fn probe_and_mark_visited(
        &mut self,
        data: &DataBlock,
    ) -> Result<(Vec<u64>, Vec<RowPtr>)> {
        if self.num_rows == 0 {
            return Ok((vec![], vec![]));
        }

        let probe_keys_entries = self.desc.probe_key(data, &self.function_ctx)?;
        let mut probe_keys_block = DataBlock::new(probe_keys_entries, data.num_rows());
        self.desc.remove_keys_nullable(&mut probe_keys_block);

        let mut matched_probe = Vec::new();
        let mut matched_build = Vec::new();

        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                let keys = ProjectedBlock::from(probe_keys_block.columns());
                let probe_ks = method.build_keys_state(keys, data.num_rows())?;
                let mut probe_hashes = Vec::with_capacity(data.num_rows());
                method.build_keys_hashes(&probe_ks, &mut probe_hashes);

                let probe_acc = method.build_keys_accessor(probe_ks)?;
                let build_accs: Vec<_> = self
                    .build_keys_states
                    .iter()
                    .map(|ks| method.build_keys_accessor(ks.clone()))
                    .collect::<Result<Vec<_>>>()?;

                let bucket_mask = self.hash_table.bucket_mask();
                for probe_idx in 0..data.num_rows() {
                    let bucket = (probe_hashes[probe_idx] as usize) & bucket_mask;
                    let mut build_idx = self.hash_table.first_index(bucket);
                    while build_idx != 0 {
                        let bi = build_idx as usize;
                        let chunk_idx = (bi - 1) >> CHUNK_BITS;
                        let offset = (bi - 1) & CHUNK_MASK;
                        let build_key = unsafe { build_accs[chunk_idx].key_unchecked(offset) };
                        let probe_key = unsafe { probe_acc.key_unchecked(probe_idx) };
                        if build_key == probe_key {
                            matched_probe.push(probe_idx as u64);
                            matched_build.push(flat_to_row_ptr(bi));
                            self.hash_table.set_visited(build_idx);
                        }
                        build_idx = self.hash_table.next_index(build_idx);
                    }
                }
            }
        });

        Ok((matched_probe, matched_build))
    }

    /// Initialize visited tracking for right-side join types.
    pub fn init_visited(&mut self) {
        self.hash_table.init_visited(self.num_rows);
    }

    /// Gather build columns for the given row pointers.
    pub fn gather_build_block(&self, row_ptrs: &[RowPtr]) -> Option<DataBlock> {
        if self.columns.is_empty() {
            return None;
        }
        Some(DataBlock::take_column_vec(
            &self.columns,
            &self.column_types,
            row_ptrs,
        ))
    }
}
