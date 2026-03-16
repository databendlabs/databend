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

use super::chunk_accumulator::FixedSizeChunkAccumulator;
use super::compact_hash_table::CompactJoinHashTable;
use super::compact_probe_stream::create_compact_probe;
use super::compact_probe_stream::create_compact_probe_matched;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;

pub const CHUNK_BITS: usize = 16;
pub const CHUNK_SIZE: usize = 1 << CHUNK_BITS; // 65536

/// Convert a 1-based flat index to RowPtr (chunk_index, row_offset).
#[inline(always)]
pub fn flat_to_row_ptr(flat_index: usize) -> RowPtr {
    let zero_based = flat_index - 1;
    RowPtr {
        chunk_index: (zero_based >> CHUNK_BITS) as u32,
        row_index: (zero_based & (CHUNK_SIZE - 1)) as u32,
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
    /// Visited bitmap for right outer/semi/anti joins (1-based indexing, index 0 unused).
    pub visited: Vec<u8>,
    /// Fixed-size chunk accumulator using mutable columns.
    accumulator: FixedSizeChunkAccumulator,
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
            visited: Vec::new(),
            accumulator: FixedSizeChunkAccumulator::new(CHUNK_SIZE),
        }
    }

    /// Push a build block. None signals end of input.
    pub fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        match data {
            Some(block) if !block.is_empty() => {
                let flushed = self.accumulator.accumulate(block);
                for chunk in flushed {
                    self.ingest_chunk(chunk)?;
                }
            }
            _ => {
                if let Some(chunk) = self.accumulator.flush() {
                    self.ingest_chunk(chunk)?;
                }
            }
        }
        Ok(())
    }

    /// Process a flushed chunk: compute KeysState immediately.
    fn ingest_chunk(&mut self, chunk: DataBlock) -> Result<()> {
        let num_rows = chunk.num_rows();
        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                let keys_entries = self.desc.build_key(&chunk, &self.function_ctx)?;
                let mut keys_block = DataBlock::new(keys_entries, num_rows);
                self.desc.remove_keys_nullable(&mut keys_block);
                let keys = ProjectedBlock::from(keys_block.columns());
                let keys_state = method.build_keys_state(keys, num_rows)?;
                self.build_keys_states.push(keys_state);
            }
        });
        self.num_rows += num_rows;
        self.chunks.push(chunk);
        Ok(())
    }

    /// Finalize build: build hash table chunk by chunk, extract ColumnVec.
    pub fn final_build(&mut self) -> Result<()> {
        if self.num_rows == 0 {
            return Ok(());
        }

        // Allocate hash table with known total rows
        self.hash_table = CompactJoinHashTable::new(self.num_rows);

        // Process one chunk at a time: compute hashes, insert into table
        let mut row_offset = 1; // 1-based indexing
        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                for keys_state in &self.build_keys_states {
                    let mut hashes = Vec::new();
                    method.build_keys_hashes(keys_state, &mut hashes);
                    self.hash_table.insert_chunk(&hashes, row_offset);
                    row_offset += hashes.len();
                }
            }
        });

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

        Ok(())
    }

    pub fn reset(&mut self) {
        self.chunks.clear();
        self.build_keys_states.clear();
        self.hash_table = CompactJoinHashTable::new(0);
        self.columns.clear();
        self.column_types.clear();
        self.num_rows = 0;
        self.visited.clear();
        self.accumulator.reset();
    }

    /// Create a probe stream that only tracks matched rows (for inner, left semi, right series).
    pub fn create_probe_matched<'a>(
        &'a self,
        data: &DataBlock,
    ) -> Result<Box<dyn ProbeStream + Send + Sync + 'a>> {
        create_compact_probe_matched(
            &self.hash_table,
            &self.build_keys_states,
            &self.method,
            &self.desc,
            &self.function_ctx,
            data,
        )
    }

    /// Create a probe stream that also tracks unmatched rows (for left, left anti).
    pub fn create_probe<'a>(
        &'a self,
        data: &DataBlock,
    ) -> Result<Box<dyn ProbeStream + Send + Sync + 'a>> {
        create_compact_probe(
            &self.hash_table,
            &self.build_keys_states,
            &self.method,
            &self.desc,
            &self.function_ctx,
            data,
        )
    }

    /// Initialize visited tracking for right-side join types.
    pub fn init_visited(&mut self) {
        self.visited = vec![0u8; self.num_rows + 1];
    }

    /// Mark a build row as visited (1-based index).
    #[inline(always)]
    pub fn set_visited(&mut self, row_index: usize) {
        unsafe {
            *self.visited.get_unchecked_mut(row_index) = 1;
        }
    }

    /// Check if a build row has been visited (1-based index).
    #[inline(always)]
    pub fn is_visited(&self, row_index: usize) -> bool {
        unsafe { *self.visited.get_unchecked(row_index) != 0 }
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
