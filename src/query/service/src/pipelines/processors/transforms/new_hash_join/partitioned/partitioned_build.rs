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

use databend_common_base::base::ProgressValues;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::with_hash_method;

use super::chunk_accumulator::FixedSizeChunkAccumulator;
use super::compact_hash_table::CompactJoinHashTable;
use super::compact_probe_stream::create_compact_probe;
use super::compact_probe_stream::create_compact_probe_matched;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;

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
    pub chunks: Vec<DataBlock>,
    pub method: HashMethodKind,
    pub build_keys_states: Vec<KeysState>,
    pub hash_table: CompactJoinHashTable<u32>,

    pub columns: Vec<ColumnVec>,
    pub column_types: Vec<DataType>,

    pub num_rows: usize,
    pub build_block_idx: usize,

    pub visited: Vec<u8>,
    pub desc: Arc<HashJoinDesc>,
    pub function_ctx: FunctionContext,

    /// When true, NULL build keys are kept in the data (not filtered out).
    /// Required for RIGHT and RIGHT ANTI joins where unmatched build rows
    /// (including those with NULL keys) must be output in final_probe.
    keep_null_keys: bool,
    /// Per-chunk validity bitmaps for build keys (only used when keep_null_keys is true).
    /// Rows with invalid (NULL) keys are skipped during hash table insertion.
    chunk_validities: Vec<Option<Bitmap>>,

    accumulator: FixedSizeChunkAccumulator,
}

impl PartitionedBuild {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
    ) -> Self {
        Self::create_with_options(method, desc, function_ctx, false)
    }

    pub fn create_keep_null_keys(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
    ) -> Self {
        Self::create_with_options(method, desc, function_ctx, true)
    }

    fn create_with_options(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        keep_null_keys: bool,
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
            keep_null_keys,
            chunk_validities: Vec::new(),
            accumulator: FixedSizeChunkAccumulator::new(CHUNK_SIZE),
            build_block_idx: 0,
        }
    }

    pub fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        let Some(data_block) = data else {
            if let Some(chunk) = self.accumulator.finalize() {
                self.ingest_chunk(chunk)?;
            }

            return Ok(());
        };

        let data_block = self.prepare_data(data_block)?;
        for ready_block in self.accumulator.accumulate(data_block) {
            self.ingest_chunk(ready_block)?;
        }

        Ok(())
    }

    fn ingest_chunk(&mut self, chunk: DataBlock) -> Result<()> {
        let num_rows = chunk.num_rows();
        let mut columns = chunk.take_columns();

        // Extract the trailing validity column if keep_null_keys is enabled.
        let chunk_validity = if self.keep_null_keys {
            let valid_entry = columns.pop().unwrap();
            let col = valid_entry.to_column();
            Some(BooleanType::try_downcast_column(&col).unwrap())
        } else {
            None
        };

        let data_columns = columns.split_off(self.desc.build_keys.len());
        let keys = ProjectedBlock::from(&columns);

        let keys_state = with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => method.build_keys_state(keys, num_rows)?,
        });

        self.num_rows += num_rows;
        self.build_keys_states.push(keys_state);
        self.chunks.push(DataBlock::new(data_columns, num_rows));
        self.chunk_validities.push(chunk_validity);
        Ok(())
    }

    fn prepare_data(&self, mut chunk: DataBlock) -> Result<DataBlock> {
        let num_rows = chunk.num_rows();

        let keys_entries = self.desc.build_key(&chunk, &self.function_ctx)?;
        let mut keys_block = DataBlock::new(keys_entries, num_rows);

        chunk = chunk.project(&self.desc.build_projection);

        let validity = self.desc.build_valids_by_keys(&keys_block)?;
        if !self.keep_null_keys {
            if let Some(ref bitmap) = validity {
                if bitmap.true_count() != bitmap.len() {
                    keys_block = keys_block.filter_with_bitmap(bitmap)?;
                    chunk = chunk.filter_with_bitmap(bitmap)?;
                }
            }
        }

        self.desc.remove_keys_nullable(&mut keys_block);
        keys_block.merge_block(chunk);

        // When keeping NULL keys, append a boolean validity column so it flows
        // through the accumulator and can be extracted in ingest_chunk.
        if self.keep_null_keys {
            let valid_col = match validity {
                Some(bitmap) => {
                    BlockEntry::from(BooleanType::from_data(bitmap.iter().collect::<Vec<bool>>()))
                }
                None => BlockEntry::new_const_column(
                    DataType::Boolean,
                    Scalar::Boolean(true),
                    keys_block.num_rows(),
                ),
            };
            keys_block.add_entry(valid_col);
        }

        Ok(keys_block)
    }

    pub fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        if self.num_rows == 0 {
            return Ok(None);
        }

        if self.build_block_idx == 0 {
            // Allocate hash table with known total rows
            self.hash_table = CompactJoinHashTable::new(self.num_rows);

            if let Some(first_chunk) = self.chunks.first() {
                self.column_types = (0..first_chunk.num_columns())
                    .map(|offset| first_chunk.get_by_offset(offset).data_type())
                    .collect();

                let num_cols = first_chunk.num_columns();
                let mut columns = Vec::with_capacity(num_cols);
                for offset in 0..num_cols {
                    let full_columns: Vec<Column> = self
                        .chunks
                        .iter()
                        .map(|chunk| chunk.get_by_offset(offset).to_column())
                        .collect();
                    columns.push(Column::take_downcast_column_vec(&full_columns));
                }
                self.columns = columns;
            }
        }

        let row_offset = CHUNK_SIZE * self.build_block_idx + 1;
        let keys_state = &self.build_keys_states[self.build_block_idx];

        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                let mut hashes = Vec::new();
                method.build_keys_hashes(keys_state, &mut hashes);
                match &self.chunk_validities[self.build_block_idx] {
                    Some(validity) => {
                        self.hash_table
                            .insert_chunk_with_validity(&hashes, row_offset, validity);
                    }
                    None => {
                        self.hash_table.insert_chunk(&hashes, row_offset);
                    }
                }
                self.build_block_idx += 1;
            }
        });

        match self.build_block_idx == self.chunks.len() {
            true => Ok(None),
            false => Ok(Some(ProgressValues { rows: 0, bytes: 0 })),
        }
    }

    pub fn reset(&mut self) {
        self.chunks.clear();
        self.build_keys_states.clear();
        self.hash_table = CompactJoinHashTable::new(0);
        self.columns.clear();
        self.column_types.clear();
        self.num_rows = 0;
        self.build_block_idx = 0;
        self.visited.clear();
        self.chunk_validities.clear();
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
