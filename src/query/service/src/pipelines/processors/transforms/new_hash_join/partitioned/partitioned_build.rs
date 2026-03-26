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
use databend_common_base::hints::assume;
use databend_common_column::binary::BinaryColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::FixedKey;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::with_hash_method;
use ethnum::u256;

use super::chunk_accumulator::FixedSizeChunkAccumulator;
use super::compact_hash_table::CompactJoinHashTable;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;
use crate::pipelines::processors::transforms::partitioned::RowIndex;
use crate::pipelines::processors::transforms::unpartitioned::hashtable::basic::AllUnmatchedProbeStream;
use crate::pipelines::processors::transforms::unpartitioned::hashtable::basic::EmptyProbeStream;

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

pub struct ProbeData {
    keys: DataBlock,
    valids: Option<Bitmap>,
}

impl ProbeData {
    pub fn new(keys: DataBlock, valids: Option<Bitmap>) -> Self {
        ProbeData { keys, valids }
    }

    pub fn num_rows(&self) -> usize {
        self.keys.num_rows()
    }

    pub fn columns(&self) -> &[BlockEntry] {
        self.keys.columns()
    }

    pub fn non_null_rows(&self) -> usize {
        match &self.valids {
            None => self.keys.num_rows(),
            Some(valids) => valids.len() - valids.null_count(),
        }
    }

    pub fn into_raw(self) -> (DataBlock, Option<Bitmap>) {
        (self.keys, self.valids)
    }
}

pub enum BuildKeysStates {
    UInt8(Vec<Buffer<u8>>),
    UInt16(Vec<Buffer<u16>>),
    UInt32(Vec<Buffer<u32>>),
    UInt64(Vec<Buffer<u64>>),
    UInt128(Vec<Buffer<u128>>),
    UInt256(Vec<Buffer<u256>>),
    Binary(Vec<BinaryColumn>),
}

impl BuildKeysStates {
    pub fn get(&self, idx: usize) -> KeysState {
        match self {
            BuildKeysStates::UInt8(v) => u8::upcast(v[idx].clone()),
            BuildKeysStates::UInt16(v) => u16::upcast(v[idx].clone()),
            BuildKeysStates::UInt32(v) => u32::upcast(v[idx].clone()),
            BuildKeysStates::UInt64(v) => u64::upcast(v[idx].clone()),
            BuildKeysStates::UInt128(v) => u128::upcast(v[idx].clone()),
            BuildKeysStates::UInt256(v) => u256::upcast(v[idx].clone()),
            BuildKeysStates::Binary(v) => KeysState::Column(Column::Binary(v[idx].clone())),
        }
    }
}

impl BuildKeysStates {
    pub fn new(method: &HashMethodKind) -> Self {
        match method {
            HashMethodKind::Serializer(_) => BuildKeysStates::Binary(vec![]),
            HashMethodKind::SingleBinary(_) => BuildKeysStates::Binary(vec![]),
            HashMethodKind::KeysU8(_) => BuildKeysStates::UInt8(vec![]),
            HashMethodKind::KeysU16(_) => BuildKeysStates::UInt16(vec![]),
            HashMethodKind::KeysU32(_) => BuildKeysStates::UInt32(vec![]),
            HashMethodKind::KeysU64(_) => BuildKeysStates::UInt64(vec![]),
            HashMethodKind::KeysU128(_) => BuildKeysStates::UInt128(vec![]),
            HashMethodKind::KeysU256(_) => BuildKeysStates::UInt256(vec![]),
        }
    }
}

/// Per-thread build state for partitioned hash join.
pub struct PartitionedHashJoinState {
    pub chunks: Vec<DataBlock>,
    pub method: HashMethodKind,
    pub build_keys_states: BuildKeysStates,
    pub hash_table: CompactJoinHashTable<u32>,

    pub columns: Vec<ColumnVec>,
    pub column_types: Vec<DataType>,

    pub num_rows: usize,
    pub build_block_idx: usize,

    pub visited: Vec<Vec<u8>>,
    pub desc: Arc<HashJoinDesc>,
    pub function_ctx: Arc<FunctionContext>,

    pub accumulator: FixedSizeChunkAccumulator,
}

impl PartitionedHashJoinState {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: Arc<FunctionContext>,
    ) -> Self {
        PartitionedHashJoinState {
            chunks: Vec::new(),
            build_keys_states: BuildKeysStates::new(&method),
            hash_table: CompactJoinHashTable::new(0),
            columns: Vec::new(),
            column_types: Vec::new(),
            num_rows: 0,
            method,
            desc,
            function_ctx,
            build_block_idx: 0,
            visited: vec![],
            accumulator: FixedSizeChunkAccumulator::new(CHUNK_SIZE),
        }
    }

    pub fn add_block<const VISITED: bool>(&mut self, data: Option<DataBlock>) -> Result<()> {
        let Some(data_block) = data else {
            if let Some(chunk) = self.accumulator.finalize() {
                self.ingest_chunk::<VISITED>(chunk)?;
            }

            return Ok(());
        };

        let data_block = self.prepare_data::<VISITED>(data_block)?;
        for ready_block in self.accumulator.accumulate(data_block) {
            self.ingest_chunk::<VISITED>(ready_block)?;
        }

        Ok(())
    }

    fn ingest_chunk<const VISITED: bool>(&mut self, chunk: DataBlock) -> Result<()> {
        let num_rows = chunk.num_rows();
        let mut columns = chunk.take_columns();
        let mut data_columns = columns.split_off(self.desc.build_keys.len());

        if VISITED && data_columns.len() != self.desc.probe_keys.len() {
            let valid_entry = data_columns.pop().unwrap();
            let valid_column = valid_entry.to_column();
            let valid_bitmap = BooleanType::try_downcast_column(&valid_column).unwrap();
            let keys_block = DataBlock::new(columns, num_rows);
            columns = keys_block.filter_with_bitmap(&valid_bitmap)?.take_columns();
        }

        let keys = ProjectedBlock::from(&columns);

        let keys_state = with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => method.build_keys_state(keys, num_rows)?,
        });

        if VISITED {
            self.visited.push(vec![0u8; num_rows]);
        }

        self.num_rows += num_rows;
        self.add_build_state(keys_state);
        self.chunks.push(DataBlock::new(data_columns, num_rows));
        Ok(())
    }

    fn prepare_data<const VISITED: bool>(&self, mut chunk: DataBlock) -> Result<DataBlock> {
        let num_rows = chunk.num_rows();

        let keys_entries = self.desc.build_key(&chunk, &self.function_ctx)?;
        let mut keys_block = DataBlock::new(keys_entries, num_rows);

        chunk = chunk.project(&self.desc.build_projection);

        if let Some(bitmap) = self.desc.build_valids_by_keys(&keys_block)? {
            if bitmap.true_count() != bitmap.len() {
                chunk = match VISITED {
                    true => {
                        let null_keys = chunk.clone().filter_with_bitmap(&(!(&bitmap)))?;
                        let nonnull_keys = chunk.filter_with_bitmap(&bitmap)?;
                        let mut chunk = DataBlock::concat(&[nonnull_keys, null_keys])?;
                        chunk.add_column(Column::Boolean(bitmap));
                        chunk
                    }
                    false => {
                        keys_block = keys_block.filter_with_bitmap(&bitmap)?;
                        chunk.filter_with_bitmap(&bitmap)?
                    }
                };
            }
        }

        self.desc.remove_keys_nullable(&mut keys_block);
        keys_block.merge_block(chunk);
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
        let keys_state = self.build_keys_states.get(self.build_block_idx);

        with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                let mut hashes = Vec::with_capacity(CHUNK_SIZE);
                method.build_keys_hashes(&keys_state, &mut hashes);
                self.hash_table.insert_chunk(&hashes, row_offset);
                self.build_block_idx += 1;
            }
        });

        match self.build_block_idx == self.chunks.len() {
            true => Ok(None),
            false => Ok(Some(ProgressValues { rows: 0, bytes: 0 })),
        }
    }

    pub fn probe<'a, const MATCHED: bool>(
        &'a self,
        data: ProbeData,
    ) -> Result<Box<dyn ProbeStream + 'a>> {
        let num_rows = data.num_rows();
        let (keys_block, valids) = data.into_raw();
        let keys = ProjectedBlock::from(keys_block.columns());
        let mut hashes = Vec::with_capacity(num_rows);

        let (keys_state, matched_rows) = with_hash_method!(|T| match &self.method {
            HashMethodKind::T(method) => {
                let keys_state = method.build_keys_state(keys, num_rows)?;
                method.build_keys_hashes(&keys_state, &mut hashes);
                (keys_state, self.hash_table.probe(&mut hashes, valids))
            }
        });

        if matched_rows == 0 {
            return match MATCHED {
                true => Ok(Box::new(EmptyProbeStream)),
                false => Ok(AllUnmatchedProbeStream::create(hashes.len())),
            };
        }

        Ok(match (&self.method, &self.build_keys_states) {
            (HashMethodKind::KeysU8(_), BuildKeysStates::UInt8(states)) => {
                let probe_keys = u8::downcast_owned(keys_state).unwrap();
                PrimitiveProbeStream::<'a, u8, MATCHED, u32>::new(
                    hashes,
                    states,
                    probe_keys,
                    &self.hash_table.next,
                )
            }
            (HashMethodKind::KeysU16(_), BuildKeysStates::UInt16(states)) => {
                let probe_keys = u16::downcast_owned(keys_state).unwrap();
                PrimitiveProbeStream::<'a, u16, MATCHED, u32>::new(
                    hashes,
                    states,
                    probe_keys,
                    &self.hash_table.next,
                )
            }
            (HashMethodKind::KeysU32(_), BuildKeysStates::UInt32(states)) => {
                let probe_keys = u32::downcast_owned(keys_state).unwrap();
                PrimitiveProbeStream::<'a, u32, MATCHED, u32>::new(
                    hashes,
                    states,
                    probe_keys,
                    &self.hash_table.next,
                )
            }
            (HashMethodKind::KeysU64(_), BuildKeysStates::UInt64(states)) => {
                let probe_keys = u64::downcast_owned(keys_state).unwrap();
                PrimitiveProbeStream::<'a, u64, MATCHED, u32>::new(
                    hashes,
                    states,
                    probe_keys,
                    &self.hash_table.next,
                )
            }
            (HashMethodKind::KeysU128(_), BuildKeysStates::UInt128(states)) => {
                let probe_keys = u128::downcast_owned(keys_state).unwrap();
                PrimitiveProbeStream::<'a, u128, MATCHED, u32>::new(
                    hashes,
                    states,
                    probe_keys,
                    &self.hash_table.next,
                )
            }
            (HashMethodKind::KeysU256(_), BuildKeysStates::UInt256(states)) => {
                let probe_keys = u256::downcast_owned(keys_state).unwrap();
                PrimitiveProbeStream::<'a, u256, MATCHED, u32>::new(
                    hashes,
                    states,
                    probe_keys,
                    &self.hash_table.next,
                )
            }
            (
                HashMethodKind::Serializer(_) | HashMethodKind::SingleBinary(_),
                BuildKeysStates::Binary(states),
            ) => match keys_state {
                KeysState::Column(Column::Binary(probe_keys))
                | KeysState::Column(Column::Variant(probe_keys))
                | KeysState::Column(Column::Bitmap(probe_keys)) => {
                    BinaryProbeStream::<'a, MATCHED, u32>::create(
                        hashes,
                        states,
                        probe_keys,
                        &self.hash_table.next,
                    )
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        })
    }

    fn add_build_state(&mut self, state: KeysState) {
        match &mut self.build_keys_states {
            BuildKeysStates::UInt8(states) => {
                states.push(u8::downcast_owned(state).unwrap());
            }
            BuildKeysStates::UInt16(states) => {
                states.push(u16::downcast_owned(state).unwrap());
            }
            BuildKeysStates::UInt32(states) => {
                states.push(u32::downcast_owned(state).unwrap());
            }
            BuildKeysStates::UInt64(states) => {
                states.push(u64::downcast_owned(state).unwrap());
            }
            BuildKeysStates::UInt128(states) => {
                states.push(u128::downcast_owned(state).unwrap());
            }
            BuildKeysStates::UInt256(states) => {
                states.push(u256::downcast_owned(state).unwrap());
            }
            BuildKeysStates::Binary(states) => match state {
                KeysState::Column(Column::Binary(build_keys))
                | KeysState::Column(Column::Variant(build_keys))
                | KeysState::Column(Column::Bitmap(build_keys)) => {
                    states.push(build_keys);
                }
                _ => unreachable!(),
            },
        };
    }
}

struct PrimitiveProbeStream<'a, T: Send + Sync + PartialEq, const MATCHED: bool, I: RowIndex = u32>
{
    key_idx: usize,
    pointers: Vec<u64>,
    build_idx: usize,
    probe_keys: Buffer<T>,
    build_keys: &'a [Buffer<T>],
    next: &'a [I],
    matched_num_rows: usize,
}

impl<'a, T: Send + Sync + PartialEq, const MATCHED: bool, I: RowIndex>
    PrimitiveProbeStream<'a, T, MATCHED, I>
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        pointers: Vec<u64>,
        build_keys: &'a [Buffer<T>],
        probe_keys: Buffer<T>,
        next: &'a [I],
    ) -> Box<dyn ProbeStream + 'a> {
        Box::new(Self {
            next,
            pointers,
            probe_keys,
            build_keys,
            key_idx: 0,
            build_idx: 0,
            matched_num_rows: 0,
        })
    }
}

impl<'a, T: Send + Sync + PartialEq, const MATCHED: bool, I: RowIndex> ProbeStream
    for PrimitiveProbeStream<'a, T, MATCHED, I>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        while self.key_idx < self.probe_keys.len() {
            assume(res.matched_probe.len() == res.matched_build.len());
            assume(res.matched_build.len() < res.matched_build.capacity());
            assume(res.matched_probe.len() < res.matched_probe.capacity());
            assume(self.key_idx < self.pointers.len());

            if res.matched_probe.len() == max_rows {
                break;
            }

            if self.build_idx == 0 {
                self.build_idx = self.pointers[self.key_idx].to_usize();

                if self.build_idx == 0 {
                    if !MATCHED {
                        res.unmatched.push(self.key_idx as u64);
                    }

                    self.key_idx += 1;
                    self.matched_num_rows = 0;
                    continue;
                }
            }

            while self.build_idx != 0 {
                let row_ptr = flat_to_row_ptr(self.build_idx);

                if self.probe_keys[self.key_idx]
                    == self.build_keys[row_ptr.chunk_index as usize][row_ptr.row_index as usize]
                {
                    res.matched_build.push(row_ptr);
                    res.matched_probe.push(self.key_idx as u64);
                    self.matched_num_rows += 1;

                    if res.matched_probe.len() == max_rows {
                        self.build_idx = self.next[self.build_idx].to_usize();

                        if self.build_idx == 0 {
                            self.key_idx += 1;
                            self.matched_num_rows = 0;
                        }

                        return Ok(());
                    }
                }

                self.build_idx = self.next[self.build_idx].to_usize();
            }

            if !MATCHED && self.matched_num_rows == 0 {
                res.unmatched.push(self.key_idx as u64);
            }

            self.key_idx += 1;
            self.matched_num_rows = 0;
        }

        Ok(())
    }
}

struct BinaryProbeStream<'a, const MATCHED: bool, I: RowIndex = u32> {
    key_idx: usize,
    pointers: Vec<u64>,
    build_idx: usize,
    probe_keys: BinaryColumn,
    build_keys: &'a [BinaryColumn],
    next: &'a [I],
    matched_num_rows: usize,
}

impl<'a, const MATCHED: bool, I: RowIndex> BinaryProbeStream<'a, MATCHED, I> {
    pub fn create(
        pointers: Vec<u64>,
        build_keys: &'a [BinaryColumn],
        probe_keys: BinaryColumn,
        next: &'a [I],
    ) -> Box<dyn ProbeStream + 'a> {
        Box::new(Self {
            next,
            pointers,
            probe_keys,
            build_keys,
            key_idx: 0,
            build_idx: 0,
            matched_num_rows: 0,
        })
    }
}

impl<'a, const MATCHED: bool, I: RowIndex> ProbeStream for BinaryProbeStream<'a, MATCHED, I> {
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        while self.key_idx < self.probe_keys.len() {
            assume(res.matched_probe.len() == res.matched_build.len());
            assume(res.matched_build.len() < res.matched_build.capacity());
            assume(res.matched_probe.len() < res.matched_probe.capacity());
            assume(self.key_idx < self.pointers.len());

            if res.matched_probe.len() == max_rows {
                break;
            }

            if self.build_idx == 0 {
                self.build_idx = self.pointers[self.key_idx].to_usize();

                if self.build_idx == 0 {
                    if !MATCHED {
                        res.unmatched.push(self.key_idx as u64);
                    }

                    self.key_idx += 1;
                    self.matched_num_rows = 0;
                    continue;
                }
            }

            while self.build_idx != 0 {
                let row_ptr = flat_to_row_ptr(self.build_idx);
                if self.probe_keys.value(self.key_idx)
                    == self.build_keys[row_ptr.chunk_index as usize]
                        .value(row_ptr.row_index as usize)
                {
                    res.matched_build.push(row_ptr);
                    res.matched_probe.push(self.key_idx as u64);
                    self.matched_num_rows += 1;

                    if res.matched_probe.len() == max_rows {
                        self.build_idx = self.next[self.build_idx].to_usize();

                        if self.build_idx == 0 {
                            self.key_idx += 1;
                            self.matched_num_rows = 0;
                        }

                        return Ok(());
                    }
                }

                self.build_idx = self.next[self.build_idx].to_usize();
            }

            if !MATCHED && self.matched_num_rows == 0 {
                res.unmatched.push(self.key_idx as u64);
            }

            self.key_idx += 1;
            self.matched_num_rows = 0;
        }

        Ok(())
    }
}
