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

use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::Notify;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodKind;
use common_datablocks::HashMethodSerializer;
use common_datavalues::combine_validities_2;
use common_datavalues::combine_validities_3;
use common_datavalues::BooleanColumn;
use common_datavalues::BooleanType;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::NullableType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashMap;
use common_planner::IndexType;
use parking_lot::RwLock;
use primitive_types::U256;
use primitive_types::U512;

use super::ProbeState;
use crate::pipelines::processors::transforms::group_by::keys_ref::KeysRef;
use crate::pipelines::processors::transforms::hash_join::desc::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::processors::transforms::hash_join::util::build_schema_wrap_nullable;
use crate::pipelines::processors::transforms::hash_join::util::probe_schema_wrap_nullable;
use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalScalar;
use crate::sql::planner::plans::JoinType;
use crate::sql::plans::JoinType::Mark;

pub struct SerializerHashTable {
    pub(crate) hash_table: HashMap<KeysRef, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct KeyU8HashTable {
    pub(crate) hash_table: HashMap<u8, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<u8>,
}

pub struct KeyU16HashTable {
    pub(crate) hash_table: HashMap<u16, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<u16>,
}

pub struct KeyU32HashTable {
    pub(crate) hash_table: HashMap<u32, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<u32>,
}

pub struct KeyU64HashTable {
    pub(crate) hash_table: HashMap<u64, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<u64>,
}

pub struct KeyU128HashTable {
    pub(crate) hash_table: HashMap<u128, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<u128>,
}

pub struct KeyU256HashTable {
    pub(crate) hash_table: HashMap<U256, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<U256>,
}

pub struct KeyU512HashTable {
    pub(crate) hash_table: HashMap<U512, Vec<RowPtr>>,
    pub(crate) hash_method: HashMethodFixedKeys<U512>,
}

pub enum HashTable {
    SerializerHashTable(SerializerHashTable),
    KeyU8HashTable(KeyU8HashTable),
    KeyU16HashTable(KeyU16HashTable),
    KeyU32HashTable(KeyU32HashTable),
    KeyU64HashTable(KeyU64HashTable),
    KeyU128HashTable(KeyU128HashTable),
    KeyU256HashTable(KeyU256HashTable),
    KeyU512HashTable(KeyU512HashTable),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
pub enum MarkerKind {
    True,
    False,
    Null,
}

pub struct MarkJoinDesc {
    pub(crate) marker_index: Option<IndexType>,
    pub(crate) has_null: RwLock<bool>,
}

pub struct JoinHashTable {
    pub(crate) ctx: Arc<QueryContext>,
    /// Reference count
    ref_count: Mutex<usize>,
    is_finished: Mutex<bool>,
    /// A shared big hash table stores all the rows from build side
    pub(crate) hash_table: RwLock<HashTable>,
    pub(crate) row_space: RowSpace,
    pub(crate) hash_join_desc: HashJoinDesc,
    pub(crate) row_ptrs: RwLock<Vec<RowPtr>>,
    pub(crate) probe_schema: DataSchemaRef,
    finished_notify: Arc<Notify>,
}

impl JoinHashTable {
    pub fn create_join_state(
        ctx: Arc<QueryContext>,
        build_keys: &[PhysicalScalar],
        build_schema: DataSchemaRef,
        probe_schema: DataSchemaRef,
        hash_join_desc: HashJoinDesc,
    ) -> Result<Arc<JoinHashTable>> {
        let hash_key_types: Vec<DataTypeImpl> =
            build_keys.iter().map(|expr| expr.data_type()).collect();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types)?;
        Ok(match method {
            HashMethodKind::Serializer(_) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::SerializerHashTable(SerializerHashTable {
                    hash_table: HashMap::<KeysRef, Vec<RowPtr>>::create(),
                    hash_method: HashMethodSerializer::default(),
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU8(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU8HashTable(KeyU8HashTable {
                    hash_table: HashMap::<u8, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU16(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU16HashTable(KeyU16HashTable {
                    hash_table: HashMap::<u16, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU32(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU32HashTable(KeyU32HashTable {
                    hash_table: HashMap::<u32, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU64(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU64HashTable(KeyU64HashTable {
                    hash_table: HashMap::<u64, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU128(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU128HashTable(KeyU128HashTable {
                    hash_table: HashMap::<u128, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU256(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU256HashTable(KeyU256HashTable {
                    hash_table: HashMap::<U256, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
            HashMethodKind::KeysU512(hash_method) => Arc::new(JoinHashTable::try_create(
                ctx,
                HashTable::KeyU512HashTable(KeyU512HashTable {
                    hash_table: HashMap::<U512, Vec<RowPtr>>::create(),
                    hash_method,
                }),
                build_schema,
                probe_schema,
                hash_join_desc,
            )?),
        })
    }

    pub fn try_create(
        ctx: Arc<QueryContext>,
        hash_table: HashTable,
        mut build_data_schema: DataSchemaRef,
        mut probe_data_schema: DataSchemaRef,
        hash_join_desc: HashJoinDesc,
    ) -> Result<Self> {
        if hash_join_desc.join_type == JoinType::Left
            || hash_join_desc.join_type == JoinType::Single
        {
            build_data_schema = build_schema_wrap_nullable(&build_data_schema);
        };
        if hash_join_desc.join_type == JoinType::Right {
            probe_data_schema = probe_schema_wrap_nullable(&probe_data_schema);
        }
        if hash_join_desc.join_type == JoinType::Full {
            build_data_schema = build_schema_wrap_nullable(&build_data_schema);
            probe_data_schema = probe_schema_wrap_nullable(&probe_data_schema);
        }
        Ok(Self {
            row_space: RowSpace::new(build_data_schema),
            ref_count: Mutex::new(0),
            is_finished: Mutex::new(false),
            hash_join_desc,
            ctx,
            hash_table: RwLock::new(hash_table),
            row_ptrs: RwLock::new(vec![]),
            probe_schema: probe_data_schema,
            finished_notify: Arc::new(Notify::new()),
        })
    }

    // Merge build block and probe block that have the same number of rows
    pub(crate) fn merge_eq_block(
        &self,
        build_block: &DataBlock,
        probe_block: &DataBlock,
    ) -> Result<DataBlock> {
        let mut probe_block = probe_block.clone();
        for (col, field) in build_block
            .columns()
            .iter()
            .zip(build_block.schema().fields().iter())
        {
            probe_block = probe_block.add_column(col.clone(), field.clone())?;
        }
        Ok(probe_block)
    }

    // Merge build block and probe block (1 row block)
    pub(crate) fn merge_with_constant_block(
        &self,
        build_block: &DataBlock,
        probe_block: &DataBlock,
    ) -> Result<DataBlock> {
        let mut replicated_probe_block = DataBlock::empty();
        for (i, col) in probe_block.columns().iter().enumerate() {
            let replicated_col = ConstColumn::new(col.clone(), build_block.num_rows()).arc();

            replicated_probe_block = replicated_probe_block
                .add_column(replicated_col, probe_block.schema().field(i).clone())?;
        }
        for (col, field) in build_block
            .columns()
            .iter()
            .zip(build_block.schema().fields().iter())
        {
            replicated_probe_block =
                replicated_probe_block.add_column(col.clone(), field.clone())?;
        }
        Ok(replicated_probe_block)
    }

    pub(crate) fn probe_cross_join(
        &self,
        input: &DataBlock,
        _probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let build_blocks = self.row_space.datablocks();
        let num_rows = build_blocks
            .iter()
            .fold(0, |acc, block| acc + block.num_rows());
        if build_blocks.is_empty() || num_rows == 0 {
            let mut fields = input.schema().fields().to_vec();
            fields.extend(self.row_space.schema().fields().clone());
            return Ok(vec![DataBlock::empty_with_schema(
                DataSchemaRefExt::create(fields.clone()),
            )]);
        }
        let build_block = DataBlock::concat_blocks(&build_blocks)?;
        let mut results: Vec<DataBlock> = Vec::with_capacity(input.num_rows());
        for i in 0..input.num_rows() {
            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
            results.push(self.merge_with_constant_block(&build_block, &probe_block)?);
        }
        Ok(results)
    }

    fn probe_join(
        &self,
        input: &DataBlock,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let probe_keys = self
            .hash_join_desc
            .probe_keys
            .iter()
            .map(|expr| Ok(expr.eval(&func_ctx, input)?.vector().clone()))
            .collect::<Result<Vec<ColumnRef>>>()?;
        let probe_keys = probe_keys.iter().collect::<Vec<&ColumnRef>>();

        if probe_keys.iter().any(|c| c.is_nullable() || c.is_null()) {
            let mut valids = None;
            for col in probe_keys.iter() {
                let (is_all_null, tmp_valids) = col.validity();
                if is_all_null {
                    let mut m = MutableBitmap::with_capacity(input.num_rows());
                    m.extend_constant(input.num_rows(), false);
                    valids = Some(m.into());
                    break;
                } else {
                    valids = combine_validities_2(valids, tmp_valids.cloned());
                }
            }
            probe_state.valids = valids;
        }

        match &*self.hash_table.read() {
            HashTable::SerializerHashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                let keys_ref =
                    keys_iter.map(|key| KeysRef::create(key.as_ptr() as usize, key.len()));

                self.result_blocks(&table.hash_table, probe_state, keys_ref, input)
            }
            HashTable::KeyU8HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU16HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU32HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU64HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU128HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU256HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
            HashTable::KeyU512HashTable(table) => {
                let keys_state = table
                    .hash_method
                    .build_keys_state(&probe_keys, input.num_rows())?;
                let keys_iter = table.hash_method.build_keys_iter(&keys_state)?;
                self.result_blocks(&table.hash_table, probe_state, keys_iter, input)
            }
        }
    }

    fn find_unmatched_build_indexes(&self) -> Result<Vec<RowPtr>> {
        // For right/full join, build side will appear at least once in the joined table
        // Find the unmatched rows in build side
        let mut unmatched_build_indexes = vec![];
        let build_indexes = self.hash_join_desc.right_join_desc.build_indexes.read();
        let build_indexes_set: HashSet<&RowPtr> = build_indexes.iter().collect();
        // TODO(xudong): remove the line of code below after https://github.com/rust-lang/rust-clippy/issues/8987
        #[allow(clippy::significant_drop_in_scrutinee)]
        for (chunk_index, chunk) in self.row_space.chunks.read().unwrap().iter().enumerate() {
            for row_index in 0..chunk.num_rows() {
                let row_ptr = RowPtr {
                    chunk_index: chunk_index as u32,
                    row_index: row_index as u32,
                    marker: None,
                };
                if !build_indexes_set.contains(&row_ptr) {
                    let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
                    row_state.entry(row_ptr).or_insert(0_usize);
                    unmatched_build_indexes.push(row_ptr);
                }
                if self.hash_join_desc.join_type == JoinType::Full {
                    if let Some(row_ptr) = build_indexes_set.get(&row_ptr) {
                        // If `marker` == `MarkerKind::False`, it means the row in build side has been filtered in left probe phase
                        if row_ptr.marker == Some(MarkerKind::False) {
                            unmatched_build_indexes.push(**row_ptr);
                        }
                    }
                }
            }
        }
        Ok(unmatched_build_indexes)
    }
}

#[async_trait::async_trait]
impl HashJoinState for JoinHashTable {
    fn build(&self, input: DataBlock) -> Result<()> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let build_cols = self
            .hash_join_desc
            .build_keys
            .iter()
            .map(|expr| Ok(expr.eval(&func_ctx, &input)?.vector().clone()))
            .collect::<Result<Vec<ColumnRef>>>()?;
        self.row_space.push_cols(input, build_cols)
    }

    fn probe(&self, input: &DataBlock, probe_state: &mut ProbeState) -> Result<Vec<DataBlock>> {
        match self.hash_join_desc.join_type {
            JoinType::Inner
            | JoinType::Semi
            | JoinType::Anti
            | JoinType::Left
            | Mark
            | JoinType::Single
            | JoinType::Right
            | JoinType::Full => self.probe_join(input, probe_state),
            JoinType::Cross => self.probe_cross_join(input, probe_state),
        }
    }

    fn attach(&self) -> Result<()> {
        let mut count = self.ref_count.lock().unwrap();
        *count += 1;
        Ok(())
    }

    fn detach(&self) -> Result<()> {
        let mut count = self.ref_count.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.finish()?;
            let mut is_finished = self.is_finished.lock().unwrap();
            *is_finished = true;
            self.finished_notify.notify_waiters();
            Ok(())
        } else {
            Ok(())
        }
    }

    fn is_finished(&self) -> Result<bool> {
        Ok(*self.is_finished.lock().unwrap())
    }

    fn finish(&self) -> Result<()> {
        macro_rules! insert_key {
            ($table: expr, $markers: expr, $method: expr, $chunk: expr, $columns: expr,  $chunk_index: expr, ) => {{
                let keys_state = $method.build_keys_state(&$columns, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                for (row_index, key) in build_keys_iter.enumerate().take($chunk.num_rows()) {
                    let mut inserted = true;
                    let ptr = RowPtr {
                        chunk_index: $chunk_index as u32,
                        row_index: row_index as u32,
                        marker: $markers[row_index],
                    };
                    {
                        let mut self_row_ptrs = self.row_ptrs.write();
                        self_row_ptrs.push(ptr.clone());
                    }
                    let entity = $table.insert_key(&key, &mut inserted);
                    if inserted {
                        entity.set_value(vec![ptr]);
                    } else {
                        entity.get_mut_value().push(ptr);
                    }
                }
            }};
        }

        let mut chunks = self.row_space.chunks.write().unwrap();
        for chunk_index in 0..chunks.len() {
            let chunk = &mut chunks[chunk_index];
            let mut columns = Vec::with_capacity(chunk.cols.len());
            let markers = if self.hash_join_desc.join_type == Mark {
                let mut markers = vec![Some(MarkerKind::False); chunk.num_rows()];
                // Only all columns' values are NULL, we set the marker to Null.
                if chunk.cols.iter().any(|c| c.is_nullable() || c.is_null()) {
                    let mut valids = None;
                    for col in chunk.cols.iter() {
                        let (is_all_null, tmp_valids) = col.validity();
                        if is_all_null {
                            let mut m = MutableBitmap::with_capacity(chunk.num_rows());
                            m.extend_constant(chunk.num_rows(), false);
                            valids = Some(m.into());
                            break;
                        } else {
                            valids = combine_validities_3(valids, tmp_valids.cloned());
                        }
                    }
                    if let Some(v) = valids {
                        for (idx, marker) in markers.iter_mut().enumerate() {
                            if !v.get_bit(idx) {
                                *marker = Some(MarkerKind::Null);
                            }
                        }
                    }
                }
                markers
            } else {
                vec![None; chunk.num_rows()]
            };
            for col in chunk.cols.iter() {
                columns.push(col);
            }
            match (*self.hash_table.write()).borrow_mut() {
                HashTable::SerializerHashTable(table) => {
                    let mut build_cols_ref = Vec::with_capacity(chunk.cols.len());
                    for build_col in chunk.cols.iter() {
                        build_cols_ref.push(build_col);
                    }
                    let keys_state = table
                        .hash_method
                        .build_keys_state(&build_cols_ref, chunk.num_rows())?;
                    chunk.keys_state = Some(keys_state);
                    let build_keys_iter = table
                        .hash_method
                        .build_keys_iter(chunk.keys_state.as_ref().unwrap())?;
                    for (row_index, key) in build_keys_iter.enumerate().take(chunk.num_rows()) {
                        let mut inserted = true;
                        let ptr = RowPtr {
                            chunk_index: chunk_index as u32,
                            row_index: row_index as u32,
                            marker: markers[row_index],
                        };
                        {
                            let mut self_row_ptrs = self.row_ptrs.write();
                            self_row_ptrs.push(ptr);
                        }
                        let keys_ref = KeysRef::create(key.as_ptr() as usize, key.len());
                        let entity = table.hash_table.insert_key(&keys_ref, &mut inserted);
                        if inserted {
                            entity.set_value(vec![ptr]);
                        } else {
                            entity.get_mut_value().push(ptr);
                        }
                    }
                }
                HashTable::KeyU8HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
                HashTable::KeyU16HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
                HashTable::KeyU32HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
                HashTable::KeyU64HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
                HashTable::KeyU128HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
                HashTable::KeyU256HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
                HashTable::KeyU512HashTable(table) => insert_key! {
                    &mut table.hash_table,
                    &markers,
                    &table.hash_method,
                    chunk,
                    columns,
                    chunk_index,
                },
            }
        }
        Ok(())
    }

    async fn wait_finish(&self) -> Result<()> {
        if !self.is_finished()? {
            self.finished_notify.notified().await;
        }

        Ok(())
    }

    fn mark_join_blocks(&self) -> Result<Vec<DataBlock>> {
        let mut row_ptrs = self.row_ptrs.write();
        let has_null = self.hash_join_desc.marker_join_desc.has_null.read();
        let mut validity = MutableBitmap::with_capacity(row_ptrs.len());
        let mut boolean_bit_map = MutableBitmap::with_capacity(row_ptrs.len());
        for row_ptr in row_ptrs.iter_mut() {
            let marker = row_ptr.marker.unwrap();
            if marker == MarkerKind::False && *has_null {
                row_ptr.marker = Some(MarkerKind::Null);
            }
            if marker == MarkerKind::Null {
                validity.push(false);
            } else {
                validity.push(true);
            }
            if marker == MarkerKind::True {
                boolean_bit_map.push(true);
            } else {
                boolean_bit_map.push(false);
            }
        }
        // transfer marker to a Nullable(BooleanColumn)
        let boolean_column = BooleanColumn::from_arrow_data(boolean_bit_map.into());
        let marker_column = Self::set_validity(&boolean_column.arc(), &validity.into())?;
        let marker_schema = DataSchema::new(vec![DataField::new(
            &self
                .hash_join_desc
                .marker_join_desc
                .marker_index
                .ok_or_else(|| ErrorCode::LogicalError("Invalid mark join"))?
                .to_string(),
            NullableType::new_impl(BooleanType::new_impl()),
        )]);
        let marker_block =
            DataBlock::create(DataSchemaRef::from(marker_schema), vec![marker_column]);
        let build_block = self.row_space.gather(&row_ptrs)?;
        Ok(vec![self.merge_eq_block(&marker_block, &build_block)?])
    }

    fn right_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        let unmatched_build_indexes = self.find_unmatched_build_indexes()?;
        if unmatched_build_indexes.is_empty() && self.hash_join_desc.other_predicate.is_none() {
            return Ok(blocks.to_vec());
        }

        let mut unmatched_build_block = self.row_space.gather(&unmatched_build_indexes)?;
        if self.hash_join_desc.join_type == JoinType::Full {
            let nullable_unmatched_build_columns = unmatched_build_block
                .columns()
                .iter()
                .map(|c| {
                    let mut probe_validity = MutableBitmap::new();
                    probe_validity.extend_constant(c.len(), true);
                    let probe_validity: Bitmap = probe_validity.into();
                    Self::set_validity(c, &probe_validity)
                })
                .collect::<Result<Vec<_>>>()?;
            unmatched_build_block =
                DataBlock::create(self.row_space.schema(), nullable_unmatched_build_columns);
        };
        // Create null block for unmatched rows in probe side
        let null_probe_block = DataBlock::create(
            self.probe_schema.clone(),
            self.probe_schema
                .fields()
                .iter()
                .map(|df| {
                    df.data_type()
                        .clone()
                        .create_constant_column(&DataValue::Null, unmatched_build_indexes.len())
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let mut merged_block = self.merge_eq_block(&unmatched_build_block, &null_probe_block)?;
        merged_block = DataBlock::concat_blocks(&[blocks, &[merged_block]].concat())?;

        // Don't need process non-equi conditions for full join in the method
        // Because non-equi conditions have been processed in left probe join
        if self.hash_join_desc.other_predicate.is_none()
            || self.hash_join_desc.join_type == JoinType::Full
        {
            return Ok(vec![merged_block]);
        }

        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        if all_true {
            return Ok(vec![merged_block]);
        }

        let validity = match (bm, all_false) {
            (Some(b), _) => b,
            (None, true) => Bitmap::new_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };
        let probe_column_len = self.probe_schema.fields().len();
        let probe_columns = merged_block.columns()[0..probe_column_len]
            .iter()
            .map(|c| Self::set_validity(c, &validity))
            .collect::<Result<Vec<_>>>()?;
        let probe_block = DataBlock::create(self.probe_schema.clone(), probe_columns);
        let build_block = DataBlock::create(
            self.row_space.data_schema.clone(),
            merged_block.columns()[probe_column_len..].to_vec(),
        );
        merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        // If build_indexes size will greater build table size, we need filter the redundant rows for build side.
        let mut build_indexes = self.hash_join_desc.right_join_desc.build_indexes.write();
        let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
        build_indexes.extend(&unmatched_build_indexes);
        if build_indexes.len() > self.row_space.rows_number() {
            let mut bm = validity.into_mut().right().unwrap();
            Self::filter_rows_for_right_join(&mut bm, &build_indexes, &mut row_state);
            let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
            let filtered_block = DataBlock::filter_block(merged_block, &predicate)?;
            return Ok(vec![filtered_block]);
        }

        Ok(vec![merged_block])
    }
}
