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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::borrow::BorrowMut;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::Notify;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodKind;
use common_datablocks::HashMethodSerializer;
use common_datavalues::combine_validities_2;
use common_datavalues::BooleanColumn;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
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
use crate::pipelines::processors::HashTable;
use crate::pipelines::processors::JoinHashTable;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalScalar;
use crate::sql::planner::plans::JoinType;

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
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::Left
            | JoinType::LeftMark
            | JoinType::RightMark
            | JoinType::Single
            | JoinType::Right
            | JoinType::Full => self.probe_join(input, probe_state),
            JoinType::Cross => self.probe_cross_join(input, probe_state),
        }
    }

    fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Release);
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

        let interrupt = self.interrupt.clone();
        let mut chunks = self.row_space.chunks.write().unwrap();
        let mut has_null = false;
        for chunk_index in 0..chunks.len() {
            if interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let chunk = &mut chunks[chunk_index];
            let mut columns = Vec::with_capacity(chunk.cols.len());
            let markers = match self.hash_join_desc.join_type {
                JoinType::LeftMark => Self::init_markers(&chunk.cols, chunk.num_rows())
                    .iter()
                    .map(|x| Some(*x))
                    .collect(),
                JoinType::RightMark => {
                    if !has_null {
                        if let Some(validity) = chunk.cols[0].validity().1 {
                            if validity.unset_bits() > 0 {
                                has_null = true;
                                let mut has_null_ref =
                                    self.hash_join_desc.marker_join_desc.has_null.write();
                                *has_null_ref = true;
                            }
                        }
                    }
                    vec![None; chunk.num_rows()]
                }
                _ => {
                    vec![None; chunk.num_rows()]
                }
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
        let row_ptrs = self.row_ptrs.read();
        let has_null = self.hash_join_desc.marker_join_desc.has_null.read();

        let markers = row_ptrs.iter().map(|r| r.marker.unwrap()).collect();
        let marker_block = self.create_marker_block(*has_null, markers)?;
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
            self.filter_rows_for_right_join(&mut bm, &build_indexes, &mut row_state);
            let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
            let filtered_block = DataBlock::filter_block(merged_block, &predicate)?;
            return Ok(vec![filtered_block]);
        }

        Ok(vec![merged_block])
    }

    fn right_semi_join_blocks(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        // Fast path for right anti join with non-equi conditions
        if self.hash_join_desc.other_predicate.is_none()
            && self.hash_join_desc.join_type == JoinType::RightAnti
        {
            let unmatched_build_indexes = self.find_unmatched_build_indexes()?;
            let unmatched_build_block = self.row_space.gather(&unmatched_build_indexes)?;
            return Ok(vec![unmatched_build_block]);
        }
        if blocks.is_empty() {
            return Ok(vec![]);
        }
        let input_block = DataBlock::concat_blocks(blocks)?;
        let probe_fields_len = self.probe_schema.fields().len();
        let build_columns = input_block.columns()[probe_fields_len..].to_vec();
        let build_block = DataBlock::create(self.row_space.data_schema.clone(), build_columns);

        // Fast path for right semi join with non-equi conditions
        if self.hash_join_desc.other_predicate.is_none()
            && self.hash_join_desc.join_type == JoinType::RightSemi
        {
            let mut bm = MutableBitmap::new();
            bm.extend_constant(build_block.num_rows(), true);
            let filtered_block = self.filter_rows_for_right_semi_join(&mut bm, build_block)?;
            return Ok(vec![filtered_block]);
        }

        // Right anti/semi join with non-equi conditions
        let (bm, all_true, all_false) = self.get_other_filters(
            &input_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        // Fast path for all non-equi conditions are true
        if all_true {
            return if self.hash_join_desc.join_type == JoinType::RightSemi {
                let mut bm = MutableBitmap::new();
                bm.extend_constant(build_block.num_rows(), true);
                let filtered_block = self.filter_rows_for_right_semi_join(&mut bm, build_block)?;
                return Ok(vec![filtered_block]);
            } else {
                let unmatched_build_indexes = self.find_unmatched_build_indexes()?;
                let unmatched_build_block = self.row_space.gather(&unmatched_build_indexes)?;
                Ok(vec![unmatched_build_block])
            };
        }

        // Fast path for all non-equi conditions are false
        if all_false {
            return if self.hash_join_desc.join_type == JoinType::RightSemi {
                Ok(vec![DataBlock::empty_with_schema(
                    self.row_space.data_schema.clone(),
                )])
            } else {
                Ok(self.row_space.datablocks())
            };
        }

        let mut bm = bm.unwrap().into_mut().right().unwrap();

        // Right semi join with non-equi conditions
        if self.hash_join_desc.join_type == JoinType::RightSemi {
            let filtered_block = self.filter_rows_for_right_semi_join(&mut bm, build_block)?;
            return Ok(vec![filtered_block]);
        }

        // Right anti join with non-equi conditions
        let mut filtered_build_indexes: Vec<RowPtr> = vec![];
        {
            let mut build_indexes = self.hash_join_desc.right_join_desc.build_indexes.write();
            for (idx, row_ptr) in build_indexes.iter().enumerate() {
                if bm.get(idx) {
                    filtered_build_indexes.push(*row_ptr)
                }
            }
            *build_indexes = filtered_build_indexes;
        }
        let unmatched_build_indexes = self.find_unmatched_build_indexes()?;
        let unmatched_build_block = self.row_space.gather(&unmatched_build_indexes)?;
        Ok(vec![unmatched_build_block])
    }
}

impl JoinHashTable {
    pub(crate) fn filter_rows_for_right_join(
        &self,
        bm: &mut MutableBitmap,
        build_indexes: &[RowPtr],
        row_state: &mut std::collections::HashMap<RowPtr, usize>,
    ) {
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] == 1 || row_state[row] == 0 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
    }

    pub(crate) fn filter_rows_for_right_semi_join(
        &self,
        bm: &mut MutableBitmap,
        input: DataBlock,
    ) -> Result<DataBlock> {
        let build_indexes = self.hash_join_desc.right_join_desc.build_indexes.read();
        let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] > 1 && !bm.get(index) {
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] > 1 && bm.get(index) {
                bm.set(index, false);
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
        let predicate = BooleanColumn::from_arrow_data(bm.clone().into()).arc();
        DataBlock::filter_block(input, &predicate)
    }
}
