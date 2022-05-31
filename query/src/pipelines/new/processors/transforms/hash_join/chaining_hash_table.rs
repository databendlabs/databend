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
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

use common_base::infallible::RwLock;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodSerializer;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;
use primitive_types::U256;
use primitive_types::U512;

use crate::common::ExpressionEvaluator;
use crate::common::HashMap;
use crate::common::HashTableKeyable;
use crate::pipelines::new::processors::transforms::hash_join::row::Chunk;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::new::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::new::processors::HashJoinState;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;
use crate::sessions::QueryContext;

pub enum HashTable {
    SerializerHashTable(HashMap<KeysRef, Vec<RowPtr>>),
    KeyU8HashTable(HashMap<u8, Vec<RowPtr>>),
    KeyU16HashTable(HashMap<u16, Vec<RowPtr>>),
    KeyU32HashTable(HashMap<u32, Vec<RowPtr>>),
    KeyU64HashTable(HashMap<u64, Vec<RowPtr>>),
    KeyU128HashTable(HashMap<u128, Vec<RowPtr>>),
    KeyU256HashTable(HashMap<U256, Vec<RowPtr>>),
    KeyU512HashTable(HashMap<U512, Vec<RowPtr>>),
}

pub struct ChainingHashTable {
    /// Reference count
    ref_count: Mutex<usize>,
    is_finished: Mutex<bool>,

    build_expressions: Vec<Expression>,
    probe_expressions: Vec<Expression>,

    ctx: Arc<QueryContext>,

    /// A shared big hash table stores all the rows from build side
    hash_table: RwLock<HashTable>,
    row_space: RowSpace,
}

impl ChainingHashTable {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        hash_table: HashTable,
        build_expressions: Vec<Expression>,
        probe_expressions: Vec<Expression>,
        build_data_schema: DataSchemaRef,
        _probe_data_schema: DataSchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            row_space: RowSpace::new(build_data_schema),
            ref_count: Mutex::new(0),
            is_finished: Mutex::new(false),
            build_expressions,
            probe_expressions,
            ctx,
            hash_table: RwLock::new(hash_table),
        })
    }

    fn generate_result_block<Key>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_keys: Vec<&ColumnRef>,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashTableKeyable + Hash + Clone + Default + Debug + 'static,
    {
        let mut results: Vec<DataBlock> = vec![];
        let method = HashMethodFixedKeys::<Key>::default();
        let keys = method.build_keys(&probe_keys, input.num_rows())?;
        for (i, key) in keys.iter().enumerate().take(input.num_rows()) {
            let probe_result_ptr = hash_table.find_key(key);
            if probe_result_ptr.is_none() {
                // No matched row for current probe row
                continue;
            }
            let probe_result_ptrs = probe_result_ptr.unwrap().get_value();
            // `result_block` is the block of build table
            let result_block = self.row_space.gather(probe_result_ptrs)?;
            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
            let mut replicated_probe_block = DataBlock::empty();
            for (i, col) in probe_block.columns().iter().enumerate() {
                let replicated_col = ConstColumn::new(col.clone(), result_block.num_rows()).arc();

                replicated_probe_block = replicated_probe_block
                    .add_column(replicated_col, probe_block.schema().field(i).clone())?;
            }
            for (col, field) in result_block
                .columns()
                .iter()
                .zip(result_block.schema().fields().iter())
            {
                replicated_probe_block =
                    replicated_probe_block.add_column(col.clone(), field.clone())?;
            }

            results.push(replicated_probe_block);
        }
        Ok(results)
    }
}

impl HashJoinState for ChainingHashTable {
    fn build(&self, input: DataBlock) -> Result<()> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let build_cols = self
            .build_expressions
            .iter()
            .map(|expr| ExpressionEvaluator::eval(&func_ctx, expr, &input))
            .collect::<Result<Vec<ColumnRef>>>()?;

        match &*self.hash_table.read() {
            HashTable::SerializerHashTable(_) => {
                let method = HashMethodSerializer::default();
                let mut build_cols_ref = Vec::with_capacity(build_cols.len());
                for build_col in build_cols.iter() {
                    build_cols_ref.push(build_col);
                }
                let build_keys = method.build_keys(&build_cols_ref, input.num_rows())?;
                self.row_space.push_keys(input, build_keys)
            }
            _ => self.row_space.push_cols(input, build_cols),
        }
    }

    fn probe(&self, input: &DataBlock) -> Result<Vec<DataBlock>> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let probe_keys = self
            .probe_expressions
            .iter()
            .map(|expr| ExpressionEvaluator::eval(&func_ctx, expr, input))
            .collect::<Result<Vec<ColumnRef>>>()?;
        let probe_keys = probe_keys.iter().collect::<Vec<&ColumnRef>>();
        let mut results: Vec<DataBlock> = vec![];
        match &*self.hash_table.read() {
            HashTable::SerializerHashTable(table) => {
                let method = HashMethodSerializer::default();
                let serialized_probe_keys = method.build_keys(&probe_keys, input.num_rows())?;
                for (i, key) in serialized_probe_keys
                    .iter()
                    .enumerate()
                    .take(input.num_rows())
                {
                    let keys_ref = KeysRef::create(key.as_ptr() as usize, key.len());
                    let probe_result_ptr = table.find_key(&keys_ref);
                    if probe_result_ptr.is_none() {
                        // No matched row for current probe row
                        continue;
                    }
                    let probe_result_ptrs = probe_result_ptr.unwrap().get_value();
                    let result_block = self.row_space.gather(probe_result_ptrs)?;
                    let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
                    let mut replicated_probe_block = DataBlock::empty();
                    for (i, col) in probe_block.columns().iter().enumerate() {
                        let replicated_col =
                            ConstColumn::new(col.clone(), result_block.num_rows()).arc();

                        replicated_probe_block = replicated_probe_block
                            .add_column(replicated_col, probe_block.schema().field(i).clone())?;
                    }
                    for (col, field) in result_block
                        .columns()
                        .iter()
                        .zip(result_block.schema().fields().iter())
                    {
                        replicated_probe_block =
                            replicated_probe_block.add_column(col.clone(), field.clone())?;
                    }

                    results.push(replicated_probe_block);
                }
            }
            HashTable::KeyU8HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
            HashTable::KeyU16HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
            HashTable::KeyU32HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
            HashTable::KeyU64HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
            HashTable::KeyU128HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
            HashTable::KeyU256HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
            HashTable::KeyU512HashTable(table) => {
                return self.generate_result_block(table, probe_keys, input);
            }
        }
        Ok(results)
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
            Ok(())
        } else {
            Ok(())
        }
    }

    fn is_finished(&self) -> Result<bool> {
        Ok(*self.is_finished.lock().unwrap())
    }

    fn finish(&self) -> Result<()> {
        let chunks = self.row_space.chunks.write().unwrap();
        for chunk_index in 0..chunks.len() {
            let chunk = &chunks[chunk_index];
            let mut columns = vec![];
            if let Some(cols) = chunk.cols.as_ref() {
                columns = Vec::with_capacity(cols.len());
                for col in cols.iter() {
                    columns.push(col);
                }
            }
            match (*self.hash_table.write()).borrow_mut() {
                HashTable::SerializerHashTable(table) => {
                    if let Some(keys) = chunk.keys.as_ref() {
                        for (row_index, key) in keys.iter().enumerate().take(chunk.num_rows()) {
                            let mut inserted = true;
                            let ptr = RowPtr {
                                chunk_index: chunk_index as u32,
                                row_index: row_index as u32,
                            };
                            let keys_ref = KeysRef::create(key.as_ptr() as usize, key.len());
                            let entity = table.insert_key(&keys_ref, &mut inserted);
                            if inserted {
                                entity.set_value(vec![ptr]);
                            } else {
                                entity.get_mut_value().push(ptr);
                            }
                        }
                    }
                }
                HashTable::KeyU8HashTable(table) => insert_key(table, chunk, columns, chunk_index)?,
                HashTable::KeyU16HashTable(table) => {
                    insert_key(table, chunk, columns, chunk_index)?
                }
                HashTable::KeyU32HashTable(table) => {
                    insert_key(table, chunk, columns, chunk_index)?
                }
                HashTable::KeyU64HashTable(table) => {
                    insert_key(table, chunk, columns, chunk_index)?
                }
                HashTable::KeyU128HashTable(table) => {
                    insert_key(table, chunk, columns, chunk_index)?
                }
                HashTable::KeyU256HashTable(table) => {
                    insert_key(table, chunk, columns, chunk_index)?
                }
                HashTable::KeyU512HashTable(table) => {
                    insert_key(table, chunk, columns, chunk_index)?
                }
            }
        }

        Ok(())
    }
}

fn insert_key<Key>(
    table: &mut HashMap<Key, Vec<RowPtr>>,
    chunk: &Chunk,
    columns: Vec<&ColumnRef>,
    chunk_index: usize,
) -> Result<()>
where
    Key: HashTableKeyable + Hash + Clone + Default + Debug + 'static,
{
    let method = HashMethodFixedKeys::<Key>::default();
    let build_keys = method.build_keys(&columns, chunk.num_rows())?;
    for (row_index, key) in build_keys.iter().enumerate().take(chunk.num_rows()) {
        let mut inserted = true;
        let ptr = RowPtr {
            chunk_index: chunk_index as u32,
            row_index: row_index as u32,
        };
        let entity = table.insert_key(key, &mut inserted);
        if inserted {
            entity.set_value(vec![ptr]);
        } else {
            entity.get_mut_value().push(ptr);
        }
    }
    Ok(())
}
