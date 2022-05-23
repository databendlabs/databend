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

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::Series;
use common_datavalues::SmallVu8;
use common_exception::Result;
use common_planners::Expression;

use crate::common::ExpressionEvaluator;
use crate::common::HashMap;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::new::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::new::processors::HashJoinState;
use crate::pipelines::transforms::group_by::keys_ref::KeysRef;
use crate::sessions::QueryContext;

pub struct ChainingHashTable {
    /// Reference count
    ref_count: Mutex<usize>,
    is_finished: Mutex<bool>,

    build_expressions: Vec<Expression>,
    probe_expressions: Vec<Expression>,

    ctx: Arc<QueryContext>,

    /// A shared big hash table stores all the rows from build side
    hash_table: RwLock<HashMap<KeysRef, Vec<RowPtr>>>,
    row_space: RowSpace,
}

impl ChainingHashTable {
    pub fn try_create(
        build_expressions: Vec<Expression>,
        probe_expressions: Vec<Expression>,
        build_data_schema: DataSchemaRef,
        _probe_data_schema: DataSchemaRef,
        ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        Ok(Self {
            row_space: RowSpace::new(build_data_schema),
            ref_count: Mutex::new(0),
            is_finished: Mutex::new(false),
            build_expressions,
            probe_expressions,
            ctx,
            hash_table: RwLock::new(HashMap::create()),
        })
    }

    fn serialize_keys(&self, keys: &Vec<ColumnRef>, num_rows: usize) -> Result<Vec<SmallVu8>> {
        let mut serialized_keys = Vec::with_capacity(keys.len());

        for _i in 0..num_rows {
            serialized_keys.push(SmallVu8::new());
        }

        for col in keys {
            Series::serialize(col, &mut serialized_keys, None)?
        }
        Ok(serialized_keys)
    }
}

impl HashJoinState for ChainingHashTable {
    fn build(&self, input: DataBlock) -> Result<()> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let build_keys = self
            .build_expressions
            .iter()
            .map(|expr| ExpressionEvaluator::eval(&func_ctx, expr, &input))
            .collect::<Result<Vec<ColumnRef>>>()?;

        let serialized_build_keys = self.serialize_keys(&build_keys, input.num_rows())?;
        self.row_space.push(input, serialized_build_keys)?;

        Ok(())
    }

    fn probe(&self, input: &DataBlock) -> Result<Vec<DataBlock>> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let probe_keys = self
            .probe_expressions
            .iter()
            .map(|expr| ExpressionEvaluator::eval(&func_ctx, expr, input))
            .collect::<Result<Vec<ColumnRef>>>()?;
        let serialized_probe_keys = self.serialize_keys(&probe_keys, input.num_rows())?;
        let hash_table = self.hash_table.read().unwrap();

        let mut results: Vec<DataBlock> = vec![];
        for (i, key) in serialized_probe_keys
            .iter()
            .enumerate()
            .take(input.num_rows())
        {
            let keys_ref = KeysRef::create(key.as_ptr() as usize, key.len());
            let probe_result_ptr = hash_table.find_key(&keys_ref);
            if probe_result_ptr.is_none() {
                // No matched row for current probe row
                continue;
            }
            let probe_result_ptrs = probe_result_ptr.unwrap().get_value();
            // `result_block` is the block of build table
            let result_blocks = self.row_space.gather(probe_result_ptrs)?;
            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;

            for result_block in result_blocks.iter() {
                assert_eq!(result_block.clone().num_rows(), 1);
                assert_eq!(probe_block.clone().num_rows(), 1);
                let mut input_block = probe_block.clone();
                for (col, field) in result_block
                    .columns()
                    .iter()
                    .zip(result_block.schema().fields().iter())
                {
                    input_block = input_block.add_column(col.clone(), field.clone())?;
                }
                results.push(input_block);
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
        let mut hash_table = self.hash_table.write().unwrap();

        let chunks = self.row_space.chunks.write().unwrap();
        for chunk_index in 0..chunks.len() {
            let chunk = &chunks[chunk_index];
            let mut inserted = true;
            for (row_index, key) in chunk.keys.iter().enumerate().take(chunk.num_rows()) {
                let ptr = RowPtr {
                    chunk_index: chunk_index as u32,
                    row_index: row_index as u32,
                };
                let keys_ref = KeysRef::create(key.as_ptr() as usize, key.len());
                let entity = hash_table.insert_key(&keys_ref, &mut inserted);
                if inserted {
                    entity.set_value(vec![ptr]);
                } else {
                    entity.get_mut_value().push(ptr);
                }
            }
        }

        Ok(())
    }
}
