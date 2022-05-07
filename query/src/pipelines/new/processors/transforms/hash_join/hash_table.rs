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
use common_exception::Result;
use common_planners::Expression;

use crate::common::ExpressionEvaluator;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashUtil;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashVector;
use crate::pipelines::new::processors::transforms::hash_join::row::compare_and_combine;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::new::processors::transforms::hash_join::row::RowSpace;
use crate::sessions::QueryContext;

/// Concurrent hash table for hash join.
pub trait HashJoinState: Send + Sync {
    /// Build hash table with input DataBlock
    fn build(&self, input: DataBlock) -> Result<()>;

    /// Probe the hash table and retrieve matched rows as DataBlocks
    fn probe(&self, input: &DataBlock) -> Result<Vec<DataBlock>>;

    /// Attach to state
    fn attach(&self) -> Result<()>;

    /// Detach to state
    fn detach(&self) -> Result<()>;

    /// Is building finished.
    fn is_finished(&self) -> Result<bool>;

    /// Finish building hash table, will be called only once as soon as all handles
    /// have been detached from current state.
    fn finish(&self) -> Result<()>;
}

pub struct ChainHashTable {
    /// Reference count
    ref_count: Mutex<usize>,
    is_finished: Mutex<bool>,

    build_expressions: Vec<Expression>,
    probe_expressions: Vec<Expression>,

    ctx: Arc<QueryContext>,

    /// A shared big hash table stores all the rows from build side
    hash_table: RwLock<Vec<Option<RowPtr>>>,
    row_space: RowSpace,
}

impl ChainHashTable {
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
            hash_table: RwLock::new(vec![]),
        })
    }

    fn get_matched_ptrs(&self, hash_key: u64) -> Vec<RowPtr> {
        let hash_table = self.hash_table.read().unwrap();
        let mut ptr: Option<RowPtr> = hash_table[hash_key as usize];
        let mut result: Vec<RowPtr> = vec![];

        while let Some(v) = ptr {
            result.push(v);
            ptr = self.row_space.get_next(&v);
        }

        result
    }

    fn hash(&self, columns: &[ColumnRef], row_count: usize) -> Result<HashVector> {
        let hash_values = columns
            .iter()
            .map(|col| HashUtil::compute_hash(col))
            .collect::<Result<Vec<HashVector>>>()?;
        Ok(HashUtil::combine_hashes(&hash_values, row_count))
    }

    fn apply_capacity(hash_vector: &HashVector, capacity: usize) -> HashVector {
        // TODO: implement in a more efficient way
        let mut result = HashVector::with_capacity(capacity);
        for hash in hash_vector {
            result.push(*hash % (capacity as u64));
        }
        result
    }
}

impl HashJoinState for ChainHashTable {
    fn build(&self, input: DataBlock) -> Result<()> {
        let build_keys = self
            .build_expressions
            .iter()
            .map(|expr| {
                ExpressionEvaluator::eval(self.ctx.try_get_function_context()?, expr, &input)
            })
            .collect::<Result<Vec<ColumnRef>>>()?;

        let hash_values = self.hash(&build_keys, input.num_rows())?;

        self.row_space.push(input, hash_values)?;

        Ok(())
    }

    fn probe(&self, input: &DataBlock) -> Result<Vec<DataBlock>> {
        let probe_keys = self
            .probe_expressions
            .iter()
            .map(|expr| {
                ExpressionEvaluator::eval(self.ctx.try_get_function_context()?, expr, input)
            })
            .collect::<Result<Vec<ColumnRef>>>()?;

        let hash_values = self.hash(&probe_keys, input.num_rows())?;
        let hash_values =
            ChainHashTable::apply_capacity(&hash_values, self.hash_table.read().unwrap().len());

        let mut results: Vec<DataBlock> = vec![];
        for (i, hash_value) in hash_values.iter().enumerate().take(input.num_rows()) {
            let probe_result_ptrs = self.get_matched_ptrs(*hash_value);
            if probe_result_ptrs.is_empty() {
                // No matched row for current probe row
                continue;
            }
            let result_block = self.row_space.gather(&probe_result_ptrs)?;

            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
            let mut replicated_probe_block = DataBlock::empty();
            for (i, col) in probe_block.columns().iter().enumerate() {
                let replicated_col = col.replicate(&[result_block.num_rows()]);
                replicated_probe_block = replicated_probe_block
                    .add_column(replicated_col, probe_block.schema().field(i).clone())?;
            }

            let build_keys = self
                .build_expressions
                .iter()
                .map(|expr| {
                    ExpressionEvaluator::eval(
                        self.ctx.try_get_function_context()?,
                        expr,
                        &result_block,
                    )
                })
                .collect::<Result<Vec<ColumnRef>>>()?;

            let probe_keys = self
                .probe_expressions
                .iter()
                .map(|expr| {
                    ExpressionEvaluator::eval(
                        self.ctx.try_get_function_context()?,
                        expr,
                        &replicated_probe_block,
                    )
                })
                .collect::<Result<Vec<ColumnRef>>>()?;

            let output = compare_and_combine(
                replicated_probe_block,
                result_block,
                &build_keys,
                &probe_keys,
                self.ctx.clone(),
            )?;
            results.push(output);
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
        hash_table.resize(self.row_space.num_rows(), None);

        {
            let mut chunks = self.row_space.chunks.write().unwrap();
            for chunk_index in 0..chunks.len() {
                let chunk = &chunks[chunk_index];
                let hash_values =
                    ChainHashTable::apply_capacity(&chunk.hash_values, hash_table.len());
                for (row_index, hash_value) in hash_values.iter().enumerate().take(chunk.num_rows())
                {
                    let ptr = RowPtr {
                        chunk_index: chunk_index as u32,
                        row_index: row_index as u32,
                    };

                    if let Some(previous_ptr) = &hash_table[*hash_value as usize] {
                        chunks[ptr.chunk_index as usize].next_ptr[ptr.row_index as usize] =
                            Some(*previous_ptr);
                    }
                    hash_table[*hash_value as usize] = Some(ptr);
                }
            }
        }

        Ok(())
    }
}
