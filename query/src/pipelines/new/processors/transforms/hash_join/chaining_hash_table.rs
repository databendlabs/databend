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
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;

use crate::common::ExpressionEvaluator;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashUtil;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashVector;
use crate::pipelines::new::processors::transforms::hash_join::row::compare_and_combine;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::new::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::new::processors::HashJoinState;
use crate::sessions::QueryContext;

pub struct ChainingHashTable {
    /// Reference count
    ref_count: Mutex<usize>,
    is_finished: Mutex<bool>,

    build_expressions: Vec<Expression>,
    probe_expressions: Vec<Expression>,

    ctx: Arc<QueryContext>,

    /// A shared big hash table stores all the rows from build side
    hash_table: RwLock<Vec<Vec<RowPtr>>>,
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
            hash_table: RwLock::new(vec![]),
        })
    }

    fn hash(&self, columns: &[ColumnRef], row_count: usize) -> Result<HashVector> {
        let hash_values = columns
            .iter()
            .map(HashUtil::compute_hash)
            .collect::<Result<Vec<HashVector>>>()?;
        Ok(HashUtil::combine_hashes(&hash_values, row_count))
    }

    fn apply_bitmask(hash_vector: &HashVector, mask: u64) -> HashVector {
        let mut result = HashVector::with_capacity(hash_vector.len());
        for hash in hash_vector {
            result.push(*hash & mask);
        }
        result
    }

    fn compute_hash_table_size(rows_count: usize) -> usize {
        // The next power of 2
        let mut power = 1;
        while power < rows_count {
            power = power << 1;
        }
        power
    }
}

impl HashJoinState for ChainingHashTable {
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

        let hash_table = self.hash_table.read().unwrap();
        let hash_values = self.hash(&probe_keys, input.num_rows())?;
        let hash_values =
            ChainingHashTable::apply_bitmask(&hash_values, (hash_table.len() - 1) as u64);

        let mut results: Vec<DataBlock> = vec![];
        for (i, hash_value) in hash_values.iter().enumerate().take(input.num_rows()) {
            let probe_result_ptrs = hash_table[*hash_value as usize].as_slice();
            if probe_result_ptrs.is_empty() {
                // No matched row for current probe row
                continue;
            }
            let result_block = self.row_space.gather(probe_result_ptrs)?;

            let probe_block = DataBlock::block_take_by_indices(input, &[i as u32])?;
            let mut replicated_probe_block = DataBlock::empty();
            for (i, col) in probe_block.columns().iter().enumerate() {
                let replicated_col = ConstColumn::new(col.clone(), result_block.num_rows()).arc();

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

            let current_probe_keys: Vec<ColumnRef> = probe_keys
                .iter()
                .map(|col| {
                    let column = col.slice(i, 1);
                    ConstColumn::new(column, result_block.num_rows()).arc()
                })
                .collect();

            let output = compare_and_combine(
                replicated_probe_block,
                result_block,
                &build_keys,
                &current_probe_keys,
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
        hash_table.resize(
            ChainingHashTable::compute_hash_table_size(self.row_space.num_rows()),
            Default::default(),
        );

        {
            let chunks = self.row_space.chunks.write().unwrap();
            for chunk_index in 0..chunks.len() {
                let chunk = &chunks[chunk_index];
                let hash_values = ChainingHashTable::apply_bitmask(
                    &chunk.hash_values,
                    (hash_table.len() - 1) as u64,
                );
                for (row_index, hash_value) in hash_values.iter().enumerate().take(chunk.num_rows())
                {
                    let ptr = RowPtr {
                        chunk_index: chunk_index as u32,
                        row_index: row_index as u32,
                    };

                    hash_table[*hash_value as usize].push(ptr);
                }
            }
        }

        Ok(())
    }
}
