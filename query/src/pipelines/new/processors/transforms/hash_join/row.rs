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
use std::sync::RwLock;

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::Expression;

use crate::common::ExpressionEvaluator;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashVector;
use crate::sessions::QueryContext;

pub struct Chunk {
    pub data_block: DataBlock,
    pub hash_values: HashVector,
}

impl Chunk {
    pub fn num_rows(&self) -> usize {
        self.data_block.num_rows()
    }
}

#[derive(Clone, Copy)]
pub struct RowPtr {
    pub chunk_index: u32,
    pub row_index: u32,
}

pub struct RowSpace {
    pub data_schema: DataSchemaRef,
    pub chunks: RwLock<Vec<Chunk>>,
}

impl RowSpace {
    pub fn new(data_schema: DataSchemaRef) -> Self {
        Self {
            data_schema,
            chunks: RwLock::new(vec![]),
        }
    }

    pub fn push(&self, data_block: DataBlock, hash_values: HashVector) -> Result<()> {
        let chunk = Chunk {
            data_block,
            hash_values,
        };

        {
            // Acquire write lock in current scope
            let mut chunks = self.chunks.write().unwrap();
            chunks.push(chunk);
        }

        Ok(())
    }

    pub fn num_rows(&self) -> usize {
        self.chunks
            .read()
            .unwrap()
            .iter()
            .fold(0, |acc, v| acc + v.num_rows())
    }

    // TODO(leiysky): gather into multiple blocks, since there are possibly massive results
    pub fn gather(&self, row_ptrs: &[RowPtr]) -> Result<DataBlock> {
        let mut data_blocks = vec![];

        {
            // Acquire read lock in current scope
            let chunks = self.chunks.read().unwrap();
            for row_ptr in row_ptrs.iter() {
                assert!((row_ptr.chunk_index as usize) < chunks.len());
                let block = self.gather_single_chunk(&chunks[row_ptr.chunk_index as usize], &[
                    row_ptr.row_index,
                ])?;
                if !block.is_empty() {
                    data_blocks.push(block);
                }
            }
        }

        if !data_blocks.is_empty() {
            let data_block = DataBlock::concat_blocks(&data_blocks)?;
            Ok(data_block)
        } else {
            Ok(DataBlock::empty_with_schema(self.data_schema.clone()))
        }
    }

    fn gather_single_chunk(&self, chunk: &Chunk, indices: &[u32]) -> Result<DataBlock> {
        DataBlock::block_take_by_indices(&chunk.data_block, indices)
    }
}

// TODO(leiysky): compare in a more efficient way
pub fn compare_and_combine(
    probe_input: DataBlock,
    probe_result: DataBlock,
    build_keys: &[ColumnRef],
    probe_keys: &[ColumnRef],
    ctx: Arc<QueryContext>,
) -> Result<DataBlock> {
    assert_eq!(build_keys.len(), probe_keys.len());
    let mut compare_exprs: Vec<Expression> = Vec::with_capacity(build_keys.len());
    let mut data_fields: Vec<DataField> = Vec::with_capacity(build_keys.len() + probe_keys.len());
    let mut columns: Vec<ColumnRef> = Vec::with_capacity(build_keys.len() + probe_keys.len());
    for (idx, (build_key, probe_key)) in build_keys.iter().zip(probe_keys).enumerate() {
        let build_key_name = format!("build_key_{idx}");
        let probe_key_name = format!("probe_key_{idx}");

        let build_key_data_type = build_key.data_type();
        let probe_key_data_type = probe_key.data_type();

        columns.push(build_key.clone());
        columns.push(probe_key.clone());

        data_fields.push(DataField::new(build_key_name.as_str(), build_key_data_type));
        data_fields.push(DataField::new(probe_key_name.as_str(), probe_key_data_type));

        let compare_expr = Expression::BinaryExpression {
            left: Box::new(Expression::Column(build_key_name.clone())),
            right: Box::new(Expression::Column(probe_key_name.clone())),
            op: "=".to_string(),
        };
        compare_exprs.push(compare_expr);
    }

    let predicate = compare_exprs
        .into_iter()
        .reduce(|prev, next| Expression::BinaryExpression {
            left: Box::new(prev),
            op: "and".to_string(),
            right: Box::new(next),
        })
        .unwrap();

    let data_block = DataBlock::create(Arc::new(DataSchema::new(data_fields)), columns);

    let filter =
        ExpressionEvaluator::eval(ctx.try_get_function_context()?, &predicate, &data_block)?;

    let mut produce_block = probe_input;
    for (col, field) in probe_result
        .columns()
        .iter()
        .zip(probe_result.schema().fields().iter())
    {
        produce_block = produce_block.add_column(col.clone(), field.clone())?;
    }
    produce_block = DataBlock::filter_block(&produce_block, &filter)?;

    Ok(produce_block)
}
