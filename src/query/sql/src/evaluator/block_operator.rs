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

use common_catalog::plan::AggIndexMeta;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::ScalarRef;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

/// `BlockOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum BlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map { exprs: Vec<Expr> },

    MapWithOutput {
        exprs: Vec<Expr>,
        /// The index of the output columns, based on the exprs.
        output_indexes: Vec<usize>,
    },

    /// Filter the input [`DataBlock`] with the predicate `eval`.
    Filter { expr: Expr },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },

    /// Expand the input [`DataBlock`] with set-returning functions.
    FlatMap { srf_exprs: Vec<Expr> },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        match self {
            BlockOperator::Map { .. }
            | BlockOperator::MapWithOutput { .. }
            | BlockOperator::Filter { .. }
                if input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .is_some() =>
            {
                // It's from aggregating index.
                Ok(input)
            }
            BlockOperator::Map { exprs } => {
                for expr in exprs {
                    let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry::new(expr.data_type().clone(), result);
                    input.add_column(col);
                }
                Ok(input)
            }

            BlockOperator::MapWithOutput {
                exprs,
                output_indexes,
            } => {
                let original_num_columns = input.num_columns();
                for expr in exprs {
                    let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry::new(expr.data_type().clone(), result);
                    input.add_column(col);
                }

                let columns: Vec<BlockEntry> = input
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| {
                        *index < original_num_columns || output_indexes.contains(index)
                    })
                    .map(|(_, col)| col.clone())
                    .collect();

                let rows = input.num_rows();
                let meta = input.get_owned_meta();

                Ok(DataBlock::new_with_meta(columns, rows, meta))
            }

            BlockOperator::Filter { expr } => {
                assert_eq!(expr.data_type(), &DataType::Boolean);

                let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                let filter = evaluator.run(expr)?.try_downcast::<BooleanType>().unwrap();
                input.filter_boolean_value(&filter)
            }

            BlockOperator::Project { projection } => {
                let mut result = DataBlock::new(vec![], input.num_rows());
                for index in projection {
                    result.add_column(input.get_by_offset(*index).clone());
                }
                Ok(result)
            }

            BlockOperator::FlatMap { srf_exprs } => {
                let eval = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);

                // [
                //   srf1: [
                //     result_set1: [
                //       col1, col2, ...
                //     ],
                //     ...
                //   ],
                //   ...
                // ]
                let srf_results = srf_exprs
                    .iter()
                    .map(|srf_expr| eval.run_srf(srf_expr))
                    .collect::<Result<Vec<_>>>()?;

                let mut max_number_rows = vec![0; input.num_rows()];
                for (_, lengths) in &srf_results {
                    for (i, length) in lengths.iter().enumerate() {
                        if length > &max_number_rows[i] {
                            max_number_rows[i] = *length;
                        }
                    }
                }
                let mut need_pad = false;
                if srf_results.len() > 1 {
                    for (_, lengths) in &srf_results {
                        for (i, length) in lengths.iter().enumerate() {
                            if length < &max_number_rows[i] {
                                need_pad = true;
                            }
                        }
                        if need_pad {
                            break;
                        }
                    }
                }
                let total_rows = max_number_rows.iter().sum();

                let mut cols = Vec::with_capacity(input.num_columns() + srf_exprs.len());
                for entry in input.columns() {
                    // Take the i-th row of input data block and add it to the row.
                    let mut builder = ColumnBuilder::with_capacity(&entry.data_type, total_rows);
                    for (row, max_number_row) in
                        max_number_rows.iter().enumerate().take(input.num_rows())
                    {
                        let scalar_ref = entry.value.index(row).unwrap();
                        (0..*max_number_row).for_each(|_| {
                            builder.push(scalar_ref.clone());
                        });
                    }
                    cols.push(BlockEntry::new(
                        entry.data_type.clone(),
                        Value::Column(builder.build()),
                    ));
                }
                // If the current result set has less rows than the max number of rows,
                // we need to pad the result set with null values.
                if need_pad {
                    for (srf_expr, (value, lengths)) in srf_exprs.iter().zip(&srf_results) {
                        if let Value::Column(Column::Tuple(fields)) = value {
                            let mut new_fields = Vec::with_capacity(fields.len());
                            for field in fields {
                                let mut offset = 0;
                                let mut builder =
                                    ColumnBuilder::with_capacity(&field.data_type(), total_rows);
                                for (length, max_number_row) in
                                    lengths.iter().zip(max_number_rows.iter())
                                {
                                    for i in offset..offset + length {
                                        let item = unsafe { field.index_unchecked(i) };
                                        builder.push(item);
                                    }
                                    offset += length;
                                    if length < max_number_row {
                                        for _ in 0..max_number_row - length {
                                            builder.push(ScalarRef::Null);
                                        }
                                    }
                                }
                                new_fields.push(builder.build());
                            }
                            let new_value = Value::Column(Column::Tuple(new_fields));
                            cols.push(BlockEntry::new(srf_expr.data_type().clone(), new_value));
                        }
                    }
                } else {
                    for (srf_expr, (value, _)) in srf_exprs.iter().zip(&srf_results) {
                        cols.push(BlockEntry::new(srf_expr.data_type().clone(), value.clone()));
                    }
                }
                let block = DataBlock::new(cols, total_rows);
                Ok(block)
            }
        }
    }
}

/// `CompoundBlockOperator` is a pipeline of `BlockOperator`s
pub struct CompoundBlockOperator {
    pub operators: Vec<BlockOperator>,
    pub ctx: FunctionContext,
}

impl CompoundBlockOperator {
    pub fn new(
        operators: Vec<BlockOperator>,
        ctx: FunctionContext,
        input_num_columns: usize,
    ) -> Self {
        let operators = Self::compact_map(operators, input_num_columns);
        Self { operators, ctx }
    }

    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        input_num_columns: usize,
        ctx: FunctionContext,
        operators: Vec<BlockOperator>,
    ) -> Box<dyn Processor> {
        let operators = Self::compact_map(operators, input_num_columns);
        Transformer::<Self>::create(input_port, output_port, Self { operators, ctx })
    }

    pub fn compact_map(
        operators: Vec<BlockOperator>,
        input_num_columns: usize,
    ) -> Vec<BlockOperator> {
        let mut results = Vec::with_capacity(operators.len());

        for op in operators {
            match op {
                BlockOperator::Map { exprs } => {
                    if let Some(BlockOperator::Map { exprs: pre_exprs }) = results.last_mut() {
                        pre_exprs.extend(exprs);
                    } else {
                        results.push(BlockOperator::Map { exprs });
                    }
                }
                _ => results.push(op),
            }
        }

        crate::evaluator::cse::apply_cse(results, input_num_columns)
    }
}

impl Transform for CompoundBlockOperator {
    const NAME: &'static str = "CompoundBlockOperator";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        self.operators
            .iter()
            .try_fold(data_block, |input, op| op.execute(&self.ctx, input))
    }

    fn name(&self) -> String {
        format!(
            "{}({})",
            Self::NAME,
            self.operators
                .iter()
                .map(|op| {
                    match op {
                        BlockOperator::Map { .. } => "Map",
                        BlockOperator::MapWithOutput { .. } => "MapWithOutput",
                        BlockOperator::Filter { .. } => "Filter",
                        BlockOperator::Project { .. } => "Project",
                        BlockOperator::FlatMap { .. } => "FlatMap",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
