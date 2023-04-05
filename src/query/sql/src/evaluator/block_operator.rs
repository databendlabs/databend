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

use common_exception::Result;
use common_expression::types::nullable::NullableColumnBuilder;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
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
            BlockOperator::Map { exprs } => {
                for expr in exprs {
                    let evaluator = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry {
                        data_type: expr.data_type().clone(),
                        value: result,
                    };
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
                    let evaluator = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry {
                        data_type: expr.data_type().clone(),
                        value: result,
                    };
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

                let evaluator = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);
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
                let eval = Evaluator::new(&input, *func_ctx, &BUILTIN_FUNCTIONS);

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

                let mut result_data_blocks = Vec::with_capacity(input.num_rows());
                for i in 0..input.num_rows() {
                    let mut row = Vec::with_capacity(input.num_columns() + srf_exprs.len());

                    // Get the max number of rows of all result sets.
                    let mut max_num_rows = 0;
                    srf_results.iter().for_each(|srf_result| {
                        let (_, result_set_rows) = &srf_result[i];
                        if *result_set_rows > max_num_rows {
                            max_num_rows = *result_set_rows;
                        }
                    });

                    if max_num_rows == 0 && !result_data_blocks.is_empty() {
                        // Skip current row
                        continue;
                    }

                    for entry in input.columns() {
                        // Take the i-th row of input data block and add it to the row.
                        let mut builder =
                            ColumnBuilder::with_capacity(&entry.data_type, max_num_rows);
                        let scalar_ref = entry.value.index(i).unwrap();
                        (0..max_num_rows).for_each(|_| {
                            builder.push(scalar_ref.clone());
                        });
                        row.push(BlockEntry {
                            value: Value::Column(builder.build()),
                            data_type: entry.data_type.clone(),
                        });
                    }

                    for (srf_expr, srf_results) in srf_exprs.iter().zip(&srf_results) {
                        let (mut row_result, repeat_times) = srf_results[i].clone();

                        if let Value::Column(Column::Tuple(fields)) = &mut row_result {
                            // If the current result set has less rows than the max number of rows,
                            // we need to pad the result set with null values.
                            // TODO(leiysky): this can be optimized by using a `zip` array function
                            if repeat_times < max_num_rows {
                                for field in fields {
                                    match field {
                                        Column::Null { .. } => {
                                            *field = ColumnBuilder::repeat(
                                                &ScalarRef::Null,
                                                max_num_rows,
                                                &DataType::Null,
                                            )
                                            .build();
                                        }
                                        Column::Nullable(box nullable_column) => {
                                            let mut column_builder =
                                                NullableColumnBuilder::from_column(
                                                    (*nullable_column).clone(),
                                                );
                                            (0..(max_num_rows - repeat_times)).for_each(|_| {
                                                column_builder.push_null();
                                            });
                                            *field =
                                                Column::Nullable(Box::new(column_builder.build()));
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            }
                        }

                        row.push(BlockEntry {
                            data_type: srf_expr.data_type().clone(),
                            value: row_result,
                        })
                    }

                    result_data_blocks.push(DataBlock::new(row, max_num_rows));
                }

                let result = DataBlock::concat(&result_data_blocks)?;
                Ok(result)
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
