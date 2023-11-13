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
use common_expression::types::array::ArrayColumn;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableColumnBuilder;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::VariantType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::Transform;
use common_pipeline_transforms::processors::Transformer;

use crate::executor::physical_plans::LambdaFunctionDesc;
use crate::optimizer::ColumnSet;

/// `BlockOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum BlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map {
        exprs: Vec<Expr>,
        /// The index of the output columns, based on the exprs.
        projections: Option<ColumnSet>,
    },

    /// Filter the input [`DataBlock`] with the predicate `eval`.
    Filter { projections: ColumnSet, expr: Expr },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },

    /// Expand the input [`DataBlock`] with set-returning functions.
    FlatMap {
        projections: ColumnSet,
        srf_exprs: Vec<Expr>,
    },

    /// Execute lambda function on input [`DataBlock`].
    LambdaMap { funcs: Vec<LambdaFunctionDesc> },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        if input.is_empty() {
            return Ok(input);
        }
        match self {
            BlockOperator::Map { exprs, projections } => {
                let num_evals = input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .map(|a| a.num_evals);

                if let Some(num_evals) = num_evals {
                    // It's from aggregating index.
                    match projections {
                        Some(projections) => {
                            Ok(input.project_with_agg_index(projections, num_evals))
                        }
                        None => Ok(input),
                    }
                } else {
                    for expr in exprs {
                        let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                        let result = evaluator.run(expr)?;
                        let col = BlockEntry::new(expr.data_type().clone(), result);
                        input.add_column(col);
                    }
                    match projections {
                        Some(projections) => Ok(input.project(projections)),
                        None => Ok(input),
                    }
                }
            }

            BlockOperator::Filter { projections, expr } => {
                assert_eq!(expr.data_type(), &DataType::Boolean);

                let num_evals = input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .map(|a| a.num_evals);

                if let Some(num_evals) = num_evals {
                    // It's from aggregating index.
                    Ok(input.project_with_agg_index(projections, num_evals))
                } else {
                    let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                    let filter = evaluator.run(expr)?.try_downcast::<BooleanType>().unwrap();
                    let data_block = input.project(projections);
                    data_block.filter_boolean_value(&filter)
                }
            }

            BlockOperator::Project { projection } => {
                let mut result = DataBlock::new(vec![], input.num_rows());
                for index in projection {
                    result.add_column(input.get_by_offset(*index).clone());
                }
                Ok(result)
            }

            BlockOperator::FlatMap {
                projections,
                srf_exprs,
            } => {
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
                let input_num_rows = input.num_rows();
                let mut max_nums_per_row = vec![0; input_num_rows];
                let srf_results = srf_exprs
                    .iter()
                    .map(|srf_expr| eval.run_srf(srf_expr, &mut max_nums_per_row))
                    .collect::<Result<Vec<_>>>()?;
                let mut total_num_rows = 0;
                for max_nums in max_nums_per_row.iter().take(input_num_rows) {
                    total_num_rows += *max_nums;
                }

                let input_num_columns = input.num_columns();
                let mut result = DataBlock::empty();
                let mut block_is_empty = true;
                for index in 0..input_num_columns {
                    if !projections.contains(&index) {
                        continue;
                    }
                    let column = input.get_by_offset(index);
                    let mut builder =
                        ColumnBuilder::with_capacity(&column.data_type, total_num_rows);
                    for (i, max_nums) in max_nums_per_row.iter().take(input_num_rows).enumerate() {
                        let scalar_ref = unsafe { column.value.index_unchecked(i) };
                        for _ in 0..*max_nums {
                            builder.push(scalar_ref.clone());
                        }
                    }
                    let block_entry =
                        BlockEntry::new(column.data_type.clone(), Value::Column(builder.build()));
                    if block_is_empty {
                        result = DataBlock::new(vec![block_entry], total_num_rows);
                        block_is_empty = false;
                    } else {
                        result.add_column(block_entry);
                    }
                }

                for (srf_expr, srf_results) in srf_exprs.iter().zip(srf_results) {
                    if let Expr::FunctionCall { function, .. } = srf_expr {
                        match function.signature.name.as_str() {
                            "json_path_query" => {
                                let mut builder: NullableColumnBuilder<VariantType> =
                                    NullableColumnBuilder::with_capacity(total_num_rows, &[]);
                                for (i, (row_result, repeat_times)) in
                                    srf_results.into_iter().enumerate()
                                {
                                    if let Value::Column(Column::Tuple(fields)) = row_result {
                                        debug_assert!(fields.len() == 1);
                                        match &fields[0] {
                                            Column::Nullable(box nullable_column) => {
                                                match &nullable_column.column {
                                                    Column::Variant(string_column) => {
                                                        for idx in 0..repeat_times {
                                                            builder.push(unsafe {
                                                                string_column.index_unchecked(idx)
                                                            });
                                                        }
                                                        for _ in
                                                            0..(max_nums_per_row[i] - repeat_times)
                                                        {
                                                            builder.push_null();
                                                        }
                                                    }
                                                    _ => unreachable!(
                                                        "json_path_query's return type is: `DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))])`"
                                                    ),
                                                }
                                            }
                                            _ => unreachable!(
                                                "json_path_query's return type is: `DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))])`"
                                            ),
                                        };
                                    }
                                }
                                let column = builder.build().upcast();
                                let block_entry = BlockEntry::new(
                                    DataType::Tuple(vec![DataType::Nullable(Box::new(
                                        DataType::Variant,
                                    ))]),
                                    Value::Column(Column::Tuple(vec![Column::Nullable(Box::new(
                                        column,
                                    ))])),
                                );
                                if block_is_empty {
                                    result = DataBlock::new(vec![block_entry], total_num_rows);
                                    block_is_empty = false;
                                } else {
                                    result.add_column(block_entry);
                                }
                            }
                            _ => {
                                let mut result_data_blocks = Vec::with_capacity(input.num_rows());
                                for (i, (mut row_result, repeat_times)) in
                                    srf_results.into_iter().enumerate()
                                {
                                    if let Value::Column(Column::Tuple(fields)) = &mut row_result {
                                        // If the current result set has less rows than the max number of rows,
                                        // we need to pad the result set with null values.
                                        // TODO(leiysky): this can be optimized by using a `zip` array function
                                        if repeat_times < max_nums_per_row[i] {
                                            for field in fields {
                                                match field {
                                                    Column::Null { .. } => {
                                                        *field = ColumnBuilder::repeat(
                                                            &ScalarRef::Null,
                                                            max_nums_per_row[i],
                                                            &DataType::Null,
                                                        )
                                                        .build();
                                                    }
                                                    Column::Nullable(box nullable_column) => {
                                                        let mut column_builder =
                                                            NullableColumnBuilder::from_column(
                                                                (*nullable_column).clone(),
                                                            );
                                                        (0..(max_nums_per_row[i] - repeat_times))
                                                            .for_each(|_| {
                                                                column_builder.push_null();
                                                            });
                                                        *field = Column::Nullable(Box::new(
                                                            column_builder.build(),
                                                        ));
                                                    }
                                                    _ => unreachable!(),
                                                }
                                            }
                                        }
                                    } else {
                                        row_result = Value::Column(
                                            ColumnBuilder::repeat(
                                                &ScalarRef::Tuple(vec![ScalarRef::Null]),
                                                max_nums_per_row[i],
                                                srf_expr.data_type(),
                                            )
                                            .build(),
                                        );
                                    }

                                    let block_entry =
                                        BlockEntry::new(srf_expr.data_type().clone(), row_result);
                                    result_data_blocks.push(DataBlock::new(
                                        vec![block_entry],
                                        max_nums_per_row[i],
                                    ))
                                }
                                let data_block = DataBlock::concat(&result_data_blocks)?;
                                debug_assert!(data_block.num_rows() == total_num_rows);
                                let block_entry = BlockEntry::new(
                                    data_block.get_by_offset(0).data_type.clone(),
                                    data_block.get_by_offset(0).value.clone(),
                                );
                                if block_is_empty {
                                    result = DataBlock::new(vec![block_entry], total_num_rows);
                                    block_is_empty = false;
                                } else {
                                    result.add_column(block_entry);
                                }
                            }
                        }
                    } else {
                        unreachable!("expr is not a set returning function: {srf_expr}");
                    }
                }
                Ok(result)
            }

            BlockOperator::LambdaMap { funcs } => {
                for func in funcs {
                    let expr = func.lambda_expr.as_expr(&BUILTIN_FUNCTIONS);
                    // TODO: Support multi args
                    let input_column = input.get_by_offset(func.arg_indices[0]);
                    match &input_column.value {
                        Value::Scalar(s) => match s {
                            Scalar::Null => {
                                let col = BlockEntry::new(
                                    expr.data_type().clone(),
                                    input_column.value.clone(),
                                );
                                input.add_column(col);
                            }
                            Scalar::Array(c) => {
                                let entry =
                                    BlockEntry::new(c.data_type(), Value::Column(c.clone()));
                                let block = DataBlock::new(vec![entry], c.len());

                                let evaluator =
                                    Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
                                let result = evaluator.run(&expr)?;
                                let result_col =
                                    result.convert_to_full_column(expr.data_type(), c.len());

                                let col = if func.func_name == "array_filter" {
                                    let result_col = result_col.remove_nullable();
                                    let bitmap = result_col.as_boolean().unwrap();
                                    let filtered_inner_col = c.filter(bitmap);
                                    BlockEntry::new(
                                        input_column.data_type.clone(),
                                        Value::Scalar(Scalar::Array(filtered_inner_col)),
                                    )
                                } else {
                                    BlockEntry::new(
                                        DataType::Array(Box::new(expr.data_type().clone())),
                                        Value::Scalar(Scalar::Array(result_col)),
                                    )
                                };
                                input.add_column(col);
                            }
                            _ => unreachable!(),
                        },
                        Value::Column(c) => {
                            let (inner_col, inner_ty, offsets, validity) = match c {
                                Column::Array(box array_col) => (
                                    array_col.values.clone(),
                                    array_col.values.data_type(),
                                    array_col.offsets.clone(),
                                    None,
                                ),
                                Column::Nullable(box nullable_col) => match &nullable_col.column {
                                    Column::Array(box array_col) => (
                                        array_col.values.clone(),
                                        array_col.values.data_type(),
                                        array_col.offsets.clone(),
                                        Some(nullable_col.validity.clone()),
                                    ),
                                    _ => unreachable!(),
                                },
                                _ => unreachable!(),
                            };

                            let entry = BlockEntry::new(inner_ty, Value::Column(inner_col.clone()));
                            let block = DataBlock::new(vec![entry], inner_col.len());

                            let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
                            let result = evaluator.run(&expr)?;
                            let result_col =
                                result.convert_to_full_column(expr.data_type(), inner_col.len());

                            let col = if func.func_name == "array_filter" {
                                let result_col = result_col.remove_nullable();
                                let bitmap = result_col.as_boolean().unwrap();
                                let filtered_inner_col = inner_col.filter(bitmap);
                                // generate new offsets after filter.
                                let mut new_offset = 0;
                                let mut filtered_offsets = Vec::with_capacity(offsets.len());
                                filtered_offsets.push(0);
                                for offset in offsets.windows(2) {
                                    let off = offset[0] as usize;
                                    let len = (offset[1] - offset[0]) as usize;
                                    let unset_count = bitmap.null_count_range(off, len);
                                    new_offset += (len - unset_count) as u64;
                                    filtered_offsets.push(new_offset);
                                }

                                let array_col = Column::Array(Box::new(ArrayColumn {
                                    values: filtered_inner_col,
                                    offsets: filtered_offsets.into(),
                                }));
                                let col = match validity {
                                    Some(validity) => {
                                        Value::Column(Column::Nullable(Box::new(NullableColumn {
                                            column: array_col,
                                            validity,
                                        })))
                                    }
                                    None => Value::Column(array_col),
                                };
                                BlockEntry::new(input_column.data_type.clone(), col)
                            } else {
                                let array_col = Column::Array(Box::new(ArrayColumn {
                                    values: result_col,
                                    offsets,
                                }));
                                let array_ty = DataType::Array(Box::new(expr.data_type().clone()));
                                let (ty, col) = match validity {
                                    Some(validity) => (
                                        DataType::Nullable(Box::new(array_ty)),
                                        Value::Column(Column::Nullable(Box::new(NullableColumn {
                                            column: array_col,
                                            validity,
                                        }))),
                                    ),
                                    None => (array_ty, Value::Column(array_col)),
                                };
                                BlockEntry::new(ty, col)
                            };
                            input.add_column(col);
                        }
                    }
                }

                Ok(input)
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
                BlockOperator::Map { exprs, projections } => {
                    if let Some(BlockOperator::Map {
                        exprs: pre_exprs,
                        projections: pre_projections,
                    }) = results.last_mut()
                    {
                        if pre_projections.is_none() && projections.is_none() {
                            pre_exprs.extend(exprs);
                        } else {
                            results.push(BlockOperator::Map { exprs, projections });
                        }
                    } else {
                        results.push(BlockOperator::Map { exprs, projections });
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
                        BlockOperator::Filter { .. } => "Filter",
                        BlockOperator::Project { .. } => "Project",
                        BlockOperator::FlatMap { .. } => "FlatMap",
                        BlockOperator::LambdaMap { .. } => "LambdaMap",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
