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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::processors::BlockingTransform;
use databend_common_pipeline_transforms::processors::BlockingTransformer;
use databend_common_sql::ColumnSet;

/// Expand the input [`DataBlock`] with set-returning functions.
pub struct TransformSRF {
    input: Option<DataBlock>,
    projections: ColumnSet,
    func_ctx: FunctionContext,
    srf_exprs: Vec<FunctionCall>,
    /// The output of each set-returning function for each input row.
    srf_results: Vec<VecDeque<(Value<AnyType>, usize)>>,
    /// The output number of rows for each input row.
    num_rows: VecDeque<usize>,
    max_block_size: usize,
    max_block_bytes: usize,
}

impl TransformSRF {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        func_ctx: FunctionContext,
        projections: ColumnSet,
        srf_exprs: Vec<Expr>,
        max_block_size: usize,
        max_block_bytes: usize,
    ) -> Box<dyn Processor> {
        let srf_results = vec![VecDeque::new(); srf_exprs.len()];
        let srf_exprs = srf_exprs
            .into_iter()
            .map(|expr| {
                expr.into_function_call().map_err(|expr| {
                    databend_common_exception::ErrorCode::BadArguments(format!(
                        "expr is not a set-returning function: {expr}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        BlockingTransformer::create(input, output, TransformSRF {
            input: None,
            projections,
            func_ctx,
            srf_exprs,
            srf_results,
            num_rows: VecDeque::new(),
            max_block_size,
            max_block_bytes,
        })
    }
}

impl BlockingTransform for TransformSRF {
    const NAME: &'static str = "TransformSRF";

    fn consume(&mut self, input: DataBlock) -> Result<()> {
        let eval = Evaluator::new(&input, &self.func_ctx, &BUILTIN_FUNCTIONS);

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
        for (i, expr) in self.srf_exprs.iter().enumerate() {
            let res = eval.run_srf(expr, &mut max_nums_per_row)?;
            debug_assert_eq!(res.len(), input_num_rows);
            self.srf_results[i] = VecDeque::from(res);
        }
        debug_assert_eq!(max_nums_per_row.len(), input_num_rows);
        debug_assert!(self.input.is_none());

        self.num_rows = VecDeque::from(max_nums_per_row);
        self.input = Some(input.project(&self.projections));

        Ok(())
    }

    fn transform(&mut self) -> Result<Option<DataBlock>> {
        if self.input.is_none() {
            return Ok(None);
        }

        let input = self.input.take().unwrap();
        if input.is_empty() {
            return Ok(None);
        }

        let mut used = 0;
        let mut result_rows = 0;
        let mut total_memory_size = 0;

        // Calculate the memory size per row for input columns
        let memory_size_per_row: usize = input
            .columns()
            .iter()
            .map(|col| col.memory_size() / input.num_rows())
            .sum();

        for (i, num_rows) in self.num_rows.iter().enumerate() {
            let input_memory_size: usize = memory_size_per_row * num_rows;
            let srf_memory_size: usize = self
                .srf_results
                .iter()
                .map(|srf_result| {
                    let (result, rows) = srf_result.get(i).unwrap();
                    if *rows > 0 {
                        result.memory_size(false)
                    } else {
                        0
                    }
                })
                .sum();

            // Check if adding this row would exceed either max_block_size or max_block_bytes
            if (result_rows + num_rows > self.max_block_size
                || total_memory_size + input_memory_size + srf_memory_size > self.max_block_bytes)
                && used > 0
            {
                break;
            }

            used += 1;
            result_rows += num_rows;
            total_memory_size += input_memory_size + srf_memory_size;
        }

        // TODO: if there is only one row can be used, we can use `Value::Scalar` directly.
        // Condition: `used == 1` and the rows of all the `srf_results` is equal to `max_nums_per_row[0]`.

        let mut result = DataBlock::empty_with_rows(result_rows);
        for column in input.columns() {
            let mut builder = ColumnBuilder::with_capacity(&column.data_type(), result_rows);
            for (i, max_nums) in self.num_rows.iter().take(used).enumerate() {
                if *max_nums > 0 {
                    let scalar_ref = unsafe { column.index_unchecked(i) };
                    builder.push_repeat(&scalar_ref, *max_nums);
                }
            }
            let column = builder.build();
            result.add_column(column);
        }

        for (srf_expr, srf_results) in self.srf_exprs.iter().zip(self.srf_results.iter_mut()) {
            match srf_expr.function.signature.name.as_str() {
                "json_path_query" | "json_array_elements" | "jq" => {
                    // The function return type:
                    // DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))])
                    let mut builder: NullableColumnBuilder<VariantType> =
                        NullableColumnBuilder::with_capacity(result_rows, &[]);

                    for (i, (row_result, repeat_times)) in srf_results.drain(0..used).enumerate() {
                        if let Value::Column(Column::Tuple(fields)) = row_result {
                            for (field_index, field) in fields.into_iter().enumerate() {
                                if field_index == 0 {
                                    push_variant_column(
                                        field,
                                        &mut builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    );
                                } else {
                                    unreachable!();
                                }
                            }
                        }
                    }

                    let column =
                        Column::Tuple(vec![Column::Nullable(Box::new(builder.build().upcast()))]);
                    result.add_column(column);
                }
                "json_each" => {
                    // The function return type:
                    // DataType::Tuple(vec![
                    //   DataType::Nullable(Box::new(DataType::String)),
                    //   DataType::Nullable(Box::new(DataType::Variant)),
                    // ]).
                    let mut key_builder =
                        NullableColumnBuilder::<StringType>::with_capacity(result_rows, &[]);
                    let mut value_builder =
                        NullableColumnBuilder::<VariantType>::with_capacity(result_rows, &[]);

                    for (i, (row_result, repeat_times)) in srf_results.drain(0..used).enumerate() {
                        if let Value::Column(Column::Tuple(fields)) = row_result {
                            for (field_index, field) in fields.into_iter().enumerate() {
                                if field_index == 0 {
                                    push_string_column(
                                        field,
                                        &mut key_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    );
                                } else {
                                    push_variant_column(
                                        field,
                                        &mut value_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    );
                                }
                            }
                        }
                    }

                    let column = Column::Tuple(vec![
                        Column::Nullable(Box::new(key_builder.build().upcast())),
                        Column::Nullable(Box::new(value_builder.build().upcast())),
                    ]);
                    result.add_column(column);
                }
                "flatten" => {
                    // The function return type:
                    // DataType::Tuple(vec![
                    //   DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                    //   DataType::Nullable(Box::new(DataType::String)),
                    //   DataType::Nullable(Box::new(DataType::String)),
                    //   DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                    //   DataType::Nullable(Box::new(DataType::Variant)),
                    //   DataType::Nullable(Box::new(DataType::Variant)),
                    // ]).
                    let mut seq_builder =
                        NullableColumnBuilder::<NumberType<u64>>::with_capacity(result_rows, &[]);
                    let mut key_builder =
                        NullableColumnBuilder::<StringType>::with_capacity(result_rows, &[]);
                    let mut path_builder =
                        NullableColumnBuilder::<StringType>::with_capacity(result_rows, &[]);
                    let mut index_builder =
                        NullableColumnBuilder::<NumberType<u64>>::with_capacity(result_rows, &[]);
                    let mut value_builder =
                        NullableColumnBuilder::<VariantType>::with_capacity(result_rows, &[]);
                    let mut this_builder =
                        NullableColumnBuilder::<VariantType>::with_capacity(result_rows, &[]);

                    for (i, (row_result, repeat_times)) in srf_results.drain(0..used).enumerate() {
                        if let Value::Column(Column::Tuple(fields)) = row_result {
                            debug_assert!(fields.len() == 6);
                            for (field_index, field) in fields.into_iter().enumerate() {
                                match field_index {
                                    0 => push_number_column(
                                        field,
                                        &mut seq_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    ),
                                    1 => push_string_column(
                                        field,
                                        &mut key_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    ),
                                    2 => push_string_column(
                                        field,
                                        &mut path_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    ),
                                    3 => push_number_column(
                                        field,
                                        &mut index_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    ),
                                    4 => push_variant_column(
                                        field,
                                        &mut value_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    ),
                                    5 => push_variant_column(
                                        field,
                                        &mut this_builder,
                                        self.num_rows[i],
                                        repeat_times,
                                    ),
                                    _ => unreachable!(),
                                }
                            }
                        }
                    }

                    let column = Column::Tuple(vec![
                        Column::Nullable(Box::new(seq_builder.build().upcast())),
                        Column::Nullable(Box::new(key_builder.build().upcast())),
                        Column::Nullable(Box::new(path_builder.build().upcast())),
                        Column::Nullable(Box::new(index_builder.build().upcast())),
                        Column::Nullable(Box::new(value_builder.build().upcast())),
                        Column::Nullable(Box::new(this_builder.build().upcast())),
                    ]);
                    result.add_column(column);
                }
                "unnest" | "regexp_split_to_table" => {
                    let mut result_data_blocks = Vec::with_capacity(used);
                    for (i, (mut row_result, repeat_times)) in
                        srf_results.drain(0..used).enumerate()
                    {
                        if let Value::Column(Column::Tuple(fields)) = &mut row_result {
                            // If the current result set has less rows than the max number of rows,
                            // we need to pad the result set with null values.
                            // TODO(leiysky): this can be optimized by using a `zip` array function
                            if repeat_times < self.num_rows[i] {
                                for field in fields {
                                    match field {
                                        Column::Null { .. } => {
                                            *field = ColumnBuilder::repeat(
                                                &ScalarRef::Null,
                                                self.num_rows[i],
                                                &DataType::Null,
                                            )
                                            .build();
                                        }
                                        Column::Nullable(box nullable_column) => {
                                            let mut column_builder =
                                                NullableColumnBuilder::from_column(
                                                    (*nullable_column).clone(),
                                                );
                                            (0..(self.num_rows[i] - repeat_times)).for_each(|_| {
                                                column_builder.push_null();
                                            });
                                            *field =
                                                Column::Nullable(Box::new(column_builder.build()));
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            }
                        } else {
                            let data_type = &srf_expr.return_type;
                            let inner_tys = data_type.as_tuple().unwrap();
                            let inner_vals = vec![ScalarRef::Null; inner_tys.len()];
                            row_result = Value::Column(
                                ColumnBuilder::repeat(
                                    &ScalarRef::Tuple(inner_vals),
                                    self.num_rows[i],
                                    data_type,
                                )
                                .build(),
                            );
                        }

                        let block_entry = row_result.into_column().unwrap().into();
                        result_data_blocks.push(DataBlock::new(vec![block_entry], self.num_rows[i]))
                    }
                    let data_block = DataBlock::concat(&result_data_blocks)?;
                    debug_assert!(data_block.num_rows() == result_rows);
                    let block_entry = BlockEntry::new(data_block.get_by_offset(0).value(), || {
                        (data_block.data_type(0), result_rows)
                    });
                    result.add_entry(block_entry);
                }
                _ => todo!(
                    "unsupported set-returning function: {}",
                    srf_expr.function.signature.name
                ),
            }
        }

        // Release consumed rows.
        self.num_rows.drain(0..used);
        // `self.srf_results` is already drained.
        let input = input.slice(used..input.num_rows());
        if input.num_rows() == 0 {
            debug_assert!(self.num_rows.is_empty());
            debug_assert!(self.srf_results.iter().all(|res| res.is_empty()));
            self.input = None;
        } else {
            self.input = Some(input);
        }

        Ok(Some(result))
    }
}

pub fn push_string_column(
    column: Column,
    builder: &mut NullableColumnBuilder<StringType>,
    num_rows: usize,
    repeat_times: usize,
) {
    if let Column::Nullable(box nullable_column) = column {
        if let Column::String(string_column) = nullable_column.column {
            let validity = nullable_column.validity;
            if validity.null_count() == 0 {
                for idx in 0..repeat_times {
                    builder.push(unsafe { string_column.index_unchecked(idx) });
                }
                builder.push_repeat_null(num_rows - repeat_times);
            } else if validity.null_count() == validity.len() {
                builder.push_repeat_null(num_rows);
            } else {
                for idx in 0..repeat_times {
                    if validity.get_bit(idx) {
                        builder.push(unsafe { string_column.index_unchecked(idx) });
                    } else {
                        builder.push_null();
                    }
                }
                builder.push_repeat_null(num_rows - repeat_times);
            }
        } else {
            unreachable!();
        }
    } else {
        unreachable!();
    }
}

fn push_variant_column(
    column: Column,
    builder: &mut NullableColumnBuilder<VariantType>,
    num_rows: usize,
    repeat_times: usize,
) {
    if let Column::Nullable(box nullable_column) = column {
        if let Column::Variant(variant_column) = nullable_column.column {
            let validity = nullable_column.validity;
            if validity.null_count() == 0 {
                for idx in 0..repeat_times {
                    builder.push(unsafe { variant_column.index_unchecked(idx) });
                }
                builder.push_repeat_null(num_rows - repeat_times);
            } else if validity.null_count() == validity.len() {
                builder.push_repeat_null(num_rows);
            } else {
                for idx in 0..repeat_times {
                    if validity.get_bit(idx) {
                        builder.push(unsafe { variant_column.index_unchecked(idx) });
                    } else {
                        builder.push_null();
                    }
                }
                builder.push_repeat_null(num_rows - repeat_times);
            }
        } else {
            unreachable!();
        }
    } else {
        unreachable!();
    }
}

fn push_number_column(
    column: Column,
    builder: &mut NullableColumnBuilder<NumberType<u64>>,
    num_rows: usize,
    repeat_times: usize,
) {
    if let Column::Nullable(box nullable_column) = column {
        if let Column::Number(NumberColumn::UInt64(number_column)) = nullable_column.column {
            let validity = nullable_column.validity;
            if validity.null_count() == 0 {
                for idx in 0..repeat_times {
                    builder.push(unsafe { *number_column.get_unchecked(idx) });
                }
                builder.push_repeat_null(num_rows - repeat_times);
            } else if validity.null_count() == validity.len() {
                builder.push_repeat_null(num_rows);
            } else {
                for idx in 0..repeat_times {
                    if validity.get_bit(idx) {
                        builder.push(unsafe { *number_column.get_unchecked(idx) });
                    } else {
                        builder.push_null();
                    }
                }
                builder.push_repeat_null(num_rows - repeat_times);
            }
        } else {
            unreachable!();
        }
    } else {
        unreachable!();
    }
}
