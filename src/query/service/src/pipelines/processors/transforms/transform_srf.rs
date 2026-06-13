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

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::processors::BlockingTransform;
use databend_common_pipeline_transforms::processors::BlockingTransformer;

/// Expand the input [`DataBlock`] with set-returning functions.
pub struct TransformSRF {
    input: Option<DataBlock>,
    projections: BTreeSet<usize>,
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
        projections: BTreeSet<usize>,
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
            let column = materialize_srf_tuple_column(
                &srf_expr.return_type,
                result_rows,
                used,
                srf_results,
                &self.num_rows,
            )?;
            result.add_column(column);
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

fn materialize_srf_tuple_column(
    return_type: &DataType,
    result_rows: usize,
    used: usize,
    srf_results: &mut VecDeque<(Value<AnyType>, usize)>,
    num_rows: &VecDeque<usize>,
) -> Result<Column> {
    let fields = return_type.as_tuple().ok_or_else(|| {
        ErrorCode::Internal(format!(
            "SRF expects a tuple return type, got {return_type}"
        ))
    })?;
    let null_tuple = ScalarRef::Tuple(vec![ScalarRef::Null; fields.len()]);
    let mut builder = ColumnBuilder::with_capacity(return_type, result_rows);

    for (i, (row_result, repeat_times)) in srf_results.drain(0..used).enumerate() {
        let output_rows = num_rows[i];
        if repeat_times > output_rows {
            return Err(ErrorCode::Internal(format!(
                "SRF result rows {repeat_times} exceed output rows {output_rows}"
            )));
        }

        if repeat_times > 0 {
            match row_result {
                Value::Column(column) => {
                    let column = normalize_srf_tuple_column(column, return_type, repeat_times)?;
                    builder.append_column(&column);
                }
                Value::Scalar(scalar) => match scalar.as_ref() {
                    ScalarRef::Tuple(_) => {
                        builder.push_repeat(&scalar.as_ref(), repeat_times);
                    }
                    ScalarRef::Null => {
                        push_repeat_null_tuple(
                            &mut builder,
                            &null_tuple,
                            return_type,
                            repeat_times,
                        )?;
                    }
                    scalar => {
                        return Err(ErrorCode::Internal(format!(
                            "SRF expects a tuple scalar result, got {scalar:?}"
                        )));
                    }
                },
            }
        }

        if output_rows > repeat_times {
            push_repeat_null_tuple(
                &mut builder,
                &null_tuple,
                return_type,
                output_rows - repeat_times,
            )?;
        }
    }

    Ok(builder.build())
}

fn push_repeat_null_tuple(
    builder: &mut ColumnBuilder,
    null_tuple: &ScalarRef,
    return_type: &DataType,
    repeat: usize,
) -> Result<()> {
    let fields = return_type.as_tuple().ok_or_else(|| {
        ErrorCode::Internal(format!(
            "SRF expects a tuple return type, got {return_type}"
        ))
    })?;

    if fields
        .iter()
        .any(|field| !matches!(field, DataType::Null | DataType::Nullable(_)))
    {
        return Err(ErrorCode::Internal(format!(
            "SRF cannot pad NULLs for non-nullable tuple return type {return_type}"
        )));
    }

    builder.push_repeat(null_tuple, repeat);
    Ok(())
}

fn normalize_srf_tuple_column(
    column: Column,
    return_type: &DataType,
    repeat_times: usize,
) -> Result<Column> {
    let return_fields = return_type.as_tuple().ok_or_else(|| {
        ErrorCode::Internal(format!(
            "SRF expects a tuple return type, got {return_type}"
        ))
    })?;

    let fields = match column {
        Column::Tuple(fields) => fields,
        column if return_fields.len() == 1 => vec![column],
        other => {
            return Err(ErrorCode::Internal(format!(
                "SRF expects a tuple column result, got {}",
                other.data_type()
            )));
        }
    };

    if fields.len() != return_fields.len() {
        return Err(ErrorCode::Internal(format!(
            "SRF tuple field count mismatch, expected {}, got {}",
            return_fields.len(),
            fields.len()
        )));
    }

    let fields = fields
        .into_iter()
        .zip(return_fields)
        .map(|(field, return_type)| normalize_srf_field_column(field, return_type, repeat_times))
        .collect::<Result<Vec<_>>>()?;

    Ok(Column::Tuple(fields))
}

fn normalize_srf_field_column(
    column: Column,
    return_type: &DataType,
    repeat_times: usize,
) -> Result<Column> {
    if column.len() != repeat_times {
        return Err(ErrorCode::Internal(format!(
            "SRF field row count mismatch, expected {repeat_times}, got {}",
            column.len()
        )));
    }

    match return_type {
        DataType::Nullable(inner_type) => match column {
            Column::Null { len } => Ok(ColumnBuilder::repeat(
                &ScalarRef::Null,
                len,
                &DataType::Nullable(inner_type.clone()),
            )
            .build()),
            column if column.data_type() == **inner_type => Ok(column.wrap_nullable(None)),
            column if column.data_type() == *return_type => Ok(column),
            other => Err(ErrorCode::Internal(format!(
                "SRF field type mismatch, expected {return_type}, got {}",
                other.data_type()
            ))),
        },
        DataType::Null => match column {
            Column::Null { .. } => Ok(column),
            other => Err(ErrorCode::Internal(format!(
                "SRF field type mismatch, expected {return_type}, got {}",
                other.data_type()
            ))),
        },
        _ if column.data_type() == *return_type => Ok(column),
        _ if repeat_times == 0 => Ok(ColumnBuilder::with_capacity(return_type, 0).build()),
        other_type => Err(ErrorCode::Internal(format!(
            "SRF field type mismatch, expected {other_type}, got {}",
            column.data_type()
        ))),
    }
}
