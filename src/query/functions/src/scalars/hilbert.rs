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

use databend_common_expression::Column;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::hilbert_index;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::ValueType;
use databend_common_expression::vectorize_with_builder_2_arg;

/// Registers Hilbert curve related functions with the function registry.
pub fn register(registry: &mut FunctionRegistry) {
    // Register the hilbert_range_index function that calculates Hilbert indices for multi-dimensional data
    let factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        let args_num = args_type.len();
        // The function supports 2, 3, 4, or 5 dimensions (each dimension requires 2 arguments)
        if ![4, 6, 8, 10].contains(&args_num) {
            return None;
        }

        // Create the function signature with appropriate argument types
        // For each dimension, we need:
        // 1. A value (the point coordinate in that dimension)
        // 2. An array of boundaries (for partitioning that dimension)
        let sig_args_type = (0..args_num / 2)
            .flat_map(|idx| {
                [
                    DataType::Nullable(Box::new(DataType::Generic(idx))),
                    DataType::Nullable(Box::new(DataType::Array(Box::new(DataType::Generic(idx))))),
                ]
            })
            .collect();

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "hilbert_range_index".to_string(),
                args_type: sig_args_type,
                return_type: DataType::Binary,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(FunctionDomain::Full),
                eval: scalar_evaluator(move |args, ctx| {
                    // Determine if we're processing scalar values or columns
                    let input_all_scalars = args.iter().all(|arg| arg.as_scalar().is_some());
                    let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };
                    let mut builder = BinaryType::create_builder(process_rows, &[]);

                    for index in 0..process_rows {
                        let mut points = Vec::with_capacity(args_num / 2);

                        // Process each dimension (each dimension has a value and boundaries array)
                        for i in (0..args_num).step_by(2) {
                            let arg1 = &args[i]; // The value in this dimension
                            let arg2 = &args[i + 1]; // The boundaries array for this dimension

                            // Get the value and boundaries for this row
                            let val = unsafe { arg1.index_unchecked(index) };
                            let arr = unsafe { arg2.index_unchecked(index) };

                            // Calculate the partition ID for this dimension (capped at 65535, i.e. 2 bytes or 16 bits)
                            // This effectively discretizes the continuous dimension into buckets
                            let id = arr
                                .as_array()
                                .map(|arr| calc_range_partition_id(val, arr).min(65535) as u16)
                                .unwrap_or(0);

                            // Encode the partition ID as bytes
                            let key = id.encode();
                            points.push(key);
                        }

                        // Convert the multi-dimensional point to a Hilbert index
                        // This maps the n-dimensional point to a 1-dimensional value
                        let points = points
                            .iter()
                            .map(|array| array.as_slice())
                            .collect::<Vec<_>>();
                        let slice = hilbert_index(&points, 2);

                        // Store the Hilbert index in the result
                        builder.put_slice(&slice);
                        builder.commit_row();
                    }

                    // Return the appropriate result type based on input
                    if input_all_scalars {
                        Value::Scalar(BinaryType::upcast_scalar(BinaryType::build_scalar(builder)))
                    } else {
                        Value::Column(BinaryType::upcast_column(BinaryType::build_column(builder)))
                    }
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("hilbert_range_index", factory);

    // This `range_partition_id(col, range_bounds)` function calculates the partition ID for each value
    // in the column based on the specified partition boundaries.
    // The column values are conceptually divided into multiple partitions defined by the range_bounds.
    // For example, given the column values (0, 1, 3, 6, 8) and a partition configuration with 3 partitions,
    // the range_bounds might be [1, 6]. The function would then return partition IDs as (0, 0, 1, 1, 2).
    registry
        .register_2_arg_core::<GenericType<0>, ArrayType<GenericType<0>>, NumberType<u64>, _, _>(
            "range_partition_id",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<
                GenericType<0>,
                ArrayType<GenericType<0>>,
                NumberType<u64>,
            >(|val, arr, builder, _| {
                let id = calc_range_partition_id(val, &arr);
                builder.push(id);
            }),
        );

    registry.register_2_arg_core::<NullableType<GenericType<0>>, NullableType<ArrayType<GenericType<0>>>, NumberType<u64>, _, _>(
        "range_partition_id",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<NullableType<GenericType<0>>, NullableType<ArrayType<GenericType<0>>>, NumberType<u64>>(|val, arr, builder, _| {
            let id = match (val, arr) {
                (Some(val), Some(arr)) => calc_range_partition_id(val, &arr),
                (None, Some(arr)) => arr.len() as u64,
                _ => 0,
            };
            builder.push(id);
        }),
    );
}

/// Calculates the partition ID for a value based on range boundaries.
///
/// # Arguments
/// * `val` - The value to find the partition for
/// * `arr` - The array of boundary values that define the partitions
///
/// # Returns
/// * The partition ID as a u64 (0 to arr.len())
///
/// # Example
/// For boundaries [10, 20, 30]:
/// - Values < 10 get partition ID 0
/// - Values >= 10 and < 20 get partition ID 1
/// - Values >= 20 and < 30 get partition ID 2
/// - Values >= 30 get partition ID 3
fn calc_range_partition_id(val: ScalarRef, arr: &Column) -> u64 {
    let mut low = 0;
    let mut high = arr.len();
    while low < high {
        let mid = low + ((high - low) / 2);
        let bound = unsafe { arr.index_unchecked(mid) };
        if val > bound {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low as u64
}
