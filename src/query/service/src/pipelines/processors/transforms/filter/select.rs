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

use common_expression::types::nullable::NullableColumn;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::Value;

use crate::pipelines::processors::transforms::filter::select_boolean_column_adapt;
use crate::pipelines::processors::transforms::filter::select_boolean_scalar_adapt;
use crate::pipelines::processors::transforms::filter::select_columns;
use crate::pipelines::processors::transforms::filter::select_scalar_and_column;
use crate::pipelines::processors::transforms::filter::select_scalars;
use crate::pipelines::processors::transforms::filter::SelectOp;
use crate::pipelines::processors::transforms::filter::SelectStrategy;
use crate::pipelines::processors::transforms::filter::Selector;

impl<'a> Selector<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn select_values(
        &self,
        op: SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
        left_data_type: DataType,
        right_data_type: DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        match (left, right) {
            (Value::Scalar(left), Value::Scalar(right)) => select_scalars(
                op,
                left,
                right,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            ),
            (Value::Column(left), Value::Column(right)) => select_columns(
                op,
                left,
                right,
                left_data_type,
                right_data_type,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            ),
            (Value::Scalar(scalar), Value::Column(column)) => select_scalar_and_column(
                op,
                scalar,
                column,
                right_data_type,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            ),
            (Value::Column(column), Value::Scalar(scalar)) => select_scalar_and_column(
                op.reverse(),
                scalar,
                column,
                left_data_type,
                true_selection,
                false_selection,
                true_idx,
                false_idx,
                select_strategy,
                count,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn select_value(
        &self,
        value: Value<AnyType>,
        data_type: &DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        debug_assert!(
            matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean))
        );

        match data_type {
            DataType::Boolean => {
                let value = value.try_downcast::<BooleanType>().unwrap();
                match value {
                    Value::Scalar(scalar) => select_boolean_scalar_adapt(
                        scalar,
                        true_selection,
                        false_selection,
                        true_idx,
                        false_idx,
                        select_strategy,
                        count,
                    ),
                    Value::Column(column) => select_boolean_column_adapt(
                        column,
                        true_selection,
                        false_selection,
                        true_idx,
                        false_idx,
                        select_strategy,
                        count,
                    ),
                }
            }
            DataType::Nullable(box DataType::Boolean) => {
                let nullable_value = value.try_downcast::<NullableType<BooleanType>>().unwrap();
                match nullable_value {
                    Value::Scalar(None) => select_boolean_scalar_adapt(
                        false,
                        true_selection,
                        false_selection,
                        true_idx,
                        false_idx,
                        select_strategy,
                        count,
                    ),
                    Value::Scalar(Some(scalar)) => select_boolean_scalar_adapt(
                        scalar,
                        true_selection,
                        false_selection,
                        true_idx,
                        false_idx,
                        select_strategy,
                        count,
                    ),
                    Value::Column(NullableColumn { column, validity }) => {
                        let bitmap = &column & &validity;
                        select_boolean_column_adapt(
                            bitmap,
                            true_selection,
                            false_selection,
                            true_idx,
                            false_idx,
                            select_strategy,
                            count,
                        )
                    }
                }
            }
            _ => unreachable!("update_selection_by_boolean_value: {:?}", data_type),
        }
    }
}

fn _get_some_test_data_types() -> Vec<DataType> {
    vec![
        // DataType::Null,
        // DataType::EmptyArray,
        // DataType::EmptyMap,
        // DataType::Boolean,
        // DataType::String,
        // DataType::Bitmap,
        // DataType::Variant,
        // DataType::Timestamp,
        // DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        // DataType::Number(NumberDataType::UInt16),
        // DataType::Number(NumberDataType::UInt32),
        // DataType::Number(NumberDataType::UInt64),
        // DataType::Number(NumberDataType::Int8),
        // DataType::Number(NumberDataType::Int16),
        // DataType::Number(NumberDataType::Int32),
        // DataType::Number(NumberDataType::Int64),
        // DataType::Number(NumberDataType::Float32),
        // DataType::Number(NumberDataType::Float64),
        // DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
        //     precision: 10,
        //     scale: 2,
        // })),
        // DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
        //     precision: 35,
        //     scale: 3,
        // })),
        // DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        // DataType::Nullable(Box::new(DataType::String)),
        // DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        // DataType::Map(Box::new(DataType::Tuple(vec![
        //     DataType::Number(NumberDataType::UInt64),
        //     DataType::String,
        // ]))),
    ]
}

// fn _rand_block_for_some_types(num_rows: usize) -> DataBlock {
//     let types = _get_some_test_data_types();
//     let mut columns = Vec::with_capacity(types.len());
//     for data_type in types.iter() {
//         columns.push(Column::random(data_type, num_rows));
//     }

//     let block = DataBlock::new_from_columns(columns);
//     block.check_valid().unwrap();

//     block
// }

// #[test]
// pub fn test_operation() -> common_exception::Result<()> {
//     use common_expression::types::NumberScalar;
//     use common_expression::Scalar;
//     use rand::Rng;

//     let mut rng = rand::thread_rng();
//     let num_blocks = rng.gen_range(5..30);

//     for _ in 0..num_blocks {
//         let len = rng.gen_range(2..30);
//         let slice_start = rng.gen_range(0..len - 1);
//         let slice_end = rng.gen_range(slice_start..len);

//         let random_block_1 = _rand_block_for_some_types(len);
//         let random_block_2 = _rand_block_for_some_types(len);

//         let random_block_1 = random_block_1.slice(slice_start..slice_end);
//         let random_block_2 = random_block_2.slice(slice_start..slice_end);

//         let len = slice_end - slice_start;

//         let mut selection = vec![0u32; len];
//         let mut false_selection = vec![0u32; len];
//         let mut count = len;

//         for (left, right) in random_block_1
//             .columns()
//             .iter()
//             .zip(random_block_2.columns().iter())
//         {
//             let op = SelectOp::Gt;
//             let number_scalar = Scalar::Number(NumberScalar::UInt8(100));
//             let _right_scalar = Value::<AnyType>::Scalar(number_scalar);
//             let mut true_idx = 0;
//             let mut false_idx = 0;
//             count = select_values(
//                 op,
//                 left.value.clone(),
//                 // right_scalar,
//                 right.value.clone(),
//                 left.data_type.clone(),
//                 right.data_type.clone(),
//                 &mut selection,
//                 (&mut false_selection, false),
//                 &mut true_idx,
//                 &mut false_idx,
//                 SelectStrategy::All,
//                 count,
//             );
//         }
//         println!("count = {:?}, len = {:?}", count, len);

//         if count > 0 {
//             let count = count + ((count < len) as usize);
//             let left = random_block_1.take(&selection[0..count], &mut None)?;
//             let right = random_block_2.take(&selection[0..count], &mut None)?;
//             println!("left = \n{:?}\nright = \n{:?}\n", left, right);
//         }
//     }

//     Ok(())
// }
