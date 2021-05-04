// // Copyright 2020-2021 The Datafuse Authors.
// //
// // SPDX-License-Identifier: Apache-2.0.
//
//
// use crate::{DataColumnarValue, DataArrayRef};
// use common_arrow::arrow::datatypes::DataType;
// use common_arrow::arrow::array::BooleanArray;
// use common_arrow::arrow::buffer::MutableBuffer;
//
// pub struct DataColumnarSplit;
//
// impl DataColumnarSplit {
//     #[inline]
//     pub fn data_columnar_split(
//         data: &DataColumnarValue,
//         indices: &DataColumnarValue,
//         nums: usize) -> Result<Vec<DataArrayRef>> {
//         /*match (data, indices) {
//             (DataColumnarValue::Array(data), DataColumnarValue::Array(right_array)) => {
//                 (data.clone(), right_array.clone())
//             }
//             (DataColumnarValue::Array(array), DataColumnarValue::Scalar(scalar)) => {
//                 (array.clone(), scalar.to_array_with_size(array.len())?)
//             }
//             (DataColumnarValue::Scalar(scalar), DataColumnarValue::Array(array)) => {
//                 (scalar.to_array_with_size(array.len())?, array.clone())
//             }
//             (DataColumnarValue::Scalar(left_scalar), DataColumnarValue::Scalar(right_scalar)) => (
//                 left_scalar.to_array_with_size(1)?,
//                 right_scalar.to_array_with_size(1)?
//             )
//         }*/
//     }
//
//     fn data_array_split(data: &DataArrayRef, indices: &DataArrayRef) -> Result<Vec<DataArrayRef>> {
//         // data.data().value
//
//         match data.data_type() {
//             DataType::Boolean => {
//                 let array = data.as_any().downcast_ref::<BooleanArray>().unwrap();
//                 for value in array.values() {
//
//                 }
//             }
//         };
//     }
// }
