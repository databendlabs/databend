// // Copyright 2020-2021 The Datafuse Authors.
// //
// // SPDX-License-Identifier: Apache-2.0.
//
// use std::fmt;
// use std::ops::Deref;
//
// use common_arrow::arrow::compute;
// use common_datablocks::DataBlock;
// use common_datavalues::{DataColumnarValue, DataArrayHash};
// use common_datavalues::DataSchema;
// use common_datavalues::DataType;
// use common_datavalues::DataValue;
// use common_exception::ErrorCodes;
// use common_exception::Result;
//
// use crate::IFunction;
// use std::collections::hash_map::DefaultHasher;
//
// #[derive(Clone)]
// pub struct SipHashFunction {
//     display_name: String,
//     input: Box<dyn IFunction>,
// }
//
// impl SipHashFunction {
//     pub fn try_create(display_name: &str, args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
//         match args.len() {
//             1 => Ok(Box::new(SipHashFunction {
//                 display_name: display_name.to_string(),
//                 input: args[0].clone(),
//             })),
//             _ => Result::Err(ErrorCodes::BadArguments(
//                 "Function Error: sipHash function args length must be 1"
//             ))
//         }
//     }
// }
//
// impl IFunction for SipHashFunction {
//     fn name(&self) -> &str {
//         "sipHash"
//     }
//
//     fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
//         match self.input.return_type(input_schema)? {
//             DataType::Utf8 => Ok(DataType::UInt64),
//             DataType::LargeUtf8 => Ok(DataType::UInt64),
//             DataType::Binary => Ok(DataType::UInt64),
//             DataType::LargeBinary => Ok(DataType::UInt64),
//             _ => Result::Err(ErrorCodes::BadArguments("Function Error: sipHash function argument must be String"))
//         }
//     }
//
//     fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
//         self.input.nullable(input_schema)
//     }
//
//     fn eval(&self, block: &DataBlock) -> Result<DataColumnarValue> {
//         let value = self.input.eval(block)?;
//         DataArrayHash::<DefaultHasher>::data_array_hash(&value)
//     }
// }
//
// impl fmt::Display for SipHashFunction {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "sipHash({})", self.input)
//     }
// }
