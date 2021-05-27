// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::fmt;

use common_datavalues::DataArrayHash;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct SipHashFunction {
    display_name: String
}

impl SipHashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(SipHashFunction {
            display_name: display_name.to_string()
        }))
    }
}

impl IFunction for SipHashFunction {
    fn name(&self) -> &str {
        "siphash"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != 1 {
            return Result::Err(ErrorCodes::BadArguments(
                "Function Error: sipHash function args length must be 1"
            ));
        }

        match args[0] {
            DataType::Utf8 => Ok(DataType::UInt64),
            DataType::LargeUtf8 => Ok(DataType::UInt64),
            DataType::Binary => Ok(DataType::UInt64),
            DataType::LargeBinary => Ok(DataType::UInt64),
            _ => Result::Err(ErrorCodes::BadArguments(
                "Function Error: sipHash function argument must be String or Binary"
            ))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        DataArrayHash::<DefaultHasher>::data_array_hash(&columns[0])
    }
}

impl fmt::Display for SipHashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "siphash")
    }
}
