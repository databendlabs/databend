// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::fmt;

use common_datavalues::{DataArrayHashDispatcher, FuseDataHasher};
use common_datavalues::DataColumnarValue;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IFunction;
use std::hash::Hasher;

#[derive(Clone)]
pub struct SipHashFunction {
    display_name: String,
}

struct SipHasher;

impl SipHashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn IFunction>> {
        Ok(Box::new(SipHashFunction {
            display_name: display_name.to_string(),
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
                "Function Error: sipHash function args length must be 1",
            ));
        }

        match args[0] {
            DataType::Int8 => Ok(DataType::UInt64),
            DataType::Int16 => Ok(DataType::UInt64),
            DataType::Int32 => Ok(DataType::UInt64),
            DataType::Int64 => Ok(DataType::UInt64),
            DataType::UInt8 => Ok(DataType::UInt64),
            DataType::UInt16 => Ok(DataType::UInt64),
            DataType::UInt32 => Ok(DataType::UInt64),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Float32 => Ok(DataType::UInt64),
            DataType::Float64 => Ok(DataType::UInt64),
            DataType::Date32 => Ok(DataType::UInt64),
            DataType::Date64 => Ok(DataType::UInt64),
            DataType::Time32(_) => Ok(DataType::UInt64),
            DataType::Time64(_) => Ok(DataType::UInt64),
            DataType::Duration(_) => Ok(DataType::UInt64),
            DataType::Interval(_) => Ok(DataType::UInt64),
            DataType::Timestamp(_, _) => Ok(DataType::UInt64),
            DataType::Utf8 => Ok(DataType::UInt64),
            DataType::LargeUtf8 => Ok(DataType::UInt64),
            DataType::Binary => Ok(DataType::UInt64),
            DataType::LargeBinary => Ok(DataType::UInt64),
            _ => Result::Err(ErrorCodes::BadArguments(format!(
                "Function Error: Siphash does not support {} type parameters",
                args[0]
            ))),
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumnarValue], _input_rows: usize) -> Result<DataColumnarValue> {
        DataArrayHashDispatcher::<SipHasher>::dispatch(&columns[0])
    }
}

impl FuseDataHasher for SipHasher {
    fn hash_bool(v: &bool) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u8(*v as u8);
        hasher.finish()
    }

    fn hash_i8(v: &i8) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_i8(*v);
        hasher.finish()
    }

    fn hash_i16(v: &i16) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_i16(*v);
        hasher.finish()
    }

    fn hash_i32(v: &i32) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_i32(*v);
        hasher.finish()
    }

    fn hash_i64(v: &i64) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_i64(*v);
        hasher.finish()
    }

    fn hash_u8(v: &u8) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u8(*v);
        hasher.finish()
    }

    fn hash_u16(v: &u16) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u16(*v);
        hasher.finish()
    }

    fn hash_u32(v: &u32) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u32(*v);
        hasher.finish()
    }

    fn hash_u64(v: &u64) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u64(*v);
        hasher.finish()
    }

    fn hash_f32(v: &f32) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u32(*v as u32);
        hasher.finish()
    }

    fn hash_f64(v: &f64) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write_u64(*v as u64);
        hasher.finish()
    }

    fn hash_bytes(bytes: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::default();
        hasher.write(bytes);
        hasher.finish()
    }
}

impl fmt::Display for SipHashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "siphash")
    }
}
