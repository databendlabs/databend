// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[macro_export]
macro_rules! apply_integer_creator {
    ($data_type: ident, $creator: ident, $creator_fn: ident, $display_name: ident, $arguments: ident) => {{
        match $data_type {
            DataType::UInt8 => $creator::<u8>::$creator_fn($display_name, $arguments),
            DataType::UInt16 => $creator::<u16>::$creator_fn($display_name, $arguments),
            DataType::UInt32 => $creator::<u32>::$creator_fn($display_name, $arguments),
            DataType::UInt64 => $creator::<u64>::$creator_fn($display_name, $arguments),
            DataType::Int8 => $creator::<i8>::$creator_fn($display_name, $arguments),
            DataType::Int16 => $creator::<i16>::$creator_fn($display_name, $arguments),
            DataType::Int32 => $creator::<i32>::$creator_fn($display_name, $arguments),
            DataType::Int64 => $creator::<i64>::$creator_fn($display_name, $arguments),

            other => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                stringify!($creator),
                other
            ))),
        }
    }};
}

#[macro_export]
macro_rules! apply_numeric_creator {
    ($data_type: ident, $creator: ident, $creator_fn: ident, $display_name: ident, $arguments: ident) => {{
        match $data_type {
            DataType::UInt8 => $creator::<u8>::$creator_fn($display_name, $arguments),
            DataType::UInt16 => $creator::<u16>::$creator_fn($display_name, $arguments),
            DataType::UInt32 => $creator::<u32>::$creator_fn($display_name, $arguments),
            DataType::UInt64 => $creator::<u64>::$creator_fn($display_name, $arguments),
            DataType::Int8 => $creator::<i8>::$creator_fn($display_name, $arguments),
            DataType::Int16 => $creator::<i16>::$creator_fn($display_name, $arguments),
            DataType::Int32 => $creator::<i32>::$creator_fn($display_name, $arguments),
            DataType::Int64 => $creator::<i64>::$creator_fn($display_name, $arguments),
            DataType::Float32 => $creator::<f32>::$creator_fn($display_name, $arguments),
            DataType::Float64 => $creator::<f64>::$creator_fn($display_name, $arguments),

            other => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                stringify!($creator),
                other
            ))),
        }
    }};
}

#[macro_export]
macro_rules! apply_string_creator {
    ($data_type: ident, $creator: ident, $creator_fn: ident, $display_name: ident, $arguments: ident) => {{
        match $data_type {
            DataType::Utf8 => $creator::<String>::$creator_fn($display_name, $arguments),
            other => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                stringify!($creator),
                other
            ))),
        }
    }};
}

#[macro_export]
macro_rules! apply_numeric_creator_with_largest_type {
    ($data_type: ident, $creator: ident, $creator_fn: ident,  $display_name: ident, $arguments: ident) => {{
        match $data_type {
            DataType::UInt8 => $creator::<u8, u64>::$creator_fn($display_name, $arguments),
            DataType::UInt16 => $creator::<u16, u64>::$creator_fn($display_name, $arguments),
            DataType::UInt32 => $creator::<u32, u64>::$creator_fn($display_name, $arguments),
            DataType::UInt64 => $creator::<u64, u64>::$creator_fn($display_name, $arguments),
            DataType::Int8 => $creator::<i8, i64>::$creator_fn($display_name, $arguments),
            DataType::Int16 => $creator::<i16, i64>::$creator_fn($display_name, $arguments),
            DataType::Int32 => $creator::<i32, i64>::$creator_fn($display_name, $arguments),
            DataType::Int64 => $creator::<i64, i64>::$creator_fn($display_name, $arguments),
            DataType::Float32 => $creator::<f32, f64>::$creator_fn($display_name, $arguments),
            DataType::Float64 => $creator::<f64, f64>::$creator_fn($display_name, $arguments),

            other => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                stringify!($creator),
                other
            ))),
        }
    }};
}
