// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use arrow::datatypes;

pub type DataType = datatypes::DataType;

fn is_numeric(dt: &DataType) -> bool {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        _ => false,
    }
}

pub fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Float64, _) => Some(Float64),
        (_, Float64) => Some(Float64),

        (_, Float32) => Some(Float32),
        (Float32, _) => Some(Float32),

        (Int64, _) => Some(Int64),
        (_, Int64) => Some(Int64),

        (Int32, _) => Some(Int32),
        (_, Int32) => Some(Int32),

        (Int16, _) => Some(Int16),
        (_, Int16) => Some(Int16),

        (Int8, _) => Some(Int8),
        (_, Int8) => Some(Int8),

        (UInt64, _) => Some(UInt64),
        (_, UInt64) => Some(UInt64),

        (UInt32, _) => Some(UInt32),
        (_, UInt32) => Some(UInt32),

        (UInt16, _) => Some(UInt16),
        (_, UInt16) => Some(UInt16),

        (UInt8, _) => Some(UInt8),
        (_, UInt8) => Some(UInt8),

        _ => None,
    }
}
