// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use arrow::datatypes::TimeUnit;
use common_datavalues::DataType;
use sqlparser::ast::DataType as SQLDataType;

use crate::error::{FuseQueryError, FuseQueryResult};

/// Maps the SQL type to the corresponding Arrow `DataType`
pub fn make_data_type(sql_type: &SQLDataType) -> FuseQueryResult<DataType> {
    match sql_type {
        SQLDataType::BigInt => Ok(DataType::Int64),
        SQLDataType::Int => Ok(DataType::Int32),
        SQLDataType::SmallInt => Ok(DataType::Int16),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => Ok(DataType::Utf8),
        SQLDataType::Decimal(_, _) => Ok(DataType::Float64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real | SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
        SQLDataType::Timestamp => Ok(DataType::Date64),

        _ => Err(FuseQueryError::build_internal_error(format!(
            "The SQL data type {:?} is not implemented",
            sql_type
        ))),
    }
}
