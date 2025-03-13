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

use std::cmp::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;

use crate::table_functions::TableArgs;

pub fn string_value(value: &Scalar) -> Result<String> {
    match value {
        Scalar::String(val) => Ok(val.clone()),
        _ => Err(ErrorCode::BadArguments("invalid string.")),
    }
}

pub fn bool_value(value: &Scalar) -> Result<bool> {
    match value {
        Scalar::Boolean(val) => Ok(*val),
        _ => Err(ErrorCode::BadArguments("invalid boolean.")),
    }
}

pub fn string_literal(val: &str) -> Scalar {
    Scalar::String(val.to_string())
}

pub fn bool_literal(val: bool) -> Scalar {
    Scalar::Boolean(val)
}

pub fn u64_literal(val: u64) -> Scalar {
    Scalar::Number(NumberScalar::UInt64(val))
}

pub fn cmp_with_null(v1: &Scalar, v2: &Scalar) -> Ordering {
    match (v1.is_null(), v2.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => v1.cmp(v2),
    }
}

pub fn parse_sequence_args(table_args: &TableArgs, func_name: &str) -> Result<String> {
    let args = table_args.expect_all_positioned(func_name, Some(1))?;
    let sequence = string_value(&args[0])?;
    Ok(sequence)
}

pub fn parse_db_tb_args(table_args: &TableArgs, func_name: &str) -> Result<(String, String)> {
    let args = table_args.expect_all_positioned(func_name, Some(2))?;
    let db = string_value(&args[0])?;
    let tbl = string_value(&args[1])?;
    Ok((db, tbl))
}

pub fn parse_db_tb_opt_args(
    table_args: &TableArgs,
    func_name: &str,
) -> Result<(String, String, Option<String>)> {
    let args = table_args.expect_all_positioned(func_name, None)?;
    match args.len() {
        3 => {
            let arg1 = string_value(&args[0])?;
            let arg2 = string_value(&args[1])?;
            let arg3 = string_value(&args[2])?;
            Ok((arg1, arg2, Some(arg3)))
        }
        2 => {
            let arg1 = string_value(&args[0])?;
            let arg2 = string_value(&args[1])?;
            Ok((arg1, arg2, None))
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "expecting <database>, <table_name> and <opt_arg> (as string literals), but got {:?}",
            args
        ))),
    }
}

pub fn parse_opt_opt_args(
    table_args: &TableArgs,
    func_name: &str,
) -> Result<(Option<String>, Option<String>)> {
    let args = table_args.expect_all_positioned(func_name, None)?;
    match args.len() {
        2 => {
            let arg1 = string_value(&args[0])?;
            let arg2 = string_value(&args[1])?;
            Ok((Some(arg1), Some(arg2)))
        }
        1 => {
            let arg1 = string_value(&args[0])?;
            Ok((Some(arg1), None))
        }
        0 => Ok((None, None)),
        _ => Err(ErrorCode::BadArguments(format!(
            "expecting <opt_arg1> and <opt_arg2> (as string literals), but got {:?}",
            args
        ))),
    }
}

pub fn parse_db_tb_col_args(table_args: &TableArgs, func_name: &str) -> Result<String> {
    let args = table_args.expect_all_positioned(func_name, Some(1))?;
    let db = string_value(&args[0])?;
    Ok(db)
}
