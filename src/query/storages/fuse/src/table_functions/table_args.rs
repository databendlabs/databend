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
use databend_common_expression::Expr;
use databend_common_expression::Scalar;

use crate::table_functions::TableArgs;

pub fn string_value(value: &Scalar) -> Result<String> {
    match value {
        Scalar::String(val) => Ok(val.clone()),
        _ => Err(ErrorCode::BadArguments("invalid string.")),
    }
}

pub fn string_literal(val: &str) -> Scalar {
    Scalar::String(val.to_string())
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

pub fn parse_db_tb_ssid_args(
    table_args: &TableArgs,
    func_name: &str,
) -> Result<(String, String, Option<String>)> {
    let args = table_args.expect_all_positioned(func_name, None)?;
    match args.len() {
        3 => {
            let db = string_value(&args[0])?;
            let tbl = string_value(&args[1])?;
            let snapshot_id = string_value(&args[2])?;
            Ok((db, tbl, Some(snapshot_id)))
        }
        2 => {
            let db = string_value(&args[0])?;
            let tbl = string_value(&args[1])?;
            Ok((db, tbl, None))
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "expecting <database>, <table_name> and <snapshot_id> (as string literals), but got {:?}",
            args
        ))),
    }
}

pub fn parse_db_tb_col_args(table_args: &TableArgs, func_name: &str) -> Result<String> {
    let args = table_args.expect_all_positioned(func_name, Some(1))?;
    let db = string_value(&args[0])?;
    Ok(db)
}

pub fn unwrap_tuple(expr: &Expr) -> Option<Vec<Expr>> {
    match expr {
        Expr::FunctionCall { function, args, .. } if function.signature.name == "tuple" => {
            Some(args.clone())
        }
        _ => None,
    }
}
