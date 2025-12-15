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

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;
use log::debug;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TableArgs {
    pub positioned: Vec<Scalar>,
    pub named: HashMap<String, Scalar>,
}

impl TableArgs {
    pub fn is_empty(&self) -> bool {
        self.named.is_empty() && self.positioned.is_empty()
    }

    pub fn new_positioned(args: Vec<Scalar>) -> Self {
        Self {
            positioned: args,
            named: HashMap::new(),
        }
    }

    pub fn new_named(args: HashMap<String, Scalar>) -> Self {
        Self {
            positioned: vec![],
            named: args,
        }
    }

    /// Check TableArgs only contain positioned args.
    /// Also check num of positioned if num is not None.
    ///
    /// Returns the vec of positioned args.
    pub fn expect_all_positioned(
        &self,
        func_name: &str,
        num: Option<usize>,
    ) -> Result<Vec<Scalar>> {
        if !self.named.is_empty() {
            Err(ErrorCode::BadArguments(format!(
                "{} accept positioned args only",
                func_name
            )))
        } else if let Some(n) = num {
            if n != self.positioned.len() {
                Err(ErrorCode::BadArguments(format!(
                    "{} must accept exactly {} positioned args",
                    func_name, n
                )))
            } else {
                Ok(self.positioned.clone())
            }
        } else {
            Ok(self.positioned.clone())
        }
    }

    pub fn expect_all_strings(args: Vec<Scalar>) -> Result<Vec<String>> {
        args.into_iter()
            .map(|arg| {
                let arg = arg
                    .into_string()
                    .map_err(|_| ErrorCode::BadArguments("Expected string argument"))?;

                Ok(arg)
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Check TableArgs only contain named args.
    ///
    /// Returns the map of named args.
    pub fn expect_all_named(&self, func_name: &str) -> Result<HashMap<String, Scalar>> {
        if !self.positioned.is_empty() {
            debug!("{:?} accept named args only", self.positioned);
            Err(ErrorCode::BadArguments(format!(
                "{} accept named args only",
                func_name
            )))
        } else {
            Ok(self.named.clone())
        }
    }
}

pub fn string_value(value: &Scalar) -> Result<String> {
    match value {
        Scalar::String(val) => Ok(val.clone()),
        _ => Err(ErrorCode::BadArguments(format!(
            "invalid value {value} expect to be string literal."
        ))),
    }
}

pub fn bool_value(value: &Scalar) -> Result<bool> {
    match value {
        Scalar::Boolean(val) => Ok(*val),
        _ => Err(ErrorCode::BadArguments(format!(
            "invalid value {value} expect to be boolean literal."
        ))),
    }
}

pub fn i64_value(value: &Scalar) -> Result<i64> {
    value.get_i64().ok_or_else(|| {
        ErrorCode::BadArguments(format!("invalid value {value} expect to be i64 literal."))
    })
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
