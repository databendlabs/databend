//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::table_functions::TableArgs;

pub fn string_value(expr: &Expression) -> Result<String> {
    if let Expression::Literal { value, .. } = expr {
        String::from_utf8(value.as_string()?)
            .map_err(|e| ErrorCode::BadArguments(format!("invalid string. {}", e)))
    } else {
        Err(ErrorCode::BadArguments(format!(
            "expecting string literal, but got {:?}",
            expr
        )))
    }
}

pub fn string_literal(val: &str) -> Expression {
    Expression::create_literal(DataValue::String(Some(val.as_bytes().to_vec())))
}

pub fn parse_table_history_args(table_args: &TableArgs) -> Result<(String, String)> {
    match table_args {
        Some(args) if args.len() == 2 => {
            let db = string_value(&args[0])?;
            let tbl = string_value(&args[1])?;
            Ok((db, tbl))
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "expecting database and table name (as two string literals), but got {:?}",
            table_args
        ))),
    }
}

pub fn parse_truncate_history_table_args(table_args: &TableArgs) -> Result<(String, String, bool)> {
    if let Some(args) = table_args {
        let len = args.len();
        if len == 2 || len == 3 {
            let db = string_value(&args[0])?;
            let tbl = string_value(&args[1])?;
            let all_flag = args
                .get(2)
                .map(|v| string_value(v))
                .unwrap_or(Ok("all".to_owned()))?;
            let all = all_flag.to_lowercase().as_str() == "fall";
            return Ok((db, tbl, all));
        }
    };

    Err(ErrorCode::BadArguments(format!(
        "expecting (database_name, table_name [, all]),  as 2 or 2 or 3 string literals, but got {:?}",
        table_args
    )))
}
