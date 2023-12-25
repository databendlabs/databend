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

use std::sync::LazyLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnIndex;
use databend_common_expression::Expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use regex::Regex;

use crate::IndexType;

/// Format the display name and index of a column into `"{display_name}"_index` format.
pub fn format_field_name(display_name: &str, index: IndexType) -> String {
    format!("\"{}\"_{}", display_name, index)
}

static FIELD_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("\"([^\"]*)\"_([0-9]+)").unwrap());

/// Decode a field name into display name and index
pub fn decode_field_name(field_name: &str) -> Result<(String, IndexType)> {
    let result = FIELD_NAME_RE.captures(field_name);
    match result {
        Some(res) => {
            if res.len() != 3 {
                Err(ErrorCode::Internal(format!(
                    "Invalid field name: {field_name}"
                )))
            } else {
                let name = res[1].to_string();
                let index = res[2].parse::<IndexType>()?;
                Ok((name, index))
            }
        }
        None => Err(ErrorCode::Internal(format!(
            "Invalid field name: {field_name}"
        ))),
    }
}

/// Wrap the expression into `is_true(try_cast(<expr> as boolean))` to make sure the expression
/// will always return a boolean value.
pub fn cast_expr_to_non_null_boolean<Index: ColumnIndex>(expr: Expr<Index>) -> Result<Expr<Index>> {
    if expr.data_type() == &DataType::Boolean {
        Ok(expr)
    } else {
        type_check::check_function(
            None,
            "is_true",
            &[],
            &[type_check::check_cast(
                None,
                true,
                expr,
                &DataType::Boolean,
                &BUILTIN_FUNCTIONS,
            )?],
            &BUILTIN_FUNCTIONS,
        )
    }
}
