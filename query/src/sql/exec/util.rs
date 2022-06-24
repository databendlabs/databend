// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use lazy_static::lazy_static;
use regex::Regex;

use crate::sql::optimizer::SExpr;
use crate::sql::plans::Operator;
use crate::sql::IndexType;

// Check if all plans in an expression are physical plans
pub fn check_physical(expression: &SExpr) -> bool {
    if !expression.plan().is_physical() {
        return false;
    }

    for child in expression.children() {
        if !check_physical(child) {
            return false;
        }
    }

    true
}

/// Format the display name and index of a column into `"{display_name}"_index` format.
pub fn format_field_name(display_name: &str, index: IndexType) -> String {
    format!("\"{}\"_{}", display_name, index)
}

lazy_static! {
    static ref FIELD_NAME_RE: Regex = Regex::new("\"([^\"]*)\"_([0-9]+)").unwrap();
}

/// Decode a field name into display name and index
pub fn decode_field_name(field_name: &str) -> Result<(String, IndexType)> {
    let result = FIELD_NAME_RE.captures(field_name);
    match result {
        Some(res) => {
            if res.len() != 3 {
                Err(ErrorCode::LogicalError(format!(
                    "Invalid field name: {field_name}"
                )))
            } else {
                let name = res[1].to_string();
                let index = res[2].parse::<IndexType>()?;
                Ok((name, index))
            }
        }
        None => Err(ErrorCode::LogicalError(format!(
            "Invalid field name: {field_name}"
        ))),
    }
}
