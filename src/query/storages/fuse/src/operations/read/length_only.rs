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

//! The string columns a prewhere filter reads only the byte lengths of, so that the scan reads
//! only the lengths of their values (see `BlockReader::with_length_only_columns`).
//!
//! A column qualifies when the filter reads it only through `octet_length(c)`, `c = ''` and
//! `c <> ''`: the comparisons are `octet_length(c) = 0` and `octet_length(c) > 0`, and are
//! evaluated from the lengths of the values in their views (see `compare_empty_string`). They
//! are left as they are: on a dictionary, `c <> ''` compares its values, while `octet_length(c)
//! > 0` decodes it.

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_expression::Cast;
use databend_common_expression::Expr;
use databend_common_expression::FunctionCall;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;

/// The `String` and `Nullable(String)` columns `filter` reads only the byte lengths of.
pub fn length_only_columns(filter: &Expr<String>) -> HashSet<String> {
    let mut length_only = HashMap::new();
    if !visit(filter, &mut length_only) {
        return HashSet::new();
    }
    length_only
        .into_iter()
        .filter_map(|(name, length_only)| length_only.then_some(name))
        .collect()
}

/// Records in `length_only` whether each column `expr` reads is read only for its lengths.
/// `false` if `expr` has a lambda, whose body is not looked into.
fn visit(expr: &Expr<String>, length_only: &mut HashMap<String, bool>) -> bool {
    match expr {
        Expr::Constant(_) => true,
        Expr::ColumnRef(column) => {
            length_only.insert(column.id.clone(), false);
            true
        }
        Expr::Cast(Cast { expr, .. }) => visit(expr, length_only),
        Expr::FunctionCall(call) => {
            if let Some(column) = length_of(call) {
                length_only.entry(column.to_string()).or_insert(true);
                return true;
            }
            call.args.iter().all(|arg| visit(arg, length_only))
        }
        Expr::LambdaFunctionCall(_) => false,
    }
}

/// The column of `octet_length(c)`, `c = ''` or `c <> ''`, if `call` is one.
fn length_of(call: &FunctionCall<String>) -> Option<&str> {
    match (call.function.signature.name.as_str(), call.args.as_slice()) {
        ("octet_length", [arg]) => string_column(arg),
        ("eq" | "noteq", [left, right]) => match (string_column(left), string_column(right)) {
            (Some(column), None) if is_empty_string(right) => Some(column),
            (None, Some(column)) if is_empty_string(left) => Some(column),
            _ => None,
        },
        _ => None,
    }
}

fn string_column(expr: &Expr<String>) -> Option<&str> {
    match expr {
        Expr::ColumnRef(column) if column.data_type.remove_nullable() == DataType::String => {
            Some(&column.id)
        }
        _ => None,
    }
}

fn is_empty_string(expr: &Expr<String>) -> bool {
    match expr {
        Expr::Constant(constant) => matches!(&constant.scalar, Scalar::String(s) if s.is_empty()),
        Expr::Cast(Cast { expr, .. }) => is_empty_string(expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::NumberDataType;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    use super::*;

    fn column(name: &str, data_type: DataType) -> Expr<String> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: name.to_string(),
            data_type,
            display_name: name.to_string(),
        })
    }

    fn string(value: &str) -> Expr<String> {
        Expr::Constant(databend_common_expression::Constant {
            span: None,
            scalar: Scalar::String(value.to_string()),
            data_type: DataType::String,
        })
    }

    fn call(name: &str, args: &[Expr<String>]) -> Expr<String> {
        check_function(None, name, &[], args, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn names(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn test_length_only_columns() {
        let url = column("url", DataType::String);
        let title = column("title", DataType::String.wrap_nullable());
        let id = column("id", DataType::Number(NumberDataType::Int64));

        let filter = call("and_filters", &[
            call("noteq", &[url.clone(), string("")]),
            call("eq", &[string(""), title.clone()]),
            call("gt", &[
                call("octet_length", &[url.clone()]),
                call("to_uint64", &[id.clone()]),
            ]),
        ]);
        assert_eq!(length_only_columns(&filter), names(&["url", "title"]));

        // Other uses read the values.
        let filter = call("and_filters", &[
            call("noteq", &[url.clone(), string("")]),
            call("noteq", &[title.clone(), string("a")]),
            call("like", &[url.clone(), string("%a%")]),
        ]);
        assert!(length_only_columns(&filter).is_empty());
        let filter = call("eq", &[url.clone(), title.clone()]);
        assert!(length_only_columns(&filter).is_empty());
        let filter = call("gt", &[
            call("length", &[url.clone()]),
            call("to_uint64", &[id.clone()]),
        ]);
        assert!(length_only_columns(&filter).is_empty());
    }
}
