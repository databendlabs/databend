// Copyright 2023 Datafuse Labs.
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

use databend_common_constraint::mir::MirDataType;
use databend_common_constraint::problem::variable_must_not_null;

use crate::parser::parse_mir_expr;

#[test]
fn test_assert_int_not_null() {
    let variables = &[("a".to_string(), MirDataType::Int)].into_iter().collect();

    assert!(variable_must_not_null(
        &parse_mir_expr("a is not null", variables),
        "a",
    ));
    assert!(variable_must_not_null(
        &parse_mir_expr("a > 0", variables),
        "a",
    ));
    assert!(variable_must_not_null(
        &parse_mir_expr("a > 0 or a < 0", variables),
        "a",
    ));
    assert!(variable_must_not_null(
        &parse_mir_expr("a > 0 and a < 1", variables),
        "a",
    ));

    assert!(!variable_must_not_null(
        &parse_mir_expr("a > 0 or true", variables),
        "a",
    ));
    assert!(!variable_must_not_null(
        &parse_mir_expr("a is null", variables),
        "a",
    ));
}

#[test]
fn test_assert_int_is_not_null_multiple_variable() {
    let variables = &[
        ("a".to_string(), MirDataType::Int),
        ("b".to_string(), MirDataType::Int),
    ]
    .into_iter()
    .collect();

    assert!(variable_must_not_null(
        &parse_mir_expr("a > 0 and b > 0", variables),
        "a",
    ));
    assert!(variable_must_not_null(
        &parse_mir_expr("a > 0 and b > 0", variables),
        "b",
    ));
}
