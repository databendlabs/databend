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

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::type_check;
use databend_common_expression::types::*;

#[test]
fn test_fmt_expr() {
    let mut fake_registry = FunctionRegistry::empty();
    fake_registry.register_2_arg_core::<StringType, StringType, StringType, _, _>(
        "test_fn",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar("x".to_string()),
    );

    let raw_expr = RawExpr::Cast {
        span: None,
        is_try: false,
        expr: Box::new(RawExpr::ColumnRef {
            span: None,
            id: 1,
            data_type: BooleanType::data_type(),
            display_name: "aaa".to_string(),
        }),
        dest_type: StringType::data_type(),
    };

    let raw_expr = RawExpr::FunctionCall {
        span: None,
        name: "test_fn".to_string(),
        params: vec![Scalar::Null],
        args: vec![raw_expr.clone(), raw_expr.clone()],
    };

    let expr = type_check::check(&raw_expr, &fake_registry).unwrap();

    assert_eq!(
        "test_fn<String, String>(CAST<Boolean>(aaa AS String), CAST<Boolean>(aaa AS String))",
        format!("{expr}")
    );
    assert_eq!(
        "test_fn<String, String>(CAST<Boolean>(aaa (#1) AS String), CAST<Boolean>(aaa (#1) AS String))",
        format!("{}", expr.fmt_with_options(true))
    );
}
