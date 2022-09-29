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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_expression::*;

#[test]
fn test_expression_validate() -> Result<()> {
    struct Test {
        desc: &'static str,
        expression: LegacyExpression,
        error: Option<ErrorCode>,
    }

    let cases = vec![
        Test {
            desc: "typeof-not-pass",
            expression: LegacyExpression::ScalarFunction {
                op: "typeof".to_string(),
                args: vec![],
            },
            error: Some(ErrorCode::NumberArgumentsNotMatch(
                "Function `typeof` expect to have 1 arguments, but got 0",
            )),
        },
        Test {
            desc: "today-not-pass",
            expression: LegacyExpression::ScalarFunction {
                op: "today".to_string(),
                args: vec![col("a")],
            },
            error: Some(ErrorCode::NumberArgumentsNotMatch(
                "Function `today` expect to have 0 arguments, but got 1",
            )),
        },
        Test {
            desc: "today-pass",
            expression: LegacyExpression::ScalarFunction {
                op: "today".to_string(),
                args: vec![],
            },
            error: None,
        },
    ];

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "a",
        u64::to_data_type(),
    )]));
    for t in cases.iter() {
        let result = validate_expression(&t.expression, &schema);
        match t.error {
            Some(_) => {
                assert_eq!(
                    t.error.as_ref().unwrap().message(),
                    result.err().unwrap().message(),
                    "{}",
                    t.desc
                );
            }
            None => assert!(result.is_ok(), "{}", t.desc),
        }
    }
    Ok(())
}
