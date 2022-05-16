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

use common_exception::Result;
use databend_query::sql::PlanParser;
use pretty_assertions::assert_eq;

#[test]
fn test_expr_parser() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: &'static str,
        expect: &'static str,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "1 + 2",
            expr: "1 + 2",
            expect: "(1 + 2)",
            error: "",
        },
        Test {
            name: "1 + 2 with parenthesis",
            expr: "(1 + 2)",
            expect: "(1 + 2)",
            error: "",
        },
        Test {
            name: "a + b * c",
            expr: "a + b * c",
            expect: "(a + (b * c))",
            error: "",
        },
        Test {
            name: "a + b * c with parenthesis",
            expr: "(a + b * c)",
            expect: "(a + (b * c))",
            error: "",
        },
        Test {
            name: "cast",
            expr: "cast(a as int)",
            expect: "cast(a as INT)",
            error: "",
        },
    ];

    for t in tests {
        match PlanParser::parse_expr(t.expr) {
            Ok(v) => {
                assert_eq!(t.expect, v.column_name(), "{}", t.name);
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string(), "{}", t.name);
            }
        }
    }

    Ok(())
}
