// Copyright 2022 Datafuse Labs.
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
use databend_query::sql::statements::DfCall;
use databend_query::sql::DfStatement;

use crate::sql::sql_parser::expect_parse_err_contains;
use crate::sql::sql_parser::expect_parse_ok;

#[test]
fn test_call() -> Result<()> {
    expect_parse_err_contains("CALL system$", "Expected (, found: EOF".to_string())?;

    expect_parse_err_contains("CALL system$(", "Expected a value, found: EOF".to_string())?;

    expect_parse_err_contains("CALL system$(1", "Expected ), found: EOF".to_string())?;

    expect_parse_ok(
        "CALL system$test()",
        DfStatement::Call(DfCall {
            name: "system$test".to_string(),
            args: vec![],
        }),
    )?;

    expect_parse_ok(
        "CALL system$test(1)",
        DfStatement::Call(DfCall {
            name: "system$test".to_string(),
            args: vec!["1".to_string()],
        }),
    )?;

    expect_parse_ok(
        "CALL system$test(1, 2)",
        DfStatement::Call(DfCall {
            name: "system$test".to_string(),
            args: vec!["1".to_string(), "2".to_string()],
        }),
    )?;

    Ok(())
}
