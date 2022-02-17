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
use databend_query::sql::statements::DfAlterUDF;
use databend_query::sql::statements::DfCreateUDF;
use databend_query::sql::statements::DfDropUDF;
use databend_query::sql::*;

use crate::sql::sql_parser::*;

#[test]
fn test_create_udf() -> Result<()> {
    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS p -> not(isnotnull(p))",
        "Expected (, found: p".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (as) -> not(isnotnull(as))",
        "Keyword can not be parameter, got: as".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (\"p\") -> not(isnotnull(p))",
        "Quote is not allowed in parameters, remove: \"".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p, p) -> not(isnotnull(p))",
        "Duplicate parameter is not allowed, keep only one: p".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p:) -> not(isnotnull(p))",
        "Expect words or comma, but got: :".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p,) -> not(isnotnull(p))",
        "Found a redundant `,` in the parameters".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p;) -> not(isnotnull(p))",
        "Can not find complete parameters, `)` is missing".to_string(),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf AS (p) -> not(isnotnull(p))",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string()],
            definition: "not(isnotnull(p))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d))",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC",
        "Expected =, found: ".to_string(),
    )?;

    expect_parse_err_contains(
        "CREATE FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC =",
        "Expected literal string, found: EOF".to_string(),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d)) DESC = 'this is a description'",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "this is a description".to_string(),
        }),
    )?;

    expect_parse_ok(
        "CREATE FUNCTION test_udf as (p, d) -> not(isnotnull(p, d)) DESC = 'this is a description'",
        DfStatement::CreateUDF(DfCreateUDF {
            if_not_exists: false,
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "this is a description".to_string(),
        }),
    )?;

    Ok(())
}

#[test]
fn test_drop_udf() -> Result<()> {
    expect_parse_ok(
        "DROP FUNCTION test_udf",
        DfStatement::DropUDF(DfDropUDF {
            if_exists: false,
            udf_name: "test_udf".to_string(),
        }),
    )?;

    expect_parse_ok(
        "DROP FUNCTION IF EXISTS test_udf",
        DfStatement::DropUDF(DfDropUDF {
            if_exists: true,
            udf_name: "test_udf".to_string(),
        }),
    )?;

    Ok(())
}

#[test]
fn test_alter_udf() -> Result<()> {
    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS p -> not(isnotnull(p))",
        "Expected (, found: p".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (as) -> not(isnotnull(as))",
        "Keyword can not be parameter, got: as".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (\"p\") -> not(isnotnull(p))",
        "Quote is not allowed in parameters, remove: \"".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p, p) -> not(isnotnull(p))",
        "Duplicate parameter is not allowed, keep only one: p".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p:) -> not(isnotnull(p))",
        "Expect words or comma, but got: :".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p,) -> not(isnotnull(p))",
        "Found a redundant `,` in the parameters".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p;) -> not(isnotnull(p))",
        "Can not find complete parameters, `)` is missing".to_string(),
    )?;

    expect_parse_ok(
        "ALTER FUNCTION test_udf AS (p) -> not(isnotnull(p))",
        DfStatement::AlterUDF(DfAlterUDF {
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string()],
            definition: "not(isnotnull(p))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_ok(
        "ALTER FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d))",
        DfStatement::AlterUDF(DfAlterUDF {
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "".to_string(),
        }),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC",
        "Expected =, found: ".to_string(),
    )?;

    expect_parse_err_contains(
        "ALTER FUNCTION test_udf AS (p) -> not(isnotnull(p)) DESC =",
        "Expected literal string, found: EOF".to_string(),
    )?;

    expect_parse_ok(
        "ALTER FUNCTION test_udf AS (p, d) -> not(isnotnull(p, d)) DESC = 'this is a description'",
        DfStatement::AlterUDF(DfAlterUDF {
            udf_name: "test_udf".to_string(),
            parameters: vec!["p".to_string(), "d".to_string()],
            definition: "not(isnotnull(p,d))".to_string(),
            description: "this is a description".to_string(),
        }),
    )?;

    Ok(())
}
