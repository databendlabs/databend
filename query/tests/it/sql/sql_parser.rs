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
use databend_query::sql::statements::DfQueryStatement;
use databend_query::sql::*;
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Tokenizer;

pub fn expect_parse_ok(sql: &str, expected: DfStatement) -> Result<()> {
    let (statements, _) = DfParser::parse_sql(sql)?;
    assert_eq!(
        statements.len(),
        1,
        "Expected to parse exactly one statement"
    );
    assert_eq!(statements[0], expected);
    Ok(())
}

pub fn expect_parse_err(sql: &str, expected: String) -> Result<()> {
    let result = DfParser::parse_sql(sql);
    assert!(result.is_err(), "'{}' SHOULD BE '{}'", sql, expected);
    assert_eq!(
        result.unwrap_err().message(),
        expected,
        "'{}' SHOULD BE '{}'",
        sql,
        expected
    );
    Ok(())
}

pub fn expect_parse_err_contains(sql: &str, expected: String) -> Result<()> {
    let result = DfParser::parse_sql(sql);
    assert!(result.is_err(), "'{}' SHOULD CONTAINS '{}'", sql, expected);
    assert!(
        result.unwrap_err().message().contains(&expected),
        "'{}' SHOULD CONTAINS '{}'",
        sql,
        expected
    );
    Ok(())
}

pub fn verified_query(sql: &str) -> Result<Box<DfQueryStatement>> {
    let mut parser = DfParser::new_with_dialect(sql, &GenericDialect {})?;
    let stmt = parser.parse_statement()?;
    if let DfStatement::Query(query) = stmt {
        return Ok(query);
    }
    Err(ParserError::ParserError("Expect query statement".to_string()).into())
}

pub fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: Ident {
            value: name.into(),
            quote_style: None,
        },
        data_type,
        collation: None,
        options: vec![],
    }
}

pub fn parse_sql_to_expr(query_expr: &str) -> Expr {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, query_expr);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(tokens, &dialect);
    parser.parse_expr().unwrap()
}

#[test]
fn hint_test() -> Result<()> {
    {
        let comment = " { ErrorCode  1002 }";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, Some(1002));
    }

    {
        let comment = " { ErrorCode1002 }";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, None);
    }

    {
        let comment = " { ErrorCode 22}";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, Some(22));
    }

    {
        let comment = " { ErrorCode: 22}";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, None);
    }

    {
        let comment = " { Errorcode 22}";
        let expected = DfHint::create_from_comment(comment, "--");
        assert_eq!(expected.error_code, None);
    }

    Ok(())
}
