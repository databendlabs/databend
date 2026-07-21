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

use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::ExplainOption;
use databend_common_ast::ast::Statement;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;

fn test_stmt_display(sql: &str) {
    let tokens = tokenize_sql(sql).unwrap();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
    let sql1 = stmt.to_string();
    let tokens1 = tokenize_sql(&sql1).unwrap();
    let (stmt1, _) = parse_sql(&tokens1, Dialect::PostgreSQL).unwrap();
    let sql2 = stmt1.to_string();
    assert_eq!(sql1, sql2);
}

#[test]
fn test_multi_table_insert_display() {
    const SQL_FILE_PATH: &str = "tests/it/testsql/multi_table_insert.sql";
    let sqls = std::fs::read_to_string(SQL_FILE_PATH).unwrap();
    for sql in sqls.split(';').filter(|s| !s.is_empty()) {
        test_stmt_display(sql);
    }
}

#[test]
fn test_multi_table_insert_parse_error() {
    const SQL_FILE_PATH: &str = "tests/it/testsql/multi_table_insert_error.sql";
    let sqls = std::fs::read_to_string(SQL_FILE_PATH).unwrap();
    for sql in sqls.split(';').filter(|s| !s.is_empty()) {
        let tokens = tokenize_sql(sql).unwrap();
        assert!(parse_sql(&tokens, Dialect::PostgreSQL).is_err());
    }
}

#[test]
fn test_like_escape_display_escapes_escape_literal() {
    for sql in [
        r#"SELECT 'a' LIKE 'a' ESCAPE '''';"#,
        r#"SELECT 'a' LIKE ANY ('a', 'b') ESCAPE '''';"#,
        r#"SELECT 'a' ILIKE 'a' ESCAPE '''';"#,
        r#"SELECT 'a' NOT ILIKE 'a' ESCAPE '''';"#,
        r#"SELECT 'a' ILIKE ANY ('a', 'b') ESCAPE '''';"#,
        r#"SELECT 'a' LIKE ANY (SELECT 'a') ESCAPE '''';"#,
    ] {
        test_stmt_display(sql);
    }
}

#[test]
fn test_rewrite_statement_display_escapes_string_literals() {
    for sql in [
        r#"SHOW SETTINGS LIKE 'a''b%';"#,
        r#"SHOW TABLES LIKE 'a''b%';"#,
        r#"LIST @test_stage PATTERN = 'a''b.*';"#,
        r#"REMOVE @test_stage PATTERN = 'a''b.*';"#,
        r#"CALL admin$tenant_quota('a''b');"#,
    ] {
        test_stmt_display(sql);
    }
}

#[test]
fn test_analyze_table_histogram_options() {
    let sql = "ANALYZE TABLE t WITH HISTOGRAM";
    let tokens = tokenize_sql(sql).unwrap();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();

    match &stmt {
        Statement::AnalyzeTable(stmt) => {
            assert!(!stmt.no_scan);
            let options = stmt
                .histogram_options
                .as_ref()
                .expect("histogram options should be parsed");
            assert_eq!(options.algorithm.as_deref(), None);
            assert_eq!(options.error_rate, None);
        }
        _ => panic!("expected ANALYZE TABLE statement"),
    }

    test_stmt_display(sql);

    let sql = "ANALYZE TABLE t NOSCAN WITH HISTOGRAM ALGORITHM = 'kll_full', ERROR_RATE = 0.01";
    let tokens = tokenize_sql(sql).unwrap();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();

    match &stmt {
        Statement::AnalyzeTable(stmt) => {
            assert!(stmt.no_scan);
            let options = stmt
                .histogram_options
                .as_ref()
                .expect("histogram options should be parsed");
            assert_eq!(options.algorithm.as_deref(), Some("kll_full"));
            assert_eq!(options.error_rate, Some(0.01));
        }
        _ => panic!("expected ANALYZE TABLE statement"),
    }

    test_stmt_display(sql);
}

#[test]
fn test_parse_sql_nested_join_conditions_without_panic() {
    let cases = [
        r#"
        SELECT * FROM (VALUES(NULL)) AS tbl(i)
          INNER JOIN (
            (VALUES(NULL)) AS tbl2(i)
            INNER JOIN (VALUES(NULL)) AS tbl3(i) ON tbl2.i = tbl3.i
          ) USING(i);
        "#,
        r#"
        SELECT i1.i AS i1_i, i2.s, i3.i AS i3_i
          FROM integers1 AS i1
          LEFT OUTER JOIN (
            integers2 AS i2
            LEFT OUTER JOIN integers3 AS i3 ON i2.i = i3.i
          ) ON NULL;
        "#,
    ];

    for sql in cases {
        test_stmt_display(sql);
        let tokens = tokenize_sql(sql).unwrap();
        parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
    }
}

#[test]
fn test_explain_verbose_alias_display() {
    let tokens = tokenize_sql("EXPLAIN VERBOSE SELECT * FROM t").unwrap();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();

    match &stmt {
        Statement::Explain {
            kind,
            options: (_, options),
            ..
        } => {
            assert_eq!(kind, &ExplainKind::Plan);
            assert_eq!(options, &vec![ExplainOption::Verbose]);
        }
        _ => panic!("expected EXPLAIN statement"),
    }

    assert_eq!(stmt.to_string(), "EXPLAIN(VERBOSE) SELECT * FROM t");
    test_stmt_display("EXPLAIN VERBOSE SELECT * FROM t");
}
