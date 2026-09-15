// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TaskSql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;

fn parse_statement(sql: &str) -> Statement {
    let tokens = tokenize_sql(sql).unwrap();
    parse_sql(&tokens, Dialect::PostgreSQL)
        .unwrap_or_else(|err| panic!("failed to parse {sql}: {err:?}"))
        .0
}

fn assert_task_roundtrip(sql: &str) -> TaskSql {
    let stmt = parse_statement(sql);
    let formatted = stmt.to_string();
    assert_eq!(parse_statement(&formatted).to_string(), formatted);

    let body = match stmt {
        Statement::CreateTask(task) => task.sql,
        Statement::AlterTask(task) => match task.options {
            AlterTaskOptions::ModifyAs(sql) => sql,
            _ => panic!("expected MODIFY AS"),
        },
        _ => panic!("expected a task statement"),
    };

    // The binder validates each stored statement, and Cloud submits it separately.
    match &body {
        TaskSql::SingleStatement(sql) => {
            parse_statement(sql);
        }
        TaskSql::ScriptBlock(sqls) => {
            for sql in sqls {
                parse_statement(sql);
            }
        }
    }
    body
}

#[test]
fn test_task_nested_modify_single_statement() {
    assert_eq!(
        assert_task_roundtrip(
            "CREATE TASK driver SCHEDULE = 60 MINUTE AS BEGIN \
             ALTER TASK target MODIFY AS SELECT 2; END;"
        ),
        TaskSql::ScriptBlock(vec!["ALTER TASK target MODIFY AS SELECT 2".to_string()])
    );

    for prefix in [
        "CREATE TASK driver SCHEDULE = 60 MINUTE AS",
        "ALTER TASK driver MODIFY AS",
    ] {
        let sql = format!(
            "{prefix} BEGIN
                SELECT 'before;';
                ALTER TASK target MODIFY AS SELECT 2;
                SELECT 'after;';
            END;"
        );
        assert_eq!(
            assert_task_roundtrip(&sql),
            TaskSql::ScriptBlock(vec![
                "SELECT 'before;'".to_string(),
                "ALTER TASK target MODIFY AS SELECT 2".to_string(),
                "SELECT 'after;'".to_string(),
            ])
        );
    }
}

#[test]
fn test_task_nested_modify_script_block() {
    for prefix in ["CREATE TASK driver AS", "ALTER TASK driver MODIFY AS"] {
        let sql = format!(
            "{prefix} BEGIN
                ALTER TASK target MODIFY AS BEGIN
                    SELECT 2;
                    SELECT 'value;';
                END;
                SELECT 3;
            END;"
        );
        assert_eq!(
            assert_task_roundtrip(&sql),
            TaskSql::ScriptBlock(vec![
                "ALTER TASK target MODIFY AS BEGIN\nSELECT 2;\nSELECT 'value;';\nEND;".to_string(),
                "SELECT 3".to_string(),
            ])
        );
    }
}

#[test]
fn test_task_nested_create() {
    assert_eq!(
        assert_task_roundtrip(
            "CREATE TASK driver AS BEGIN
                CREATE TASK target AS SELECT 1;
                CREATE TASK nested_driver AS BEGIN
                    ALTER TASK target MODIFY AS BEGIN SELECT 2; END;
                END;
            END;"
        ),
        TaskSql::ScriptBlock(vec![
            "CREATE TASK target AS SELECT 1".to_string(),
            "CREATE TASK nested_driver AS BEGIN\nALTER TASK target MODIFY AS BEGIN\nSELECT 2;\nEND;\nEND;".to_string(),
        ])
    );
}

#[test]
fn test_task_single_statement_body() {
    for prefix in ["CREATE TASK driver AS", "ALTER TASK driver MODIFY AS"] {
        for body in ["SELECT 1", "SELECT 1;", "SELECT 1 FORMAT JSON;"] {
            assert_eq!(
                assert_task_roundtrip(&format!("{prefix} {body}")),
                TaskSql::SingleStatement("SELECT 1".to_string())
            );
        }
        assert_eq!(
            assert_task_roundtrip(&format!("{prefix} BEGIN;")),
            TaskSql::SingleStatement("BEGIN".to_string())
        );
        assert_eq!(
            assert_task_roundtrip(&format!(
                "{prefix} ALTER TASK target MODIFY AS BEGIN SELECT 2; END;"
            )),
            TaskSql::SingleStatement(
                "ALTER TASK target MODIFY AS BEGIN\nSELECT 2;\nEND;".to_string()
            )
        );
    }
}

#[test]
fn test_task_transaction_statements() {
    assert_eq!(
        assert_task_roundtrip(
            "CREATE TASK driver AS BEGIN
                BEGIN;
                INSERT INTO t VALUES ('value;');
                COMMIT;
            END;"
        ),
        TaskSql::ScriptBlock(vec![
            "BEGIN".to_string(),
            "INSERT INTO t VALUES ('value;')".to_string(),
            "COMMIT".to_string(),
        ])
    );
}

#[test]
fn test_task_rejects_invalid_statement_boundaries() {
    for sql in [
        "CREATE TASK driver AS SELECT 1; SELECT 2;",
        "ALTER TASK target MODIFY AS SELECT 1; SELECT 2;",
        "CREATE TASK driver AS BEGIN END;",
        "CREATE TASK driver AS BEGIN ALTER TASK target MODIFY AS SELECT 2 END;",
        "CREATE TASK driver AS BEGIN ALTER TASK target MODIFY AS SELECT 2;; END;",
        "CREATE TASK driver AS BEGIN ALTER TASK target MODIFY AS BEGIN SELECT 2; END END;",
        "CREATE TASK driver AS BEGIN ALTER TASK target MODIFY AS BEGIN SELECT 2; END;",
    ] {
        let tokens = tokenize_sql(sql).unwrap();
        assert!(
            parse_sql(&tokens, Dialect::PostgreSQL).is_err(),
            "unexpectedly accepted {sql}"
        );
    }
}
