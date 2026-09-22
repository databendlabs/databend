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

//! Definition-time validation of task SQL.
//!
//! `CREATE TASK` and `ALTER TASK ... MODIFY AS` run this pass over the task body. It is
//! deliberately static: it parses SQL, expands constant `EXECUTE IMMEDIATE` scripts and
//! compiles script blocks. It never binds statements, never resolves catalog objects or
//! UDFs, never executes anything and never touches the session context, so the task body
//! is rejected only for errors that are certain regardless of the runtime environment.

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::ScriptBlock;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TaskSql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::ScriptBlockOrStmt;
use databend_common_ast::parser::script::script_block_or_stmt;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::ParseError;
use databend_common_exception::Result;
use databend_common_script::compile_block;
use databend_common_script::ir::ScriptIR;

/// Upper bound on how many levels of constant `EXECUTE IMMEDIATE` are expanded.
///
/// Deeper nesting is still valid SQL; it is simply left to runtime planning.
const MAX_SCRIPT_NESTING: usize = 8;

/// Task bodies are normalised to PostgreSQL-dialect SQL when the `CREATE TASK` statement
/// is parsed, so that is the dialect they are re-parsed with here.
const TASK_SQL_DIALECT: Dialect = Dialect::PostgreSQL;

/// Validate every statement of a task body.
pub(super) fn validate_task_sql(sql: &TaskSql) -> Result<()> {
    match sql {
        TaskSql::SingleStatement(stmt) => validate_statement_sql(stmt),
        TaskSql::ScriptBlock(stmts) => stmts
            .iter()
            .try_for_each(|stmt| validate_statement_sql(stmt)),
    }
}

fn validate_statement_sql(sql: &str) -> Result<()> {
    let stmt = parse_statement(sql)?;
    validate_statement(stmt)
}

fn parse_statement(sql: &str) -> Result<Statement> {
    let syntax_error = |e: ParseError| {
        ErrorCode::SyntaxException(format!(
            "syntax error for task formatted sql: {}, error: {:?}",
            sql, e
        ))
    };
    let tokens = tokenize_sql(sql).map_err(syntax_error)?;
    let (stmt, _) = parse_sql(&tokens, TASK_SQL_DIALECT).map_err(syntax_error)?;
    Ok(stmt)
}

/// Walk a statement and the constant scripts it carries.
///
/// Only `EXECUTE IMMEDIATE` with a string literal is expanded. Any other script
/// expression needs evaluation, which is out of scope for a static check, so those
/// statements are accepted here and planned at runtime.
fn validate_statement(stmt: Statement) -> Result<()> {
    let mut pending = vec![(stmt, 0usize)];
    while let Some((stmt, depth)) = pending.pop() {
        let Some(script) = constant_execute_immediate(&stmt) else {
            continue;
        };
        if depth >= MAX_SCRIPT_NESTING {
            continue;
        }
        match parse_script(script)? {
            ScriptBlockOrStmt::Statement(nested) => pending.push((nested, depth + 1)),
            ScriptBlockOrStmt::ScriptBlock(block) => {
                for nested in compile_script_block(block, script)? {
                    pending.push((nested, depth + 1));
                }
            }
        }
    }
    Ok(())
}

fn parse_script(script: &str) -> Result<ScriptBlockOrStmt> {
    let tokens = tokenize_sql(script)?;
    Ok(run_parser(
        &tokens,
        TASK_SQL_DIALECT,
        ParseMode::Template,
        false,
        script_block_or_stmt,
    )?)
}

/// The script of a constant `EXECUTE IMMEDIATE`, looking through a `SETTINGS` wrapper.
fn constant_execute_immediate(stmt: &Statement) -> Option<&str> {
    match stmt {
        Statement::StatementWithSettings { stmt, .. } => constant_execute_immediate(stmt),
        Statement::ExecuteImmediate(execute) => match &execute.script {
            Expr::Literal {
                value: Literal::String(script),
                ..
            } => Some(script),
            _ => None,
        },
        _ => None,
    }
}

/// Compile a script block and collect the SQL statements it lowers to.
///
/// Compilation checks declarations, scopes and control flow (for example `BREAK` outside
/// a loop or a `RETURN` of an undeclared variable). Expressions and SQL in every branch
/// are lowered to `Query` instructions; the resulting IR is never run. The statements may
/// still contain `:var` holes that are only filled at runtime.
fn compile_script_block(block: ScriptBlock, script: &str) -> Result<Vec<Statement>> {
    let compiled = compile_block(block).map_err(|e| e.display_with_sql(script))?;
    Ok(compiled
        .into_iter()
        .filter_map(|instruction| match instruction {
            ScriptIR::Query { stmt, .. } => Some(stmt.stmt),
            _ => None,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(sql: &str) -> Result<()> {
        validate_task_sql(&TaskSql::SingleStatement(sql.to_string()))
    }

    fn assert_code(sql: &str, code: u16) {
        let err = validate(sql).unwrap_err();
        assert_eq!(err.code(), code, "{sql}: {err}");
    }

    #[test]
    fn rejects_syntax_errors_in_constant_scripts() {
        assert_code("SELECT FROM", ErrorCode::SYNTAX_EXCEPTION);
        assert_code(
            "EXECUTE IMMEDIATE 'SELECT FROM'",
            ErrorCode::SYNTAX_EXCEPTION,
        );
        assert_code(
            "EXECUTE IMMEDIATE $$ BEGIN IF FALSE THEN SELECT FROM; END IF; END; $$",
            ErrorCode::SYNTAX_EXCEPTION,
        );
        // Nested constant scripts are expanded, including through a SETTINGS wrapper.
        assert_code(
            "EXECUTE IMMEDIATE $$ BEGIN EXECUTE IMMEDIATE 'BEGIN SELECT FROM; END;'; END; $$",
            ErrorCode::SYNTAX_EXCEPTION,
        );
        assert_code(
            "SETTINGS (max_threads = 1) EXECUTE IMMEDIATE 'SELECT FROM'",
            ErrorCode::SYNTAX_EXCEPTION,
        );
    }

    #[test]
    fn rejects_script_compile_errors() {
        assert_code(
            "EXECUTE IMMEDIATE $$ BEGIN RETURN unknown_variable; END; $$",
            ErrorCode::SCRIPT_SEMANTIC_ERROR,
        );
        assert_code(
            "EXECUTE IMMEDIATE $$ BEGIN BREAK; END; $$",
            ErrorCode::SCRIPT_SEMANTIC_ERROR,
        );
        // A nested script is compiled on its own: its loop context is not inherited.
        assert_code(
            "EXECUTE IMMEDIATE $$ BEGIN LOOP EXECUTE IMMEDIATE 'BEGIN CONTINUE; END;'; END LOOP; END; $$",
            ErrorCode::SCRIPT_SEMANTIC_ERROR,
        );
    }

    #[test]
    fn checks_every_statement_of_a_script_block() {
        let sql = TaskSql::ScriptBlock(vec![
            "SELECT 1".to_string(),
            "EXECUTE IMMEDIATE 'SELECT FROM'".to_string(),
        ]);
        let err = validate_task_sql(&sql).unwrap_err();
        assert_eq!(err.code(), ErrorCode::SYNTAX_EXCEPTION);
    }

    #[test]
    fn accepts_anything_that_needs_runtime_context() {
        for sql in [
            // Scripts that are not string literals are not evaluated.
            "EXECUTE IMMEDIATE 1",
            "EXECUTE IMMEDIATE 'SELECT ' || 'FROM'",
            // Nothing is bound: objects, UDFs and columns are never resolved.
            "INSERT INTO not_created_yet SELECT 1",
            "SELECT udf_not_created_yet(1)",
            "SELECT no_such_col FROM not_created_yet",
            "SELECT * FROM s WITH CONSUME",
            "SELECT * FROM t PIVOT(SUM(a) FOR m IN (SELECT m FROM t))",
            "WITH m AS MATERIALIZED (SELECT 1) SELECT * FROM m",
            // Runtime script variables and dynamic identifiers.
            "EXECUTE IMMEDIATE $$ DECLARE t := 'x'; BEGIN SELECT * FROM IDENTIFIER(:t); RETURN t; END; $$",
            "EXECUTE IMMEDIATE $$ BEGIN FOR i IN 1 TO 2 DO SELECT :i; END FOR; END; $$",
        ] {
            validate(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn stops_expanding_beyond_the_nesting_limit() {
        // A syntax error hidden below the limit is left to runtime rather than expanded.
        let mut sql = "SELECT FROM".to_string();
        for _ in 0..=MAX_SCRIPT_NESTING {
            sql = format!("EXECUTE IMMEDIATE '{}'", sql.replace('\'', "''"));
        }
        validate(&sql).unwrap();
        // One level shallower is still expanded.
        let mut sql = "SELECT FROM".to_string();
        for _ in 0..MAX_SCRIPT_NESTING {
            sql = format!("EXECUTE IMMEDIATE '{}'", sql.replace('\'', "''"));
        }
        assert_code(&sql, ErrorCode::SYNTAX_EXCEPTION);
    }
}
