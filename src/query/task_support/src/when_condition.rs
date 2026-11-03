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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::WalkMut;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Build the query used to evaluate a private task's WHEN condition.
/// STREAM_STATUS is a table function in ordinary SQL, but task conditions
/// accept it as a boolean expression. Keep this adaptation local to tasks.
pub fn task_when_sql(condition: &str, dialect: Dialect) -> Result<String> {
    let tokens = tokenize_sql(condition)?;
    let mut expr = parse_expr(&tokens, dialect)?;
    expr.walk_mut(&mut StreamStatusRewriter { dialect })?;
    Ok(format!("SELECT {expr}"))
}

struct StreamStatusRewriter {
    dialect: Dialect,
}

impl VisitorMut for StreamStatusRewriter {
    type Error = ErrorCode;

    fn visit_expr(&mut self, expr: &mut Expr) -> Result<VisitControl> {
        let Expr::FunctionCall { func, .. } = expr else {
            return Ok(VisitControl::Continue);
        };
        if !func.name.name.eq_ignore_ascii_case("stream_status") {
            return Ok(VisitControl::Continue);
        }
        if !matches!(func.args.as_slice(), [Expr::Literal {
            value: Literal::String(_),
            ..
        }]) || func.distinct
            || !func.params.is_empty()
            || !func.order_by.is_empty()
            || func.filter.is_some()
            || func.window.is_some()
            || func.lambda.is_some()
        {
            return Err(ErrorCode::BadArguments(
                "STREAM_STATUS in a task WHEN condition requires one stream name string, for example STREAM_STATUS('mydb.my_stream')",
            ));
        }
        // Format the parsed string literal, not its raw contents, so embedded
        // quotes remain escaped and cannot introduce SQL into the query.
        let sql = format!("(SELECT has_data FROM STREAM_STATUS({}))", func.args[0]);
        *expr = parse_expr(&tokenize_sql(&sql)?, self.dialect)?;
        Ok(VisitControl::SkipChildren)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;

    use super::task_when_sql;

    #[test]
    fn test_stream_status_condition() {
        for dialect in [Dialect::PostgreSQL, Dialect::MySQL] {
            assert_eq!(
                task_when_sql("STREAM_STATUS('db.s')", dialect).unwrap(),
                "SELECT (SELECT has_data FROM STREAM_STATUS('db.s'))"
            );
            assert_eq!(
                task_when_sql(
                    "NOT stream_status('db.s') OR STREAM_STATUS('db.other') = TRUE",
                    dialect
                )
                .unwrap(),
                "SELECT NOT (SELECT has_data FROM STREAM_STATUS('db.s')) OR (SELECT has_data FROM STREAM_STATUS('db.other')) = TRUE"
            );
            assert_eq!(
                task_when_sql("CAST(STREAM_STATUS('db.s') AS BOOLEAN)", dialect).unwrap(),
                "SELECT CAST((SELECT has_data FROM STREAM_STATUS('db.s')) AS BOOLEAN)"
            );
        }
    }

    #[test]
    fn test_existing_conditions() {
        assert_eq!(
            task_when_sql("1 = 0", Dialect::PostgreSQL).unwrap(),
            "SELECT 1 = 0"
        );
        assert_eq!(
            task_when_sql("EXISTS (SELECT 1 FROM t WHERE c = 1)", Dialect::PostgreSQL).unwrap(),
            "SELECT EXISTS (SELECT 1 FROM t WHERE c = 1)"
        );
        assert_eq!(
            task_when_sql(
                "(SELECT has_data FROM STREAM_STATUS('db.s'))",
                Dialect::PostgreSQL
            )
            .unwrap(),
            "SELECT (SELECT has_data FROM STREAM_STATUS('db.s'))"
        );
    }

    #[test]
    fn test_invalid_stream_status_arguments() {
        for condition in [
            "STREAM_STATUS()",
            "STREAM_STATUS('db.s', 'db.other')",
            "STREAM_STATUS(1)",
            "STREAM_STATUS(\"db.s\")",
            "STREAM_STATUS(DISTINCT 'db.s')",
            "STREAM_STATUS('db.s') OVER ()",
        ] {
            let err = task_when_sql(condition, Dialect::PostgreSQL).unwrap_err();
            assert!(
                err.message().contains("requires one stream name string"),
                "{condition}: {err}"
            );
        }
    }

    #[test]
    fn test_string_literals_are_not_rewritten() {
        assert_eq!(
            task_when_sql("'STREAM_STATUS(''db.s'')'", Dialect::PostgreSQL).unwrap(),
            r"SELECT 'STREAM_STATUS(\'db.s\')'"
        );
        for dialect in [Dialect::PostgreSQL, Dialect::MySQL] {
            assert_eq!(
                task_when_sql("STREAM_STATUS('db.s''quoted')", dialect).unwrap(),
                r"SELECT (SELECT has_data FROM STREAM_STATUS('db.s\'quoted'))"
            );
            assert_eq!(
                task_when_sql(r#"STREAM_STATUS('"db"."s"')"#, dialect).unwrap(),
                r#"SELECT (SELECT has_data FROM STREAM_STATUS('"db"."s"'))"#
            );
        }
    }
}
