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

use nom::Parser;

use crate::ParseError;
use crate::Result;
use crate::ast::DatabaseRef;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Literal;
use crate::ast::ProcedureIdentity;
use crate::ast::Query;
use crate::ast::SelectTarget;
use crate::ast::SetExpr;
use crate::ast::Statement;
use crate::ast::StatementWithFormat;
use crate::ast::TableRef;
use crate::parser::Backtrace;
use crate::parser::common::IResult;
use crate::parser::common::comma_separated_list0;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::database_ref;
use crate::parser::common::ident;
use crate::parser::common::table_ref;
use crate::parser::common::transform_span;
use crate::parser::error::display_parser_error;
use crate::parser::expr::expr;
use crate::parser::expr::values;
use crate::parser::input::Dialect;
use crate::parser::input::Input;
use crate::parser::input::ParseMode;
use crate::parser::statement::insert_stmt;
use crate::parser::statement::procedure_type_name;
use crate::parser::statement::replace_stmt;
use crate::parser::statement::statement;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::visit::VisitControl;
use crate::visit::VisitorMut;
use crate::visit::WalkMut;

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token<'_>>> {
    Tokenizer::new(sql).collect::<Result<Vec<_>>>()
}

/// Parse a SQL string into `Statement`s.
#[fastrace::trace]
pub fn parse_sql(tokens: &[Token], dialect: Dialect) -> Result<(Statement, Option<String>)> {
    let stmt = run_parser(tokens, dialect, ParseMode::Default, false, statement)?;

    #[cfg(debug_assertions)]
    if let Err(message) = assert_reparse(tokens[0].source, stmt.clone()) {
        eprintln!("[WARN] {message}");
    }

    Ok((stmt.stmt, stmt.format))
}

/// Parse udf function into Expr
pub fn parse_expr(tokens: &[Token], dialect: Dialect) -> Result<Expr> {
    run_parser(tokens, dialect, ParseMode::Default, false, expr)
}

/// Parse a table reference string like "table", "db.table", or "catalog.db.table".
/// Correctly handles quoted identifiers like `"my.weird.table"`.
pub fn parse_table_ref(sql: &str, dialect: Dialect) -> Result<TableRef> {
    let tokens = tokenize_sql(sql)?;
    run_parser(&tokens, dialect, ParseMode::Default, false, table_ref)
}

/// Parse a database reference string like "db" or "catalog.db".
/// Correctly handles quoted identifiers.
pub fn parse_database_ref(sql: &str, dialect: Dialect) -> Result<DatabaseRef> {
    let tokens = tokenize_sql(sql)?;
    run_parser(&tokens, dialect, ParseMode::Default, false, database_ref)
}

/// Parse a procedure reference string like "my_proc(INT, STRING)" or "my_proc()".
/// Returns a `ProcedureIdentity` with the procedure name and argument types.
pub fn parse_procedure_ref(sql: &str, dialect: Dialect) -> Result<ProcedureIdentity> {
    let tokens = tokenize_sql(sql)?;
    run_parser(&tokens, dialect, ParseMode::Default, false, |i| {
        nom::combinator::map(
            nom::sequence::pair(ident, procedure_type_name),
            |(name, args_type): (Identifier, _)| ProcedureIdentity {
                name: name.to_string(),
                args_type,
            },
        )
        .parse(i)
    })
}

/// Parse a UDF name string (a single identifier).
/// Rejects trailing tokens to avoid silently ignoring malformed input.
pub fn parse_udf_ref(sql: &str, dialect: Dialect) -> Result<Identifier> {
    let tokens = tokenize_sql(sql)?;
    run_parser(&tokens, dialect, ParseMode::Default, false, ident)
}

pub fn parse_comma_separated_exprs(tokens: &[Token], dialect: Dialect) -> Result<Vec<Expr>> {
    run_parser(tokens, dialect, ParseMode::Default, true, |i| {
        comma_separated_list0(expr)(i)
    })
}

pub fn parse_comma_separated_idents(tokens: &[Token], dialect: Dialect) -> Result<Vec<Identifier>> {
    run_parser(tokens, dialect, ParseMode::Default, true, |i| {
        comma_separated_list1(ident).parse(i)
    })
}

pub fn parse_values(tokens: &[Token], dialect: Dialect) -> Result<Vec<Expr>> {
    run_parser(tokens, dialect, ParseMode::Default, false, values)
}

pub fn parse_cluster_key_exprs(cluster_key: &str) -> Result<Vec<Expr>> {
    // `cluster_key` is persisted in table metadata and may be created/rewritten under a
    // different session dialect. Parse it with a dialect that accepts both identifier
    // quote styles to keep ALTER behavior stable across sessions.
    let tokens = tokenize_sql(cluster_key)?;
    let mut ast_exprs = parse_comma_separated_exprs(&tokens, Dialect::default())?;
    // unwrap tuple.
    if ast_exprs.len() == 1
        && let Expr::Tuple { exprs, .. } = &ast_exprs[0]
    {
        ast_exprs = exprs.clone();
    }
    Ok(ast_exprs)
}

pub fn parse_raw_insert_stmt(
    tokens: &[Token],
    dialect: Dialect,
    in_streaming_load: bool,
) -> Result<Statement> {
    run_parser(
        tokens,
        dialect,
        ParseMode::Default,
        false,
        insert_stmt(true, in_streaming_load),
    )
}

pub fn parse_raw_replace_stmt(tokens: &[Token], dialect: Dialect) -> Result<Statement> {
    run_parser(
        tokens,
        dialect,
        ParseMode::Default,
        false,
        replace_stmt(true),
    )
}

pub fn run_parser<O>(
    tokens: &[Token],
    dialect: Dialect,
    mode: ParseMode,
    allow_partial: bool,
    mut parser: impl FnMut(Input) -> IResult<O>,
) -> Result<O> {
    let backtrace = Backtrace::new();
    let input = Input {
        tokens,
        dialect,
        mode,
        backtrace: &backtrace,
    };
    match parser(input) {
        Ok((rest, res)) => {
            let is_complete = rest[0].kind == TokenKind::EOI;
            if is_complete || allow_partial {
                Ok(res)
            } else {
                Err(ParseError(
                    transform_span(&rest[..1]),
                    format!(
                        "unable to parse rest of the sql, rest tokens:  {:?} ",
                        rest.tokens
                    ),
                ))
            }
        }
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = tokens[0].source;
            Err(ParseError(None, display_parser_error(err, source)))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

/// Check that the statement can be displayed and reparsed without loss
#[cfg(debug_assertions)]
fn assert_reparse(sql: &str, stmt: StatementWithFormat) -> std::result::Result<(), String> {
    let stmt = reset_ast(stmt);

    let normalized_sql = stmt.to_string();
    let new_tokens = crate::parser::tokenize_sql(&normalized_sql).map_err(|err| {
        format!(
            "failed to tokenize round-tripped SQL: {} in {}",
            err.1, normalized_sql
        )
    })?;
    let new_stmt = run_parser(
        &new_tokens,
        Dialect::PostgreSQL,
        ParseMode::Default,
        false,
        statement,
    )
    .map_err(|err| {
        format!(
            "failed to reparse round-tripped SQL: {} in {}",
            err.1, normalized_sql
        )
    })?;

    let reparsed_sql = reset_ast(new_stmt).to_string();
    if normalized_sql != reparsed_sql {
        return Err(format!(
            "statement changed after round-tripping.\noriginal:\n{}\nnormalized:\n{}\nreparsed:\n{}",
            sql, normalized_sql, reparsed_sql
        ));
    }

    Ok(())
}

#[cfg(debug_assertions)]
fn reset_ast(mut stmt: StatementWithFormat) -> StatementWithFormat {
    struct ResetAst;

    impl ResetAst {
        fn reset_select_targets(select_list: &mut [SelectTarget]) {
            for target in select_list {
                if let SelectTarget::StarColumns { column_filter, .. } = target {
                    *column_filter = None;
                }
            }
        }

        fn is_grouping_wrapper(query: &Query) -> bool {
            query.with.is_none()
                && query.order_by.is_empty()
                && query.limit.is_empty()
                && query.offset.is_none()
                && !query.ignore_result
        }
    }

    impl VisitorMut for ResetAst {
        fn visit_statement(
            &mut self,
            stmt: &mut Statement,
        ) -> std::result::Result<VisitControl, !> {
            if let Statement::CreateTag(stmt) = stmt
                && let Some(allowed_values) = &mut stmt.allowed_values
            {
                for value in allowed_values {
                    *value = Literal::Null;
                }
            }
            Ok(VisitControl::Continue)
        }

        fn visit_set_expr(
            &mut self,
            set_expr: &mut SetExpr,
        ) -> std::result::Result<VisitControl, !> {
            let collapsed = match set_expr {
                SetExpr::Query(query) if Self::is_grouping_wrapper(query) => {
                    Some(query.body.clone())
                }
                _ => None,
            };

            if let Some(body) = collapsed {
                *set_expr = body;
            }
            Ok(VisitControl::Continue)
        }

        fn visit_select_stmt(
            &mut self,
            select: &mut crate::ast::SelectStmt,
        ) -> std::result::Result<VisitControl, !> {
            Self::reset_select_targets(&mut select.select_list);
            Ok(VisitControl::Continue)
        }

        fn visit_expr(&mut self, expr: &mut Expr) -> std::result::Result<VisitControl, !> {
            match expr {
                Expr::Literal { value, .. } => {
                    *value = Literal::Null;
                }
                Expr::Map { kvs, .. } => {
                    for (key, _) in kvs {
                        *key = Literal::Null;
                    }
                }
                _ => {}
            }
            Ok(VisitControl::Continue)
        }
    }

    let _ = stmt.stmt.walk_mut(&mut ResetAst);

    stmt
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_statement_with_format(sql: &str) -> StatementWithFormat {
        let tokens = tokenize_sql(sql).unwrap();
        run_parser(
            &tokens,
            Dialect::PostgreSQL,
            ParseMode::Default,
            false,
            statement,
        )
        .unwrap()
    }

    #[test]
    fn test_reset_ast_normalizes_select_star_filters_and_literals() {
        let stmt = reset_ast(parse_statement_with_format(
            "SELECT * EXCLUDE (c1), 1, 'x' FROM customer",
        ));

        assert_eq!(stmt.to_string(), "SELECT *, NULL, NULL FROM customer");
    }

    #[test]
    fn test_reset_ast_normalizes_tag_allowed_values() {
        let stmt = reset_ast(parse_statement_with_format(
            "CREATE TAG IF NOT EXISTS tag_a ALLOWED_VALUES = ('dev', 'prod') COMMENT = 'environment tag'",
        ));

        assert_eq!(
            stmt.to_string(),
            "CREATE TAG IF NOT EXISTS tag_a ALLOWED_VALUES = (NULL, NULL) COMMENT = 'environment tag'"
        );
    }

    #[test]
    fn test_assert_reparse_uses_normalized_sql_fixpoint() {
        let sql = "SELECT * EXCLUDE (c1), 1 FROM customer";
        let stmt = parse_statement_with_format(sql);

        assert!(assert_reparse(sql, stmt).is_ok());
    }
}
