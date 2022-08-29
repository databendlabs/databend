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

use pretty::RcDoc;

use super::expr::pretty_expr;
use super::query::pretty_query;
use super::query::pretty_table;
use crate::ast::format::syntax::inline_comma;
use crate::ast::format::syntax::interweave_comma;
use crate::ast::format::syntax::parenthenized;
use crate::ast::format::syntax::NEST_FACTOR;
use crate::ast::CopyStmt;
use crate::ast::CopyUnit;
use crate::ast::Expr;
use crate::ast::InsertSource;
use crate::ast::InsertStmt;
use crate::ast::TableReference;

pub(crate) fn pretty_insert(insert_stmt: InsertStmt) -> RcDoc {
    RcDoc::text("INSERT")
        .append(RcDoc::space())
        .append(if insert_stmt.overwrite {
            RcDoc::text("OVERWRITE")
        } else {
            RcDoc::text("INTO")
        })
        .append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(if let Some(catalog) = insert_stmt.catalog {
                    RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(if let Some(database) = insert_stmt.database {
                    RcDoc::text(database.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(insert_stmt.table.to_string()))
                .append(if !insert_stmt.columns.is_empty() {
                    RcDoc::space()
                        .append(RcDoc::text("("))
                        .append(inline_comma(
                            insert_stmt
                                .columns
                                .into_iter()
                                .map(|ident| RcDoc::text(ident.to_string())),
                        ))
                        .append(RcDoc::text(")"))
                } else {
                    RcDoc::nil()
                }),
        )
        .append(pretty_source(insert_stmt.source))
}

fn pretty_source(source: InsertSource) -> RcDoc {
    RcDoc::line().append(match source {
        InsertSource::Streaming {
            format,
            rest_tokens,
        } => RcDoc::text("FORMAT")
            .append(RcDoc::space())
            .append(RcDoc::text(format))
            .append(
                RcDoc::line().nest(NEST_FACTOR).append(RcDoc::text(
                    (&rest_tokens[0].source[rest_tokens.first().unwrap().span.start
                        ..rest_tokens.last().unwrap().span.end])
                        .to_string(),
                )),
            ),
        InsertSource::Values { rest_tokens } => RcDoc::text("VALUES").append(
            RcDoc::line().nest(NEST_FACTOR).append(RcDoc::text(
                (&rest_tokens[0].source[rest_tokens.first().unwrap().span.start
                    ..rest_tokens.last().unwrap().span.end])
                    .to_string(),
            )),
        ),
        InsertSource::Select { query } => pretty_query(*query),
    })
}

pub(crate) fn pretty_delete<'a>(
    table: TableReference<'a>,
    selection: Option<Expr<'a>>,
) -> RcDoc<'a> {
    RcDoc::text("DELETE FROM")
        .append(RcDoc::line().nest(NEST_FACTOR).append(pretty_table(table)))
        .append(if let Some(selection) = selection {
            RcDoc::line().append(RcDoc::text("WHERE")).append(
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(pretty_expr(selection).nest(NEST_FACTOR).group()),
            )
        } else {
            RcDoc::nil()
        })
}

pub(crate) fn pretty_copy(copy_stmt: CopyStmt) -> RcDoc {
    RcDoc::text("COPY")
        .append(RcDoc::line().append(RcDoc::text("INTO ")))
        .append(pretty_copy_unit(copy_stmt.dst))
        .append(RcDoc::line().append(RcDoc::text("FROM ")))
        .append(pretty_copy_unit(copy_stmt.src))
        .append(if !copy_stmt.files.is_empty() {
            RcDoc::line()
                .append(RcDoc::text("FILES = "))
                .append(parenthenized(
                    interweave_comma(
                        copy_stmt
                            .files
                            .into_iter()
                            .map(|file| RcDoc::text(format!("{:?}", file))),
                    )
                    .group(),
                ))
        } else {
            RcDoc::nil()
        })
        .append(if !copy_stmt.pattern.is_empty() {
            RcDoc::line()
                .append(RcDoc::text("PATTERN = "))
                .append(RcDoc::text(format!("{:?}", copy_stmt.pattern)))
        } else {
            RcDoc::nil()
        })
        .append(if !copy_stmt.file_format.is_empty() {
            RcDoc::line()
                .append(RcDoc::text("FILE_FORMAT = "))
                .append(parenthenized(
                    interweave_comma(copy_stmt.file_format.iter().map(|(k, v)| {
                        RcDoc::text(k.to_string())
                            .append(RcDoc::space())
                            .append(RcDoc::text("="))
                            .append(RcDoc::space())
                            .append(RcDoc::text(format!("{:?}", v)))
                    }))
                    .group(),
                ))
        } else {
            RcDoc::nil()
        })
        .append(if !copy_stmt.validation_mode.is_empty() {
            RcDoc::line()
                .append(RcDoc::text("VALIDATION_MODE = "))
                .append(RcDoc::text(copy_stmt.validation_mode))
        } else {
            RcDoc::nil()
        })
        .append(if copy_stmt.size_limit != 0 {
            RcDoc::line()
                .append(RcDoc::text("SIZE_LIMIT = "))
                .append(RcDoc::text(format!("{}", copy_stmt.size_limit)))
        } else {
            RcDoc::nil()
        })
}

fn pretty_copy_unit(copy_unit: CopyUnit) -> RcDoc {
    match copy_unit {
        CopyUnit::Table {
            catalog,
            database,
            table,
        } => if let Some(catalog) = catalog {
            RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
        } else {
            RcDoc::nil()
        }
        .append(if let Some(database) = database {
            RcDoc::text(database.to_string()).append(RcDoc::text("."))
        } else {
            RcDoc::nil()
        })
        .append(RcDoc::text(table.to_string())),
        CopyUnit::StageLocation { name, path } => RcDoc::text("@")
            .append(RcDoc::text(name))
            .append(RcDoc::text(path)),
        CopyUnit::UriLocation(v) => RcDoc::text(v.to_string()),
        CopyUnit::Query(query) => RcDoc::text("(")
            .append(pretty_query(*query))
            .append(RcDoc::text(")")),
    }
}
