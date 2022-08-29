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
use crate::ast::format::syntax::interweave_comma;
use crate::ast::format::syntax::parenthenized;
use crate::ast::format::syntax::NEST_FACTOR;
use crate::ast::AlterTableAction;
use crate::ast::AlterTableStmt;
use crate::ast::AlterViewStmt;
use crate::ast::CreateTableSource;
use crate::ast::CreateTableStmt;
use crate::ast::CreateViewStmt;

pub(crate) fn pretty_create_table(stmt: CreateTableStmt) -> RcDoc {
    RcDoc::text("CREATE")
        .append(if stmt.transient {
            RcDoc::space().append(RcDoc::text("TRANSIENT"))
        } else {
            RcDoc::nil()
        })
        .append(RcDoc::space().append(RcDoc::text("TABLE")))
        .append(if stmt.if_not_exists {
            RcDoc::space().append(RcDoc::text("IF NOT EXISTS"))
        } else {
            RcDoc::nil()
        })
        .append(
            RcDoc::space()
                .append(if let Some(catalog) = stmt.catalog {
                    RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(if let Some(database) = stmt.database {
                    RcDoc::text(database.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(stmt.table.to_string())),
        )
        .append(if let Some(source) = stmt.source {
            pretty_table_source(source)
        } else {
            RcDoc::nil()
        })
        .append(if let Some(engine) = stmt.engine {
            RcDoc::space()
                .append(RcDoc::text("ENGINE ="))
                .append(RcDoc::space())
                .append(engine.to_string())
        } else {
            RcDoc::nil()
        })
        .append(if !stmt.cluster_by.is_empty() {
            RcDoc::line()
                .append(RcDoc::text("CLUSTER BY "))
                .append(parenthenized(
                    interweave_comma(stmt.cluster_by.into_iter().map(pretty_expr)).group(),
                ))
        } else {
            RcDoc::nil()
        })
        .append(if !stmt.table_options.is_empty() {
            RcDoc::line()
                .append(interweave_comma(stmt.table_options.iter().map(|(k, v)| {
                    RcDoc::text(k.clone())
                        .append(RcDoc::space())
                        .append(RcDoc::text("="))
                        .append(RcDoc::space())
                        .append(RcDoc::text("'"))
                        .append(RcDoc::text(v.clone()))
                        .append(RcDoc::text("'"))
                })))
                .group()
        } else {
            RcDoc::nil()
        })
        .append(if let Some(as_query) = stmt.as_query {
            RcDoc::line().append(RcDoc::text("AS")).append(
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(pretty_query(*as_query).nest(NEST_FACTOR).group()),
            )
        } else {
            RcDoc::nil()
        })
}

fn pretty_table_source(source: CreateTableSource) -> RcDoc {
    match source {
        CreateTableSource::Columns(columns) => RcDoc::space().append(parenthenized(
            interweave_comma(
                columns
                    .into_iter()
                    .map(|column| RcDoc::text(column.to_string())),
            )
            .group(),
        )),
        CreateTableSource::Like {
            catalog,
            database,
            table,
        } => RcDoc::space()
            .append(RcDoc::text("LIKE"))
            .append(RcDoc::space())
            .append(if let Some(catalog) = catalog {
                RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
            } else {
                RcDoc::nil()
            })
            .append(if let Some(database) = database {
                RcDoc::text(database.to_string()).append(RcDoc::text("."))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::text(table.to_string())),
    }
}

pub(crate) fn pretty_alter_table(stmt: AlterTableStmt) -> RcDoc {
    RcDoc::text("ALTER TABLE")
        .append(if stmt.if_exists {
            RcDoc::space().append(RcDoc::text("IF EXISTS"))
        } else {
            RcDoc::nil()
        })
        .append(
            RcDoc::space()
                .append(if let Some(catalog) = stmt.catalog {
                    RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(if let Some(database) = stmt.database {
                    RcDoc::text(database.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(stmt.table.to_string())),
        )
        .append(pretty_alter_table_action(stmt.action))
}

pub(crate) fn pretty_alter_table_action(action: AlterTableAction) -> RcDoc {
    match action {
        AlterTableAction::RenameTable { new_table } => RcDoc::line()
            .append(RcDoc::text("RENAME TO "))
            .append(RcDoc::text(new_table.to_string())),
        AlterTableAction::AlterTableClusterKey { cluster_by } => RcDoc::line()
            .append(RcDoc::text("CLUSTER BY "))
            .append(parenthenized(
                interweave_comma(cluster_by.into_iter().map(pretty_expr)).group(),
            )),
        AlterTableAction::DropTableClusterKey => {
            RcDoc::line().append(RcDoc::text("DROP CLUSTER KEY"))
        }
    }
}

pub(crate) fn pretty_create_view(stmt: CreateViewStmt) -> RcDoc {
    RcDoc::text("CREATE VIEW")
        .append(if stmt.if_not_exists {
            RcDoc::space().append(RcDoc::text("IF NOT EXISTS"))
        } else {
            RcDoc::nil()
        })
        .append(
            RcDoc::space()
                .append(if let Some(catalog) = stmt.catalog {
                    RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(if let Some(database) = stmt.database {
                    RcDoc::text(database.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(stmt.view.to_string())),
        )
        .append(
            RcDoc::line().append(RcDoc::text("AS")).append(
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(pretty_query(*stmt.query).nest(NEST_FACTOR).group()),
            ),
        )
}

pub(crate) fn pretty_alter_view(stmt: AlterViewStmt) -> RcDoc {
    RcDoc::text("ALTER VIEW")
        .append(
            RcDoc::space()
                .append(if let Some(catalog) = stmt.catalog {
                    RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(if let Some(database) = stmt.database {
                    RcDoc::text(database.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(stmt.view.to_string())),
        )
        .append(
            RcDoc::line().append(RcDoc::text("AS")).append(
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(pretty_query(*stmt.query).nest(NEST_FACTOR).group()),
            ),
        )
}
