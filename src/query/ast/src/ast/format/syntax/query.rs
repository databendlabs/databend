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
use crate::ast::format::syntax::inline_comma;
use crate::ast::format::syntax::inline_dot;
use crate::ast::format::syntax::interweave_comma;
use crate::ast::format::syntax::parenthenized;
use crate::ast::format::syntax::NEST_FACTOR;
use crate::ast::Expr;
use crate::ast::JoinCondition;
use crate::ast::JoinOperator;
use crate::ast::OrderByExpr;
use crate::ast::Query;
use crate::ast::SelectTarget;
use crate::ast::SetExpr;
use crate::ast::SetOperator;
use crate::ast::TableReference;
use crate::ast::TimeTravelPoint;
use crate::ast::With;
use crate::ast::CTE;

pub(crate) fn pretty_query(query: Query) -> RcDoc {
    pretty_with(query.with)
        .append(pretty_body(query.body))
        .append(pretty_order_by(query.order_by))
        .append(pretty_limit(query.limit))
        .append(pretty_offset(query.offset))
        .append(pretty_format(query.format))
        .group()
}

fn pretty_with(with: Option<With>) -> RcDoc {
    if let Some(with) = with {
        RcDoc::text("WITH")
            .append(if with.recursive {
                RcDoc::space().append(RcDoc::text("RECURSIVE"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::line().nest(NEST_FACTOR))
            .append(
                interweave_comma(with.ctes.into_iter().map(pretty_cte))
                    .nest(NEST_FACTOR)
                    .group(),
            )
            .append(RcDoc::line())
    } else {
        RcDoc::nil()
    }
}

fn pretty_cte(cte: CTE) -> RcDoc {
    RcDoc::text(format!("{} AS", cte.alias))
        .append(RcDoc::softline())
        .append(parenthenized(pretty_query(cte.query)))
}

fn pretty_body(body: SetExpr) -> RcDoc {
    match body {
        SetExpr::Select(select_stmt) => if select_stmt.distinct {
            RcDoc::text("SELECT DISTINCT")
        } else {
            RcDoc::text("SELECT")
        }
        .append(pretty_select_list(select_stmt.select_list))
        .append(pretty_from(select_stmt.from))
        .append(pretty_selection(select_stmt.selection))
        .append(pretty_group_by(select_stmt.group_by))
        .append(pretty_having(select_stmt.having)),
        SetExpr::Query(query) => parenthenized(pretty_query(*query)),
        SetExpr::SetOperation(set_operation) => pretty_body(*set_operation.left)
            .append(
                RcDoc::line()
                    .append(match set_operation.op {
                        SetOperator::Union => RcDoc::text("UNION"),
                        SetOperator::Except => RcDoc::text("EXCEPT"),
                        SetOperator::Intersect => RcDoc::text("INTERSECT"),
                    })
                    .append(if set_operation.all {
                        RcDoc::space().append(RcDoc::text("ALL"))
                    } else {
                        RcDoc::nil()
                    }),
            )
            .append(RcDoc::line())
            .append(pretty_body(*set_operation.right)),
    }
}

fn pretty_select_list(select_list: Vec<SelectTarget>) -> RcDoc {
    if select_list.len() > 1 {
        RcDoc::line()
    } else {
        RcDoc::space()
    }
    .nest(NEST_FACTOR)
    .append(
        interweave_comma(select_list.into_iter().map(|select_target| {
            match select_target {
                SelectTarget::AliasedExpr { expr, alias } => {
                    pretty_expr(*expr).append(if let Some(alias) = alias {
                        RcDoc::space()
                            .append(RcDoc::text("AS"))
                            .append(RcDoc::space())
                            .append(RcDoc::text(alias.to_string()))
                    } else {
                        RcDoc::nil()
                    })
                }
                SelectTarget::QualifiedName(object_name) => inline_dot(
                    object_name
                        .into_iter()
                        .map(|indirection| RcDoc::text(indirection.to_string())),
                )
                .group(),
            }
        }))
        .nest(NEST_FACTOR)
        .group(),
    )
}

fn pretty_from(from: Vec<TableReference>) -> RcDoc {
    if !from.is_empty() {
        RcDoc::line()
            .append(RcDoc::text("FROM").append(RcDoc::line().nest(NEST_FACTOR)))
            .append(
                interweave_comma(from.into_iter().map(pretty_table))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_selection(selection: Option<Expr>) -> RcDoc {
    if let Some(selection) = selection {
        RcDoc::line().append(RcDoc::text("WHERE")).append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(pretty_expr(selection).nest(NEST_FACTOR).group()),
        )
    } else {
        RcDoc::nil()
    }
}

fn pretty_group_by(group_by: Vec<Expr>) -> RcDoc {
    if !group_by.is_empty() {
        RcDoc::line()
            .append(
                RcDoc::text("GROUP BY").append(
                    if group_by.len() > 1 {
                        RcDoc::line()
                    } else {
                        RcDoc::space()
                    }
                    .nest(NEST_FACTOR),
                ),
            )
            .append(
                interweave_comma(group_by.into_iter().map(pretty_expr))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_having(having: Option<Expr>) -> RcDoc {
    if let Some(having) = having {
        RcDoc::line()
            .append(RcDoc::text("HAVING").append(RcDoc::line().nest(NEST_FACTOR)))
            .append(pretty_expr(having))
    } else {
        RcDoc::nil()
    }
}

pub(crate) fn pretty_table(table: TableReference) -> RcDoc {
    match table {
        TableReference::Table {
            span: _,
            catalog,
            database,
            table,
            alias,
            travel_point,
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
        .append(RcDoc::text(table.to_string()))
        .append(if let Some(TimeTravelPoint::Snapshot(sid)) = travel_point {
            RcDoc::text(format!(" AT (SNAPSHOT => {sid})"))
        } else if let Some(TimeTravelPoint::Timestamp(ts)) = travel_point {
            RcDoc::text(format!(" AT (TIMESTAMP => {ts})"))
        } else {
            RcDoc::nil()
        })
        .append(if let Some(alias) = alias {
            RcDoc::text(format!(" AS {alias}"))
        } else {
            RcDoc::nil()
        }),
        TableReference::Subquery {
            span: _,
            subquery,
            alias,
        } => parenthenized(pretty_query(*subquery)).append(if let Some(alias) = alias {
            RcDoc::text(format!(" AS {alias}"))
        } else {
            RcDoc::nil()
        }),
        TableReference::TableFunction {
            span: _,
            name,
            params,
            alias,
        } => RcDoc::text(name.to_string())
            .append(RcDoc::text("("))
            .append(inline_comma(params.into_iter().map(pretty_expr)))
            .append(RcDoc::text(")"))
            .append(if let Some(alias) = alias {
                RcDoc::text(format!(" AS {alias}"))
            } else {
                RcDoc::nil()
            }),
        TableReference::Join { span: _, join } => pretty_table(*join.left)
            .append(RcDoc::line())
            .append(if join.condition == JoinCondition::Natural {
                RcDoc::text("NATURAL").append(RcDoc::space())
            } else {
                RcDoc::nil()
            })
            .append(match join.op {
                JoinOperator::Inner => RcDoc::text("INNER JOIN"),
                JoinOperator::LeftOuter => RcDoc::text("LEFT OUTER JOIN"),
                JoinOperator::RightOuter => RcDoc::text("RIGHT OUTER JOIN"),
                JoinOperator::FullOuter => RcDoc::text("FULL OUTER JOIN"),
                JoinOperator::CrossJoin => RcDoc::text("CROSS JOIN"),
            })
            .append(RcDoc::space().append(pretty_table(*join.right)))
            .append(match &join.condition {
                JoinCondition::On(expr) => RcDoc::space()
                    .append(RcDoc::text("ON"))
                    .append(RcDoc::space())
                    .append(pretty_expr(*expr.clone())),
                JoinCondition::Using(idents) => RcDoc::space()
                    .append(RcDoc::text("USING("))
                    .append(inline_comma(
                        idents.iter().map(|ident| RcDoc::text(ident.to_string())),
                    ))
                    .append(RcDoc::text(")")),
                _ => RcDoc::nil(),
            }),
    }
}

fn pretty_order_by(order_by: Vec<OrderByExpr>) -> RcDoc {
    if !order_by.is_empty() {
        RcDoc::line()
            .append(
                RcDoc::text("ORDER BY").append(
                    if order_by.len() > 1 {
                        RcDoc::line()
                    } else {
                        RcDoc::space()
                    }
                    .nest(NEST_FACTOR),
                ),
            )
            .append(
                interweave_comma(order_by.into_iter().map(pretty_order_by_expr))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_limit(limit: Vec<Expr>) -> RcDoc {
    if !limit.is_empty() {
        RcDoc::line()
            .append(
                RcDoc::text("LIMIT").append(
                    if limit.len() > 1 {
                        RcDoc::line()
                    } else {
                        RcDoc::space()
                    }
                    .nest(NEST_FACTOR),
                ),
            )
            .append(
                interweave_comma(limit.into_iter().map(pretty_expr))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_offset(offset: Option<Expr>) -> RcDoc {
    if let Some(offset) = offset {
        RcDoc::line()
            .append(RcDoc::text("OFFSET").append(RcDoc::space().nest(NEST_FACTOR)))
            .append(pretty_expr(offset))
    } else {
        RcDoc::nil()
    }
}

fn pretty_format<'a>(format: Option<String>) -> RcDoc<'a> {
    if let Some(format) = format {
        RcDoc::line()
            .append(RcDoc::text("FORMAT").append(RcDoc::space().nest(NEST_FACTOR)))
            .append(RcDoc::text(format))
    } else {
        RcDoc::nil()
    }
}

fn pretty_order_by_expr(order_by_expr: OrderByExpr) -> RcDoc {
    RcDoc::text(order_by_expr.expr.to_string())
        .append(if let Some(asc) = order_by_expr.asc {
            if asc {
                RcDoc::space().append(RcDoc::text("ASC"))
            } else {
                RcDoc::space().append(RcDoc::text("DESC"))
            }
        } else {
            RcDoc::nil()
        })
        .append(if let Some(nulls_first) = order_by_expr.nulls_first {
            if nulls_first {
                RcDoc::space().append(RcDoc::text("NULLS FIRST"))
            } else {
                RcDoc::space().append(RcDoc::text("NULLS LAST"))
            }
        } else {
            RcDoc::nil()
        })
}
