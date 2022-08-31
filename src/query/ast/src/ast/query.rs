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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::write_comma_separated_list;
use crate::ast::write_period_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::parser::token::Token;

/// Root node of a query tree
#[derive(Debug, Clone, PartialEq)]
pub struct Query<'a> {
    pub span: &'a [Token<'a>],
    // With clause, common table expression
    pub with: Option<With<'a>>,
    // Set operator: SELECT or UNION / EXCEPT / INTERSECT
    pub body: SetExpr<'a>,

    // The following clauses can only appear in top level of a subquery/query
    // `ORDER BY` clause
    pub order_by: Vec<OrderByExpr<'a>>,
    // `LIMIT` clause
    pub limit: Vec<Expr<'a>>,
    // `OFFSET` expr
    pub offset: Option<Expr<'a>>,
    // FORMAT <format>
    pub format: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct With<'a> {
    pub span: &'a [Token<'a>],
    pub recursive: bool,
    pub ctes: Vec<CTE<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CTE<'a> {
    pub span: &'a [Token<'a>],
    pub alias: TableAlias<'a>,
    pub query: Query<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation<'a> {
    pub span: &'a [Token<'a>],
    pub op: SetOperator,
    pub all: bool,
    pub left: Box<SetExpr<'a>>,
    pub right: Box<SetExpr<'a>>,
}

/// A subquery represented with `SELECT` statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt<'a> {
    pub span: &'a [Token<'a>],
    pub distinct: bool,
    // Result set of current subquery
    pub select_list: Vec<SelectTarget<'a>>,
    // `FROM` clause, a list of table references.
    // The table references split by `,` will be joined with cross join,
    // and the result set is union of the joined tables by default.
    pub from: Vec<TableReference<'a>>,
    // `WHERE` clause
    pub selection: Option<Expr<'a>>,
    // `GROUP BY` clause
    pub group_by: Vec<Expr<'a>>,
    // `HAVING` clause
    pub having: Option<Expr<'a>>,
}

/// A relational set expression, like `SELECT ... FROM ... {UNION|EXCEPT|INTERSECT} SELECT ... FROM ...`
#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr<'a> {
    Select(Box<SelectStmt<'a>>),
    Query(Box<Query<'a>>),
    // UNION/EXCEPT/INTERSECT operator
    SetOperation(Box<SetOperation<'a>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

/// `ORDER BY` clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr<'a> {
    pub expr: Expr<'a>,
    // Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    // Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq)]
pub enum SelectTarget<'a> {
    // Expression with alias, e.g. `SELECT b AS a, a+1 AS b FROM t`
    AliasedExpr {
        expr: Box<Expr<'a>>,
        alias: Option<Identifier<'a>>,
    },

    // Qualified name, e.g. `SELECT t.a, t.* FROM t`.
    // For simplicity, wildcard is involved.
    QualifiedName(QualifiedName<'a>),
}

pub type QualifiedName<'a> = Vec<Indirection<'a>>;

/// Indirection of a select result, like a part of `db.table.column`.
/// Can be a database name, table name, field name or wildcard star(`*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Indirection<'a> {
    // Field name
    Identifier(Identifier<'a>),
    // Wildcard star
    Star,
}

/// Time Travel specification
#[derive(Debug, Clone, PartialEq)]
pub enum TimeTravelPoint<'a> {
    Snapshot(String),
    Timestamp(Box<Expr<'a>>),
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference<'a> {
    // Table name
    Table {
        span: &'a [Token<'a>],
        catalog: Option<Identifier<'a>>,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
        alias: Option<TableAlias<'a>>,
        travel_point: Option<TimeTravelPoint<'a>>,
    },
    // `TABLE(expr)[ AS alias ]`
    TableFunction {
        span: &'a [Token<'a>],
        name: Identifier<'a>,
        params: Vec<Expr<'a>>,
        alias: Option<TableAlias<'a>>,
    },
    // Derived table, which can be a subquery or joined tables or combination of them
    Subquery {
        span: &'a [Token<'a>],
        subquery: Box<Query<'a>>,
        alias: Option<TableAlias<'a>>,
    },
    Join {
        span: &'a [Token<'a>],
        join: Join<'a>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAlias<'a> {
    pub name: Identifier<'a>,
    pub columns: Vec<Identifier<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join<'a> {
    pub op: JoinOperator,
    pub condition: JoinCondition<'a>,
    pub left: Box<TableReference<'a>>,
    pub right: Box<TableReference<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinOperator {
    Inner,
    // Outer joins can not work with `JoinCondition::None`
    LeftOuter,
    RightOuter,
    FullOuter,
    // CrossJoin can only work with `JoinCondition::None`
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition<'a> {
    On(Box<Expr<'a>>),
    Using(Vec<Identifier<'a>>),
    Natural,
    None,
}

impl<'a> Display for OrderByExpr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(asc) = self.asc {
            if asc {
                write!(f, " ASC")?;
            } else {
                write!(f, " DESC")?;
            }
        }
        if let Some(nulls_first) = self.nulls_first {
            if nulls_first {
                write!(f, " NULLS FIRST")?;
            } else {
                write!(f, " NULLS LAST")?;
            }
        }
        Ok(())
    }
}

impl<'a> Display for TableAlias<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name)?;
        if !self.columns.is_empty() {
            write!(f, "(")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl<'a> Display for TableReference<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReference::Table {
                span: _,
                catalog,
                database,
                table,
                alias,
                travel_point,
            } => {
                write_period_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(table)),
                )?;

                if let Some(TimeTravelPoint::Snapshot(sid)) = travel_point {
                    write!(f, " AT (SNAPSHOT => {sid})")?;
                }

                if let Some(TimeTravelPoint::Timestamp(ts)) = travel_point {
                    write!(f, " AT (TIMESTAMP => {ts})")?;
                }

                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
            }
            TableReference::TableFunction {
                span: _,
                name,
                params,
                alias,
            } => {
                write!(f, "{name}(")?;
                write_comma_separated_list(f, params)?;
                write!(f, ")")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
            }
            TableReference::Subquery {
                span: _,
                subquery,
                alias,
            } => {
                write!(f, "({subquery})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
            }
            TableReference::Join { span: _, join } => {
                write!(f, "{}", join.left)?;
                if join.condition == JoinCondition::Natural {
                    write!(f, " NATURAL")?;
                }
                match join.op {
                    JoinOperator::Inner => {
                        write!(f, " INNER JOIN")?;
                    }
                    JoinOperator::LeftOuter => {
                        write!(f, " LEFT OUTER JOIN")?;
                    }
                    JoinOperator::RightOuter => {
                        write!(f, " RIGHT OUTER JOIN")?;
                    }
                    JoinOperator::FullOuter => {
                        write!(f, " FULL OUTER JOIN")?;
                    }
                    JoinOperator::CrossJoin => {
                        write!(f, " CROSS JOIN")?;
                    }
                }
                write!(f, " {}", join.right)?;
                match &join.condition {
                    JoinCondition::On(expr) => {
                        write!(f, " ON {expr}")?;
                    }
                    JoinCondition::Using(idents) => {
                        write!(f, " USING(")?;
                        write_comma_separated_list(f, idents)?;
                        write!(f, ")")?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

impl<'a> Display for Indirection<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Indirection::Identifier(ident) => {
                write!(f, "{ident}")?;
            }
            Indirection::Star => {
                write!(f, "*")?;
            }
        }
        Ok(())
    }
}

impl<'a> Display for SelectTarget<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectTarget::AliasedExpr { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(ident) = alias {
                    write!(f, " AS {ident}")?;
                }
            }
            SelectTarget::QualifiedName(indirections) => {
                write_period_separated_list(f, indirections)?;
            }
        }
        Ok(())
    }
}

impl<'a> Display for SelectStmt<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // SELECT clause
        write!(f, "SELECT ")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        write_comma_separated_list(f, &self.select_list)?;

        // FROM clause
        if !self.from.is_empty() {
            write!(f, " FROM ")?;
            write_comma_separated_list(f, &self.from)?;
        }

        // WHERE clause
        if let Some(expr) = &self.selection {
            write!(f, " WHERE {expr}")?;
        }

        // GROUP BY clause
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            write_comma_separated_list(f, &self.group_by)?;
        }

        // HAVING clause
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }

        Ok(())
    }
}

impl<'a> Display for SetExpr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetExpr::Select(select_stmt) => {
                write!(f, "{select_stmt}")?;
            }
            SetExpr::Query(query) => {
                write!(f, "({query})")?;
            }
            SetExpr::SetOperation(set_operation) => {
                write!(f, "{}", set_operation.left)?;
                match set_operation.op {
                    SetOperator::Union => {
                        write!(f, " UNION ")?;
                    }
                    SetOperator::Except => {
                        write!(f, " EXCEPT")?;
                    }
                    SetOperator::Intersect => {
                        write!(f, " INTERSECT")?;
                    }
                }
                if set_operation.all {
                    write!(f, " ALL")?;
                }
                write!(f, "{}", set_operation.right)?;
            }
        }
        Ok(())
    }
}

impl<'a> Display for CTE<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AS ({})", self.alias, self.query)?;
        Ok(())
    }
}
impl<'a> Display for With<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.recursive {
            write!(f, "RECURSIVE ")?;
        }

        write_comma_separated_list(f, &self.ctes)?;
        Ok(())
    }
}

impl<'a> Display for Query<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // CTE, with clause
        if let Some(with) = &self.with {
            write!(f, "WITH {with} ")?;
        }

        // Query body
        write!(f, "{}", self.body)?;

        // ORDER BY clause
        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            write_comma_separated_list(f, &self.order_by)?;
        }

        // LIMIT clause
        if !self.limit.is_empty() {
            write!(f, " LIMIT ")?;
            write_comma_separated_list(f, &self.limit)?;
        }

        // TODO: We should validate if offset exists, limit should be empty or just one element
        if let Some(offset) = &self.offset {
            write!(f, " OFFSET {offset}")?;
        }

        if let Some(format) = &self.format {
            write!(f, " FORMAT {format}")?;
        }
        Ok(())
    }
}
