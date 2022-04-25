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

// Root node of a query tree
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    // Set operator: SELECT or UNION / EXCEPT / INTERSECT
    pub body: SetExpr,

    // The following clauses can only appear in top level of a subquery/query
    // `ORDER BY` clause
    pub order_by: Vec<OrderByExpr>,
    // `LIMIT` clause
    pub limit: Vec<Expr>,
    // `OFFSET` expr
    pub offset: Option<Expr>,
}

// A relational set expression, like `SELECT ... FROM ... {UNION|EXCEPT|INTERSECT} SELECT ... FROM ...`
#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Select(Box<SelectStmt>),
    Query(Box<Query>),
    // UNION/EXCEPT/INTERSECT operator
    SetOperation {
        op: SetOperator,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

// A subquery represented with `SELECT` statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct: bool,
    // Result set of current subquery
    pub select_list: Vec<SelectTarget>,
    // `FROM` clause, a list of table references.
    // The table references split by `,` will be joined with cross join,
    // and the result set is union of the joined tables by default.
    pub from: TableReference,
    // `WHERE` clause
    pub selection: Option<Expr>,
    // `GROUP BY` clause
    pub group_by: Vec<Expr>,
    // `HAVING` clause
    pub having: Option<Expr>,
}

// `ORDER BY` clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    // Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    // Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq)]
pub enum SelectTarget {
    // Expression with alias, e.g. `SELECT b AS a, a+1 AS b FROM t`
    AliasedExpr {
        expr: Expr,
        alias: Option<Identifier>,
    },

    // Qualified name, e.g. `SELECT t.a, t.* FROM t`.
    // For simplicity, wildcard is involved.
    QualifiedName(QualifiedName),
}

pub type QualifiedName = Vec<Indirection>;

// Indirection of a select result, like a part of `db.table.column`.
// Can be a database name, table name, field name or wildcard star(`*`).
#[derive(Debug, Clone, PartialEq)]
pub enum Indirection {
    // Field name
    Identifier(Identifier),
    // Wildcard star
    Star,
}

// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    // Table name
    Table {
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
        alias: Option<TableAlias>,
    },
    // Derived table, which can be a subquery or joined tables or combination of them
    Subquery {
        subquery: Box<Query>,
        alias: TableAlias,
    },
    // `TABLE(expr)[ AS alias ]`
    TableFunction {
        expr: Expr,
        alias: Option<TableAlias>,
    },
    Join(Join),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableAlias {
    pub name: Identifier,
    pub columns: Vec<Identifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub op: JoinOperator,
    pub condition: JoinCondition,
    pub left: Box<TableReference>,
    pub right: Box<TableReference>,
}

#[derive(Debug, Clone, PartialEq)]
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
pub enum JoinCondition {
    On(Expr),
    Using(Vec<Identifier>),
    Natural,
    None,
}

impl Display for OrderByExpr {
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

impl Display for TableAlias {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AS {}", &self.name)?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_period_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl Display for TableReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReference::Table {
                catalog,
                database,
                table,
                alias,
            } => {
                write_period_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(table)),
                )?;
                if let Some(alias) = alias {
                    write!(f, " {}", alias)?;
                }
            }
            TableReference::Subquery { subquery, alias } => {
                write!(f, "({})", subquery)?;
                write!(f, " {}", alias)?;
            }
            TableReference::TableFunction { expr, alias } => {
                write!(f, "{}", expr)?;
                if let Some(alias) = alias {
                    write!(f, " {}", alias)?;
                }
            }
            TableReference::Join(join) => {
                write!(f, "{} ", join.left)?;
                if join.condition == JoinCondition::Natural {
                    write!(f, "NATURAL ")?;
                }
                match join.op {
                    JoinOperator::Inner => {
                        write!(f, "INNER JOIN ")?;
                    }
                    JoinOperator::LeftOuter => {
                        write!(f, "LEFT OUTER JOIN ")?;
                    }
                    JoinOperator::RightOuter => {
                        write!(f, "RIGHT OUTER JOIN ")?;
                    }
                    JoinOperator::FullOuter => {
                        write!(f, "FULL OUTER JOIN ")?;
                    }
                    JoinOperator::CrossJoin => {
                        write!(f, "CROSS JOIN ")?;
                    }
                }
                write!(f, "{}", join.right)?;
                match &join.condition {
                    JoinCondition::On(expr) => {
                        write!(f, " ON {}", expr)?;
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

impl Display for Indirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Indirection::Identifier(ident) => {
                write!(f, "{}", ident)?;
            }
            Indirection::Star => {
                write!(f, "*")?;
            }
        }
        Ok(())
    }
}

impl Display for SelectTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectTarget::AliasedExpr { expr, alias } => {
                write!(f, "{}", expr)?;
                if let Some(ident) = alias {
                    write!(f, " AS {}", ident)?;
                }
            }
            SelectTarget::QualifiedName(indirections) => {
                write_period_separated_list(f, indirections)?;
            }
        }
        Ok(())
    }
}

impl Display for SelectStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // SELECT clause
        write!(f, "SELECT ")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        write_comma_separated_list(f, &self.select_list)?;

        // FROM clause
        write!(f, " FROM {}", self.from)?;

        // WHERE clause
        if let Some(expr) = &self.selection {
            write!(f, " WHERE ")?;
            write!(f, "{}", expr)?;
        }

        // GROUP BY clause
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            write_comma_separated_list(f, &self.group_by)?;
        }

        // HAVING clause
        if let Some(having) = &self.having {
            write!(f, " HAVING ")?;
            write!(f, "{}", having)?;
        }

        Ok(())
    }
}

impl Display for SetExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetExpr::Select(query) => {
                write!(f, "{}", query)?;
            }
            SetExpr::Query(query) => {
                write!(f, "({})", query)?;
            }
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                write!(f, "{} ", left)?;
                match op {
                    SetOperator::Union => {
                        write!(f, "UNION ")?;
                    }
                    SetOperator::Except => {
                        write!(f, "EXCEPT ")?;
                    }
                    SetOperator::Intersect => {
                        write!(f, "INTERSECT ")?;
                    }
                }
                if *all {
                    write!(f, "ALL ")?;
                }
                write!(f, "{}", right)?;
            }
        }
        Ok(())
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
            write!(f, " OFFSET {}", offset)?;
        }

        Ok(())
    }
}
