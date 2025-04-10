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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use educe::Educe;

use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::ast::write_comma_separated_string_map;
use crate::ast::write_dot_separated_list;
use crate::ast::Expr;
use crate::ast::FileLocation;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::Lambda;
use crate::ast::SelectStageOptions;
use crate::ast::WindowDefinition;
use crate::ParseError;
use crate::Result;
use crate::Span;

/// Root node of a query tree
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Query {
    pub span: Span,
    // With clause, common table expression
    pub with: Option<With>,
    // Set operator: SELECT or UNION / EXCEPT / INTERSECT
    pub body: SetExpr,

    // The following clauses can only appear in top level of a subquery/query
    // `ORDER BY` clause
    pub order_by: Vec<OrderByExpr>,
    // `LIMIT` clause
    pub limit: Vec<Expr>,
    // `OFFSET` expr
    pub offset: Option<Expr>,

    // If ignore the result (not output).
    pub ignore_result: bool,
}

impl Display for Query {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

        if self.ignore_result {
            write!(f, " IGNORE_RESULT")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct With {
    pub span: Span,
    pub recursive: bool,
    pub ctes: Vec<CTE>,
}

impl Display for With {
    #[recursive::recursive]
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.recursive {
            write!(f, "RECURSIVE ")?;
        }

        write_comma_separated_list(f, &self.ctes)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CTE {
    pub span: Span,
    pub alias: TableAlias,
    pub materialized: bool,
    pub query: Box<Query>,
}

impl Display for CTE {
    #[recursive::recursive]
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} AS ", self.alias)?;
        if self.materialized {
            write!(f, "MATERIALIZED ")?;
        }
        write!(f, "({})", self.query)?;
        Ok(())
    }
}

#[derive(Educe, Drive, DriveMut)]
#[educe(
    PartialEq(bound = false, attrs = "#[recursive::recursive]"),
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
pub struct SetOperation {
    pub span: Span,
    pub op: SetOperator,
    pub all: bool,
    pub left: Box<SetExpr>,
    pub right: Box<SetExpr>,
}

/// A subquery represented with `SELECT` statement
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct SelectStmt {
    pub span: Span,
    pub hints: Option<Hint>,
    pub distinct: bool,
    pub top_n: Option<u64>,
    // Result set of current subquery
    pub select_list: Vec<SelectTarget>,
    // `FROM` clause, a list of table references.
    // The table references split by `,` will be joined with cross join,
    // and the result set is union of the joined tables by default.
    pub from: Vec<TableReference>,
    // `WHERE` clause
    pub selection: Option<Expr>,
    // `GROUP BY` clause
    pub group_by: Option<GroupBy>,
    // `HAVING` clause
    pub having: Option<Expr>,
    // `WINDOW` clause
    pub window_list: Option<Vec<WindowDefinition>>,
    // `QUALIFY` clause
    pub qualify: Option<Expr>,
}

impl Display for SelectStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // SELECT clause
        write!(f, "SELECT ")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        if let Some(topn) = &self.top_n {
            write!(f, "TOP {} ", topn)?;
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
        if self.group_by.is_some() {
            write!(f, " GROUP BY ")?;
            write!(f, "{}", self.group_by.as_ref().unwrap())?;
        }
        // HAVING clause
        if let Some(having) = &self.having {
            write!(f, " HAVING {having}")?;
        }

        // WINDOW clause
        if let Some(windows) = &self.window_list {
            write!(f, " WINDOW ")?;
            write_comma_separated_list(f, windows)?;
        }

        if let Some(qualify) = &self.qualify {
            write!(f, " QUALIFY {qualify}")?;
        }
        Ok(())
    }
}

/// Group by Clause.
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum GroupBy {
    /// GROUP BY expr [, expr]*
    Normal(Vec<Expr>),
    /// GROUP By ALL
    All,
    /// GROUP BY GROUPING SETS ( GroupSet [, GroupSet]* )
    ///
    /// GroupSet := (expr [, expr]*) | expr
    GroupingSets(Vec<Vec<Expr>>),
    /// GROUP BY CUBE ( expr [, expr]* )
    Cube(Vec<Expr>),
    /// GROUP BY ROLLUP ( expr [, expr]* )
    Rollup(Vec<Expr>),
    Combined(Vec<GroupBy>),
}

impl GroupBy {
    pub fn normal_items(&self) -> Vec<Expr> {
        match self {
            GroupBy::Normal(exprs) => exprs.clone(),
            _ => Vec::new(),
        }
    }
}

impl Display for GroupBy {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            GroupBy::Normal(exprs) => {
                write_comma_separated_list(f, exprs)?;
            }
            GroupBy::All => {
                write!(f, "ALL")?;
            }
            GroupBy::GroupingSets(sets) => {
                write!(f, "GROUPING SETS (")?;
                for (i, set) in sets.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    write_comma_separated_list(f, set)?;
                    write!(f, ")")?;
                }
                write!(f, ")")?;
            }
            GroupBy::Cube(exprs) => {
                write!(f, "CUBE (")?;
                write_comma_separated_list(f, exprs)?;
                write!(f, ")")?;
            }
            GroupBy::Rollup(exprs) => {
                write!(f, "ROLLUP (")?;
                write_comma_separated_list(f, exprs)?;
                write!(f, ")")?;
            }
            GroupBy::Combined(group_bys) => {
                for (i, group_by) in group_bys.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", group_by)?;
                }
            }
        }
        Ok(())
    }
}

/// A relational set expression, like `SELECT ... FROM ... {UNION|EXCEPT|INTERSECT} SELECT ... FROM ...`
#[derive(Educe, Drive, DriveMut)]
#[educe(
    PartialEq(bound = false, attrs = "#[recursive::recursive]"),
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
pub enum SetExpr {
    Select(Box<SelectStmt>),
    Query(Box<Query>),
    // UNION/EXCEPT/INTERSECT operator
    SetOperation(Box<SetOperation>),
    // Values clause
    Values { span: Span, values: Vec<Vec<Expr>> },
}

impl Display for SetExpr {
    #[recursive::recursive]
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SetExpr::Select(select_stmt) => {
                write!(f, "{select_stmt}")?;
            }
            SetExpr::Query(query) => {
                write!(f, "({query})")?;
            }
            SetExpr::SetOperation(set_operation) => {
                // Check if the left or right expressions are also SetOperations
                let left_needs_parentheses = matches!(set_operation.left.as_ref(), SetExpr::SetOperation(left_op) if left_op.op < set_operation.op);
                let right_needs_parentheses = matches!(set_operation.right.as_ref(), SetExpr::SetOperation(right_op) if right_op.op < set_operation.op);

                if left_needs_parentheses {
                    write!(f, "(")?;
                }
                write!(f, "{}", set_operation.left)?;
                if left_needs_parentheses {
                    write!(f, ")")?;
                }

                match set_operation.op {
                    SetOperator::Union => {
                        write!(f, " UNION ")?;
                    }
                    SetOperator::Except => {
                        write!(f, " EXCEPT ")?;
                    }
                    SetOperator::Intersect => {
                        write!(f, " INTERSECT ")?;
                    }
                }
                if set_operation.all {
                    write!(f, "ALL ")?;
                }
                // Add parentheses if necessary
                if right_needs_parentheses {
                    write!(f, "(")?;
                }
                write!(f, "{}", set_operation.right)?;
                if right_needs_parentheses {
                    write!(f, ")")?;
                }
            }
            SetExpr::Values { values, .. } => {
                write!(f, "VALUES")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    write_comma_separated_list(f, value)?;
                    write!(f, ")")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl PartialOrd for SetOperator {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            Some(std::cmp::Ordering::Equal)
        } else if self == &SetOperator::Intersect {
            Some(std::cmp::Ordering::Greater)
        } else {
            Some(std::cmp::Ordering::Less)
        }
    }
}

/// `ORDER BY` clause
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct OrderByExpr {
    pub expr: Expr,
    /// `ASC` or `DESC`
    pub asc: Option<bool>,
    /// `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
}

impl Display for OrderByExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum SelectTarget {
    /// Expression with alias, e.g. `SELECT t.a, b AS a, a+1 AS b FROM t`
    AliasedExpr {
        expr: Box<Expr>,
        alias: Option<Identifier>,
    },

    /// Qualified star name, e.g. `SELECT t.*  exclude a, columns(expr) FROM t`.
    /// Columns("pattern_str")
    /// Columns(lambda expression)
    /// For simplicity, star wildcard is involved.
    StarColumns {
        qualified: QualifiedName,
        column_filter: Option<ColumnFilter>,
    },
}

impl SelectTarget {
    pub fn is_star(&self) -> bool {
        match self {
            SelectTarget::AliasedExpr { .. } => false,
            SelectTarget::StarColumns { qualified, .. } => {
                matches!(qualified.last(), Some(Indirection::Star(_)))
            }
        }
    }

    pub fn exclude(&mut self, exclude: Vec<Identifier>) {
        match self {
            SelectTarget::AliasedExpr { .. } => unreachable!(),
            SelectTarget::StarColumns { column_filter, .. } => {
                *column_filter = Some(ColumnFilter::Excludes(exclude));
            }
        }
    }

    pub fn has_window(&self) -> bool {
        match self {
            SelectTarget::AliasedExpr { expr, .. } => match &**expr {
                Expr::FunctionCall { func, .. } => func.window.is_some(),
                _ => false,
            },
            SelectTarget::StarColumns { .. } => false,
        }
    }

    pub fn function_call_name(&self) -> Option<String> {
        match self {
            SelectTarget::AliasedExpr { expr, .. } => match &**expr {
                Expr::FunctionCall { func, .. } if func.window.is_none() => {
                    Some(func.name.name.to_lowercase())
                }
                _ => None,
            },
            SelectTarget::StarColumns { .. } => None,
        }
    }
}

impl Display for SelectTarget {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SelectTarget::AliasedExpr { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(ident) = alias {
                    write!(f, " AS {ident}")?;
                }
            }
            SelectTarget::StarColumns {
                qualified,
                column_filter,
            } => match column_filter {
                Some(ColumnFilter::Excludes(excludes)) => {
                    write_dot_separated_list(f, qualified)?;
                    write!(f, " EXCLUDE (")?;
                    write_comma_separated_list(f, excludes)?;
                    write!(f, ")")?;
                }
                Some(ColumnFilter::Lambda(lambda)) => {
                    write!(f, "COLUMNS({lambda})")?;
                }
                None => {
                    write_dot_separated_list(f, qualified)?;
                }
            },
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum ColumnFilter {
    Excludes(Vec<Identifier>),
    Lambda(Lambda),
}

impl ColumnFilter {
    pub fn get_excludes(&self) -> Option<&[Identifier]> {
        if let ColumnFilter::Excludes(ex) = self {
            Some(ex)
        } else {
            None
        }
    }

    pub fn get_lambda(&self) -> Option<&Lambda> {
        if let ColumnFilter::Lambda(l) = self {
            Some(l)
        } else {
            None
        }
    }
}

pub type QualifiedName = Vec<Indirection>;

/// Indirection of a select result, like a part of `db.table.column`.
/// Can be a database name, table name, field name or wildcard star(`*`).
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum Indirection {
    // Field name
    Identifier(Identifier),
    // Wildcard star
    Star(Span),
}

impl Display for Indirection {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Indirection::Identifier(ident) => {
                write!(f, "{ident}")?;
            }
            Indirection::Star(_) => {
                write!(f, "*")?;
            }
        }
        Ok(())
    }
}

/// Time Travel specification
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TimeTravelPoint {
    Snapshot(String),
    Timestamp(Box<Expr>),
    Offset(Box<Expr>),
    Stream {
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        name: Identifier,
    },
}

impl Display for TimeTravelPoint {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TimeTravelPoint::Snapshot(sid) => {
                write!(f, "(SNAPSHOT => '{sid}')")?;
            }
            TimeTravelPoint::Timestamp(ts) => {
                write!(f, "(TIMESTAMP => {ts})")?;
            }
            TimeTravelPoint::Offset(num) => {
                write!(f, "(OFFSET => {num})")?;
            }
            TimeTravelPoint::Stream {
                catalog,
                database,
                name,
            } => {
                write!(f, "(STREAM => ")?;
                write_dot_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(name)),
                )?;
                write!(f, ")")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum PivotValues {
    ColumnValues(Vec<Expr>),
    Subquery(Box<Query>),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Pivot {
    pub aggregate: Expr,
    pub value_column: Identifier,
    pub values: PivotValues,
}

impl Display for Pivot {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PIVOT({} FOR {} IN (", self.aggregate, self.value_column)?;
        match &self.values {
            PivotValues::ColumnValues(column_values) => {
                write_comma_separated_list(f, column_values)?;
            }
            PivotValues::Subquery(subquery) => {
                write!(f, "{}", subquery)?;
            }
        }
        write!(f, "))")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct UnpivotName {
    pub ident: Identifier,
    pub alias: Option<String>,
}

impl Display for UnpivotName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.alias {
            Some(alias) => {
                write!(f, "{} AS {}", self.ident, QuotedString(alias, '\''))
            }
            None => write!(f, "{}", self.ident),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Unpivot {
    pub value_column: Identifier,
    pub unpivot_column: Identifier,
    pub column_names: Vec<UnpivotName>,
}

impl Display for Unpivot {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "UNPIVOT({} FOR {} IN (",
            self.value_column, self.unpivot_column
        )?;
        write_comma_separated_list(f, &self.column_names)?;
        write!(f, "))")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Drive, DriveMut)]
pub struct WithOptions {
    pub options: BTreeMap<String, String>,
}

impl Display for WithOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WITH (")?;
        write_comma_separated_string_map(f, &self.options)?;
        write!(f, ")")
    }
}

impl WithOptions {
    /// Used for build change query.
    pub fn to_change_query_with_clause(&self) -> String {
        let mut result = String::from(" WITH (");
        for (i, (k, v)) in self.options.iter().enumerate() {
            if i > 0 {
                result.push_str(", ");
            }

            if k == "consume" {
                // The consume stream will be recorded in QueryContext.
                // Skip 'consume' to avoid unnecessary operations.
                result.push_str("consume = false");
            } else {
                result.push_str(&format!("{k} = '{v}'"));
            }
        }
        result.push(')');
        result
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ChangesInterval {
    pub append_only: bool,
    pub at_point: TimeTravelPoint,
    pub end_point: Option<TimeTravelPoint>,
}

impl Display for ChangesInterval {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CHANGES (INFORMATION => ")?;
        if self.append_only {
            write!(f, "APPEND_ONLY")?;
        } else {
            write!(f, "DEFAULT")?;
        }
        write!(f, ") AT {}", self.at_point)?;
        if let Some(end_point) = &self.end_point {
            write!(f, " END {}", end_point)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TemporalClause {
    TimeTravel(TimeTravelPoint),
    Changes(ChangesInterval),
}

impl Display for TemporalClause {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TemporalClause::TimeTravel(point) => {
                write!(f, "AT {}", point)?;
            }
            TemporalClause::Changes(changes) => {
                write!(f, "{}", changes)?;
            }
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum SampleRowLevel {
    RowsNum(f64),
    Probability(f64),
}

impl SampleRowLevel {
    pub fn sample_probability(&self, stats_rows: Option<u64>) -> Result<Option<f64>> {
        let rand = match &self {
            SampleRowLevel::Probability(probability) => probability / 100.0,
            SampleRowLevel::RowsNum(rows) => {
                if let Some(row_num) = stats_rows {
                    if row_num > 0 {
                        rows / row_num as f64
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
        };
        if rand > 1.0 {
            return Err(ParseError(
                None,
                format!(
                    "Sample value should be less than or equal to 100, but got {}",
                    rand * 100.0
                ),
            ));
        }
        Ok(Some(rand))
    }
}

impl Eq for SampleRowLevel {}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut, Default,
)]
pub struct SampleConfig {
    pub row_level: Option<SampleRowLevel>,
    pub block_level: Option<f64>,
}

impl SampleConfig {
    pub fn set_row_level_sample(&mut self, value: f64, rows: bool) {
        if rows {
            self.row_level = Some(SampleRowLevel::RowsNum(value));
        } else {
            self.row_level = Some(SampleRowLevel::Probability(value));
        }
    }

    pub fn set_block_level_sample(&mut self, probability: f64) {
        self.block_level = Some(probability);
    }
}

impl Eq for SampleConfig {}

impl Display for SampleConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SAMPLE ")?;
        if let Some(block_level) = self.block_level {
            write!(f, "BLOCK ({}) ", block_level)?;
        }
        if let Some(row_level) = &self.row_level {
            match row_level {
                SampleRowLevel::RowsNum(rows) => {
                    write!(f, "ROW ({} ROWS)", rows)?;
                }
                SampleRowLevel::Probability(probability) => {
                    write!(f, "ROW ({})", probability)?;
                }
            }
        }
        Ok(())
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TableReference {
    // Table name
    Table {
        span: Span,
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
        alias: Option<TableAlias>,
        temporal: Option<TemporalClause>,
        with_options: Option<WithOptions>,
        pivot: Option<Box<Pivot>>,
        unpivot: Option<Box<Unpivot>>,
        sample: Option<SampleConfig>,
    },
    // `TABLE(expr)[ AS alias ]`
    TableFunction {
        span: Span,
        /// Whether the table function is a lateral table function
        lateral: bool,
        name: Identifier,
        params: Vec<Expr>,
        named_params: Vec<(Identifier, Expr)>,
        alias: Option<TableAlias>,
        sample: Option<SampleConfig>,
    },
    // Derived table, which can be a subquery or joined tables or combination of them
    Subquery {
        span: Span,
        /// Whether the subquery is a lateral subquery
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
        pivot: Option<Box<Pivot>>,
        unpivot: Option<Box<Unpivot>>,
    },
    Join {
        span: Span,
        join: Join,
    },
    Location {
        span: Span,
        location: FileLocation,
        options: SelectStageOptions,
        alias: Option<TableAlias>,
    },
}

impl TableReference {
    pub fn pivot(&self) -> Option<&Pivot> {
        match self {
            TableReference::Table { pivot, .. } => pivot.as_ref().map(|b| b.as_ref()),
            TableReference::Subquery { pivot, .. } => pivot.as_ref().map(|b| b.as_ref()),
            _ => None,
        }
    }

    pub fn unpivot(&self) -> Option<&Unpivot> {
        match self {
            TableReference::Table { unpivot, .. } => unpivot.as_ref().map(|b| b.as_ref()),
            TableReference::Subquery { unpivot, .. } => unpivot.as_ref().map(|b| b.as_ref()),
            _ => None,
        }
    }

    pub fn is_lateral_table_function(&self) -> bool {
        match self {
            TableReference::TableFunction { lateral, .. } => *lateral,
            _ => false,
        }
    }

    pub fn is_lateral_subquery(&self) -> bool {
        match self {
            TableReference::Subquery { lateral, .. } => *lateral,
            _ => false,
        }
    }
}

impl Display for TableReference {
    #[recursive::recursive]
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TableReference::Table {
                span: _,
                catalog,
                database,
                table,
                alias,
                temporal,
                with_options,
                pivot,
                unpivot,
                sample,
            } => {
                write_dot_separated_list(
                    f,
                    catalog.iter().chain(database.iter()).chain(Some(table)),
                )?;

                if let Some(temporal) = temporal {
                    write!(f, " {temporal}")?;
                }

                if let Some(with_options) = with_options {
                    write!(f, " {with_options}")?;
                }

                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if let Some(pivot) = pivot {
                    write!(f, " {pivot}")?;
                }

                if let Some(unpivot) = unpivot {
                    write!(f, " {unpivot}")?;
                }

                if let Some(sample) = sample {
                    write!(f, " {sample}")?;
                }
            }
            TableReference::TableFunction {
                span: _,
                lateral,
                name,
                params,
                named_params,
                alias,
                sample,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "{name}(")?;
                write_comma_separated_list(f, params)?;
                if !params.is_empty() && !named_params.is_empty() {
                    write!(f, ",")?;
                }
                for (i, (k, v)) in named_params.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{k}=>{v}")?;
                }
                write!(f, ")")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if let Some(sample) = sample {
                    write!(f, " {sample}")?;
                }
            }
            TableReference::Subquery {
                span: _,
                lateral,
                subquery,
                alias,
                pivot,
                unpivot,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "({subquery})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }

                if let Some(pivot) = pivot {
                    write!(f, " {pivot}")?;
                }

                if let Some(unpivot) = unpivot {
                    write!(f, " {unpivot}")?;
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
                    JoinOperator::LeftSemi => {
                        write!(f, " LEFT SEMI JOIN")?;
                    }
                    JoinOperator::RightSemi => {
                        write!(f, " RIGHT SEMI JOIN")?;
                    }
                    JoinOperator::LeftAnti => {
                        write!(f, " LEFT ANTI JOIN")?;
                    }
                    JoinOperator::RightAnti => {
                        write!(f, " RIGHT ANTI JOIN")?;
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
            TableReference::Location {
                span: _,
                location,
                options,
                alias,
            } => {
                write!(f, "{location}")?;
                if !options.is_empty() {
                    write!(f, "{options}")?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct TableAlias {
    pub name: Identifier,
    pub columns: Vec<Identifier>,
}

impl Display for TableAlias {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", &self.name)?;
        if !self.columns.is_empty() {
            write!(f, "(")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Join {
    pub op: JoinOperator,
    pub condition: JoinCondition,
    pub left: Box<TableReference>,
    pub right: Box<TableReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum JoinOperator {
    Inner,
    // Outer joins can not work with `JoinCondition::None`
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    LeftAnti,
    RightSemi,
    RightAnti,
    // CrossJoin can only work with `JoinCondition::None`
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum JoinCondition {
    On(Box<Expr>),
    Using(Vec<Identifier>),
    Natural,
    None,
}

impl SetExpr {
    pub fn span(&self) -> Span {
        match self {
            SetExpr::Select(stmt) => stmt.span,
            SetExpr::Query(query) => query.span,
            SetExpr::SetOperation(op) => op.span,
            SetExpr::Values { span, .. } => *span,
        }
    }

    pub fn into_query(self) -> Query {
        match self {
            SetExpr::Query(query) => *query,
            _ => Query {
                span: self.span(),
                with: None,
                body: self,
                order_by: vec![],
                limit: vec![],
                offset: None,
                ignore_result: false,
            },
        }
    }
}
