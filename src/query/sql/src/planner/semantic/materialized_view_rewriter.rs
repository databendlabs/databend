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

//! Materialized-view definition rewriter.
//!
//! The rewriter describes both schemas while converting an aggregate query into
//! the query whose outputs are persisted. Types are filled from the bound plans
//! by the materialized-view binder.

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitResult;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::VisitorMut;
use databend_common_ast::visit::Walk;
use databend_common_ast::visit::WalkMut;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BASE_ROW_ID_COL_NAME;
use databend_common_expression::FunctionKind;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN;

use crate::MetadataRef;
use crate::planner::SUPPORTED_AGGREGATING_INDEX_FUNCTIONS;

/// Parse persisted materialized-view SQL and require a query statement.
///
/// MV definitions are serialized from ASTs with `Query::to_string()`, so they are an internal
/// persisted format rather than SQL in the current session's dialect. Parse them with one fixed
/// dialect to keep CREATE, REFRESH, and reads independent of session settings. PostgreSQL is the
/// canonical choice here and its parser accepts both `"` and backtick identifier quotes, covering
/// the quote characters that the serialized AST may preserve from the CREATE statement.
pub fn parse_materialized_view_query(sql: &str, error: impl Into<String>) -> Result<Query> {
    let tokens = tokenize_sql(sql)?;
    let (statement, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
    let Statement::Query(query) = statement else {
        return Err(ErrorCode::InvalidMaterializedView(error.into()));
    };
    Ok(*query)
}

/// Require a bound persisted MV definition to resolve to its original single source table.
pub fn validate_materialized_view_source(
    metadata: &MetadataRef,
    expected_source_table_id: u64,
    materialized_view_name: &str,
) -> Result<()> {
    let actual_source_table_id = {
        let metadata = metadata.read();
        let [source] = metadata.tables() else {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {materialized_view_name} physical definition must resolve to exactly one source table"
            )));
        };
        source.table().get_id()
    };
    if actual_source_table_id != expected_source_table_id {
        return Err(ErrorCode::InvalidMaterializedView(format!(
            "materialized view {materialized_view_name} source table changed: expected table id {expected_source_table_id}, resolved table id {actual_source_table_id}"
        )));
    }
    Ok(())
}

/// Rewrites a user MV definition into its physical storage query and records
/// physical names and logical definition expressions in their schema order.
#[derive(Debug, Clone, Default)]
pub struct MaterializedViewRewriter {
    is_aggregating: bool,
    source_database: String,
    physical_names: Vec<String>,
    logical_names: Vec<String>,
    logical_define_exprs: Vec<String>,
    specified_columns: Vec<String>,
}

impl MaterializedViewRewriter {
    pub fn new(
        is_aggregating: bool,
        source_database: impl Into<String>,
        specified_columns: Vec<String>,
    ) -> Self {
        Self {
            is_aggregating,
            source_database: source_database.into(),
            specified_columns,
            ..Default::default()
        }
    }

    pub fn physical_names(&self) -> &[String] {
        &self.physical_names
    }

    pub fn logical_names(&self) -> &[String] {
        &self.logical_names
    }

    pub fn logical_define_exprs(&self) -> &[String] {
        &self.logical_define_exprs
    }

    pub fn rewrite_query(&mut self, query: &mut Query) -> Result<()> {
        self.physical_names.clear();
        self.logical_names.clear();
        self.logical_define_exprs.clear();
        query.walk_mut(self)?;
        if self.logical_define_exprs.is_empty() {
            return Err(ErrorCode::Internal(
                "materialized view rewriter produced no logical outputs".to_string(),
            ));
        }
        Ok(())
    }

    fn output_names(&self, stmt: &SelectStmt) -> Result<Vec<String>> {
        if !self.specified_columns.is_empty() {
            if self.specified_columns.len() != stmt.select_list.len() {
                return Err(ErrorCode::SemanticError(format!(
                    "materialized view column list has {} columns, but SELECT has {} columns",
                    self.specified_columns.len(),
                    stmt.select_list.len()
                )));
            }
            return Ok(self.specified_columns.clone());
        }

        stmt.select_list
            .iter()
            .map(|target| match target {
                SelectTarget::AliasedExpr {
                    alias: Some(alias), ..
                } => Ok(alias.name.clone()),
                SelectTarget::AliasedExpr { expr, alias: None } => match expr.as_ref() {
                    Expr::ColumnRef { column, .. } => Ok(column.column.name().to_string()),
                    _ => Err(ErrorCode::SemanticError(
                        "materialized view SELECT expressions must have aliases when no column list is specified"
                            .to_string(),
                    )),
                },
                SelectTarget::StarColumns { .. } => Err(ErrorCode::SemanticError(
                    "materialized view does not support SELECT *".to_string(),
                )),
            })
            .collect()
    }

    fn rewrite_aggregate_expr(
        &mut self,
        expr: &Expr,
        output_name: &str,
        physical_field_count: usize,
        aggregate_targets: &mut Vec<SelectTarget>,
    ) -> Result<Expr> {
        let mut expr = expr.clone();
        let mut rewriter = AggregateExprRewriter {
            aggregate_targets,
            output_name,
            physical_field_count,
            next_field_index: 0,
        };
        expr.walk_mut(&mut rewriter)?;
        Ok(expr)
    }

    fn rewrite_non_aggregate(&mut self, stmt: &mut SelectStmt) -> Result<()> {
        let output_names = self.output_names(stmt)?;
        for (target, name) in stmt.select_list.iter_mut().zip(output_names) {
            let SelectTarget::AliasedExpr { alias, .. } = target else {
                return Err(ErrorCode::SemanticError(
                    "materialized view does not support SELECT *".to_string(),
                ));
            };
            *alias = Some(Identifier::from_name(None, &name));
            self.physical_names.push(name.clone());
            self.logical_names.push(name.clone());
            self.logical_define_exprs.push(name);
        }

        stmt.select_list.push(SelectTarget::AliasedExpr {
            expr: Box::new(Expr::ColumnRef {
                span: None,
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(Identifier::from_name(None, BASE_ROW_ID_COL_NAME)),
                },
            }),
            alias: Some(Identifier::from_name(
                None,
                MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN,
            )),
        });
        self.physical_names
            .push(MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN.to_string());
        Ok(())
    }

    fn rewrite_aggregate(&mut self, stmt: &mut SelectStmt) -> Result<()> {
        let original_targets = stmt.select_list.clone();
        let output_names = self.output_names(stmt)?;
        self.logical_names = output_names.clone();
        let mut aggregate_targets = Vec::new();

        for (target, output_name) in original_targets.iter().zip(&output_names) {
            let SelectTarget::AliasedExpr { expr, .. } = target else {
                return Err(ErrorCode::SemanticError(
                    "materialized view does not support SELECT *".to_string(),
                ));
            };
            let physical_field_count = AggregateFieldCounter::count(expr);
            let default_expr = self
                .rewrite_aggregate_expr(
                    expr,
                    output_name,
                    physical_field_count,
                    &mut aggregate_targets,
                )?
                .to_string();
            self.logical_define_exprs.push(default_expr);
        }

        if let Some(GroupBy::Normal(groups)) = &stmt.group_by {
            for group in groups {
                // Prefer the SELECT expression itself over an alias. This avoids choosing an
                // aggregate output whose alias happens to equal the GROUP BY expression.
                let selected_group = original_targets
                    .iter()
                    .enumerate()
                    .find(|(_, target)| {
                        matches!(target, SelectTarget::AliasedExpr { expr, .. }
                            if expr.as_ref().to_string() == group.to_string())
                    })
                    .or_else(|| {
                        original_targets.iter().enumerate().find(|(_, target)| {
                            matches!(target, SelectTarget::AliasedExpr { alias: Some(alias), .. }
                                if matches!(group, Expr::ColumnRef { column, .. }
                                    if column.column.name() == alias.name))
                        })
                    });

                let Some((first_output_index, selected_group)) = selected_group else {
                    return Err(ErrorCode::InvalidMaterializedView(format!(
                        "GROUP BY key '{}' is not in the view definition's select list",
                        group
                    )));
                };
                let SelectTarget::AliasedExpr {
                    expr: selected_expr,
                    ..
                } = selected_group
                else {
                    unreachable!()
                };
                let selected_expr_text = selected_expr.to_string();
                // Store a repeated GROUP BY expression only once, under the first matching output
                // name. Every logical output of that expression must reference this same physical
                // column; otherwise later outputs would retain a source expression that does not
                // exist in the MV storage schema.
                let name = output_names[first_output_index].clone();
                for (output_index, target) in original_targets.iter().enumerate() {
                    if matches!(target, SelectTarget::AliasedExpr { expr, .. }
                        if expr.to_string() == selected_expr_text)
                    {
                        self.logical_define_exprs[output_index] = name.clone();
                    }
                }
                aggregate_targets.push(AggregateExprRewriter::target(
                    selected_expr.as_ref().clone(),
                    &name,
                ));
            }
        }

        self.physical_names = aggregate_targets
            .iter()
            .map(|target| match target {
                SelectTarget::AliasedExpr {
                    alias: Some(alias), ..
                } => Ok(alias.name.clone()),
                _ => Err(ErrorCode::Internal(
                    "materialized view physical target has no alias".to_string(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        stmt.select_list = aggregate_targets;
        Ok(())
    }

    fn rewrite_select_stmt(&mut self, stmt: &mut SelectStmt) -> Result<()> {
        if self.is_aggregating {
            self.rewrite_aggregate(stmt)
        } else {
            self.rewrite_non_aggregate(stmt)
        }
    }
}

struct AggregateFieldCounter {
    count: usize,
}

impl AggregateFieldCounter {
    fn count(expr: &Expr) -> usize {
        let mut counter = Self { count: 0 };
        let _ = expr.walk(&mut counter);
        counter.count
    }
}

impl Visitor for AggregateFieldCounter {
    fn visit_expr(&mut self, expr: &Expr) -> VisitResult {
        if matches!(expr, Expr::CountAll { .. }) {
            self.count += 1;
            return Ok(VisitControl::SkipChildren);
        }
        Ok(VisitControl::Continue)
    }

    fn visit_function_call(&mut self, call: &FunctionCall) -> VisitResult {
        let name = call.name.name.to_ascii_lowercase();
        if SUPPORTED_AGGREGATING_INDEX_FUNCTIONS.contains(&name.as_str()) {
            self.count += if name == "avg" { 2 } else { 1 };
            return Ok(VisitControl::SkipChildren);
        }
        Ok(VisitControl::Continue)
    }
}

struct AggregateExprRewriter<'a> {
    aggregate_targets: &'a mut Vec<SelectTarget>,
    output_name: &'a str,
    physical_field_count: usize,
    next_field_index: usize,
}

impl AggregateExprRewriter<'_> {
    fn column_ref(name: &str) -> Expr {
        Expr::ColumnRef {
            span: None,
            column: ColumnRef {
                database: None,
                table: None,
                column: ColumnID::Name(Identifier::from_name(None, name)),
            },
        }
    }

    fn target(expr: Expr, name: &str) -> SelectTarget {
        SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias: Some(Identifier::from_name(None, name)),
        }
    }

    fn state_function(original: &Expr, func: &FunctionCall, name: &str) -> Expr {
        Expr::FunctionCall {
            span: original.span(),
            func: FunctionCall {
                name: Identifier::from_name(original.span(), name),
                ..func.clone()
            },
        }
    }

    fn add_target(&mut self, expr: Expr, name: &str) -> String {
        if let Some(existing_name) = self
            .aggregate_targets
            .iter()
            .find_map(|target| match target {
                SelectTarget::AliasedExpr {
                    expr: existing_expr,
                    alias: Some(alias),
                } if existing_expr.to_string() == expr.to_string() => Some(alias.name.clone()),
                _ => None,
            })
        {
            return existing_name;
        }
        self.aggregate_targets.push(Self::target(expr, name));
        name.to_string()
    }

    fn next_physical_name(&mut self) -> String {
        let name = if self.physical_field_count == 1 {
            self.output_name.to_string()
        } else {
            format!("{}$sys_facade${}", self.output_name, self.next_field_index)
        };
        self.next_field_index += 1;
        name
    }

    fn rewrite_avg(&mut self, expr: &Expr, func: &FunctionCall) -> Result<Expr> {
        if func.args.len() != 1 {
            return Err(ErrorCode::SemanticError(
                "materialized view avg() requires exactly one argument".to_string(),
            ));
        }

        let sum_expr = Self::state_function(expr, func, "sum_state");
        let sum_name = self.next_physical_name();
        let sum_name = self.add_target(sum_expr, &sum_name);

        let count_expr = Self::state_function(expr, func, "count_state");
        let count_name = self.next_physical_name();
        let count_name = self.add_target(count_expr, &count_name);

        let count_ref = Self::column_ref(&count_name);
        let denominator = Expr::FunctionCall {
            span: expr.span(),
            func: FunctionCall {
                distinct: false,
                name: Identifier::from_name(expr.span(), "if"),
                args: vec![
                    Expr::BinaryOp {
                        span: expr.span(),
                        op: BinaryOperator::Eq,
                        left: Box::new(count_ref.clone()),
                        right: Box::new(Expr::Literal {
                            span: expr.span(),
                            value: Literal::UInt64(0),
                        }),
                    },
                    Expr::Literal {
                        span: expr.span(),
                        value: Literal::UInt64(1),
                    },
                    count_ref,
                ],
                params: vec![],
                order_by: vec![],
                filter: None,
                window: None,
                lambda: None,
            },
        };
        Ok(Expr::BinaryOp {
            span: expr.span(),
            op: BinaryOperator::Divide,
            left: Box::new(Self::column_ref(&sum_name)),
            right: Box::new(denominator),
        })
    }
}

impl VisitorMut for AggregateExprRewriter<'_> {
    type Error = ErrorCode;

    fn visit_expr(&mut self, expr: &mut Expr) -> std::result::Result<VisitControl, ErrorCode> {
        match expr {
            Expr::FunctionCall { func, .. }
                if !func.distinct
                    && func.filter.is_none()
                    && func.window.is_none()
                    && func.lambda.is_none()
                    && func.order_by.is_empty()
                    && func.params.is_empty()
                    && SUPPORTED_AGGREGATING_INDEX_FUNCTIONS
                        .contains(&func.name.name.to_ascii_lowercase().as_str()) =>
            {
                let original = expr.clone();
                let Expr::FunctionCall { func, .. } = &original else {
                    unreachable!()
                };
                let func_name = func.name.name.to_ascii_lowercase();
                if func_name == "avg" {
                    *expr = self.rewrite_avg(&original, func)?;
                } else {
                    let state_name = format!("{func_name}_state");
                    let state_expr = Self::state_function(&original, func, &state_name);
                    let name = self.next_physical_name();
                    let name = self.add_target(state_expr, &name);
                    *expr = Self::column_ref(&name);
                }
                Ok(VisitControl::SkipChildren)
            }
            Expr::CountAll {
                filter: None,
                window: None,
                ..
            } => {
                let state_expr = Expr::FunctionCall {
                    span: expr.span(),
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(expr.span(), "count_state"),
                        args: vec![],
                        params: vec![],
                        order_by: vec![],
                        filter: None,
                        window: None,
                        lambda: None,
                    },
                };
                let name = self.next_physical_name();
                let name = self.add_target(state_expr, &name);
                *expr = Self::column_ref(&name);
                Ok(VisitControl::SkipChildren)
            }
            _ => Ok(VisitControl::Continue),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaterializedViewChecker {
    has_aggregate: bool,
    has_group_by: bool,
    not_supported: bool,
}

impl MaterializedViewChecker {
    pub fn check_query(query: &Query) -> Self {
        let mut checker = Self::default();
        let _ = query.walk(&mut checker);
        checker
    }

    pub fn is_aggregating(&self) -> bool {
        self.has_aggregate || self.has_group_by
    }

    pub fn is_supported(&self) -> bool {
        !self.not_supported
    }
}

impl Visitor for MaterializedViewChecker {
    fn visit_query(&mut self, query: &Query) -> VisitResult {
        // An MV definition is deliberately limited to one SELECT query block.
        if query.with.is_some()
            || !query.order_by.is_empty()
            || !query.limit.is_empty()
            || query.offset.is_some()
            || query.ignore_result
            || !matches!(&query.body, SetExpr::Select(_))
        {
            self.not_supported = true;
        }
        Ok(VisitControl::Continue)
    }

    fn visit_select_stmt(&mut self, stmt: &SelectStmt) -> VisitResult {
        let has_plain_source = matches!(stmt.from.as_slice(), [TableReference::Table {
            temporal: None,
            with_options: None,
            pivot: None,
            unpivot: None,
            sample: None,
            ..
        }]);
        // Keep the accepted shape explicit: SELECT expressions FROM one plain table, with only
        // optional WHERE and normal GROUP BY clauses. In particular, derived tables, joins,
        // table functions, stages and table decorators must not become refresh sources.
        if stmt.hints.is_some()
            || stmt.distinct
            || stmt.having.is_some()
            || stmt.window_list.is_some()
            || stmt.qualify.is_some()
            || stmt.top_n.is_some()
            || !matches!(stmt.group_by, None | Some(GroupBy::Normal(_)))
            || !has_plain_source
            || stmt
                .select_list
                .iter()
                .any(|target| matches!(target, SelectTarget::StarColumns { .. }))
        {
            self.not_supported = true;
        }
        self.has_group_by |= stmt.group_by.is_some();
        Ok(VisitControl::Continue)
    }

    fn visit_expr(&mut self, expr: &Expr) -> VisitResult {
        match expr {
            Expr::CountAll { filter, window, .. } => {
                self.has_aggregate = true;
                if filter.is_some() || window.is_some() {
                    self.not_supported = true;
                }
            }
            Expr::InSubquery { .. }
            | Expr::LikeSubquery { .. }
            | Expr::Exists { .. }
            | Expr::Subquery { .. }
            | Expr::Hole { .. }
            | Expr::Placeholder { .. }
            | Expr::StageLocation { .. } => self.not_supported = true,
            _ => {}
        }
        Ok(VisitControl::Continue)
    }

    fn visit_function_call(&mut self, call: &FunctionCall) -> VisitResult {
        let name = call.name.name.to_ascii_lowercase();
        // Function-level ordering/filtering/windowing changes evaluation semantics beyond the
        // supported SELECT/FROM/WHERE/GROUP BY shape. Aggregate-specific checks below retain the
        // same restriction and additionally validate the supported aggregate family.
        if call.window.is_some() || call.filter.is_some() || !call.order_by.is_empty() {
            self.not_supported = true;
        }
        if AggregateFunctionFactory::instance().contains(&name) {
            self.has_aggregate = true;
            if !SUPPORTED_AGGREGATING_INDEX_FUNCTIONS.contains(&name.as_str())
                || call.distinct
                || call.filter.is_some()
                || call.window.is_some()
                || !call.order_by.is_empty()
            {
                self.not_supported = true;
            }
        } else if let Some(property) = BUILTIN_FUNCTIONS.get_property(&name) {
            if property.kind == FunctionKind::SRF || property.non_deterministic {
                self.not_supported = true;
            }
        } else {
            self.not_supported = true;
        }
        Ok(VisitControl::Continue)
    }
}

impl VisitorMut for MaterializedViewRewriter {
    type Error = ErrorCode;

    fn visit_query(&mut self, query: &mut Query) -> std::result::Result<VisitControl, ErrorCode> {
        if let SetExpr::Select(stmt) = &mut query.body
            && let Some(TableReference::Table { table, .. }) = stmt.from.first_mut()
            && table.database.is_none()
        {
            table.database = Some(Identifier::from_name(query.span, &self.source_database));
        }
        Ok(VisitControl::Continue)
    }

    fn visit_select_stmt(
        &mut self,
        select: &mut SelectStmt,
    ) -> std::result::Result<VisitControl, ErrorCode> {
        self.rewrite_select_stmt(select)?;
        Ok(VisitControl::SkipChildren)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite_with_columns(
        sql: &str,
        columns: Vec<String>,
    ) -> Result<(Query, MaterializedViewRewriter)> {
        let mut query =
            parse_materialized_view_query(sql, "test materialized view query must be a query")?;
        let checker = MaterializedViewChecker::check_query(&query);
        let mut rewriter =
            MaterializedViewRewriter::new(checker.is_aggregating(), "default", columns);
        rewriter.rewrite_query(&mut query)?;
        Ok((query, rewriter))
    }

    fn rewrite(sql: &str) -> Result<(Query, MaterializedViewRewriter)> {
        rewrite_with_columns(sql, Vec::new())
    }

    fn check_query(sql: &str) -> Result<MaterializedViewChecker> {
        let query =
            parse_materialized_view_query(sql, "test materialized view query must be a query")?;
        Ok(MaterializedViewChecker::check_query(&query))
    }

    #[test]
    fn test_parse_persisted_query_with_backtick_identifiers() -> Result<()> {
        let query = parse_materialized_view_query(
            "SELECT `value` FROM `default`.`source`",
            "test materialized view query must be a query",
        )?;
        assert_eq!(query.to_string(), "SELECT `value` FROM `default`.`source`");
        Ok(())
    }

    #[test]
    fn test_materialized_view_checker_accepts_simple_queries() -> Result<()> {
        for sql in [
            "SELECT amount AS value FROM t",
            "SELECT amount AS value FROM t WHERE amount > 0",
            "SELECT category, sum(amount) AS total FROM t WHERE amount > 0 GROUP BY category",
        ] {
            let checker = check_query(sql)?;
            assert!(checker.is_supported(), "should support: {sql}");
        }
        Ok(())
    }

    #[test]
    fn test_materialized_view_checker_rejects_non_simple_queries() -> Result<()> {
        for sql in [
            "WITH s AS (SELECT amount FROM t) SELECT amount FROM s WHERE amount > 0",
            "SELECT DISTINCT amount FROM t WHERE amount > 0",
            "SELECT amount FROM t WHERE amount > 0 ORDER BY amount",
            "SELECT amount FROM t WHERE amount > 0 LIMIT 1",
            "SELECT amount FROM t WHERE amount > 0 OFFSET 1",
            "SELECT amount FROM t1, t2 WHERE t1.id = t2.id",
            "SELECT t1.amount FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.amount > 0",
            "SELECT amount FROM (SELECT amount FROM t) AS s WHERE amount > 0",
            "SELECT number FROM numbers(10) WHERE number > 0",
            "SELECT amount FROM @stage WHERE amount > 0",
            "SELECT amount FROM t AT (SNAPSHOT => 'snapshot') WHERE amount > 0",
            "SELECT amount FROM t WITH (consume = true) WHERE amount > 0",
            "SELECT amount FROM t SAMPLE BLOCK (10) WHERE amount > 0",
            "SELECT amount FROM t PIVOT(sum(amount) FOR amount IN (1)) WHERE amount > 0",
            "SELECT amount FROM t UNPIVOT(value FOR name IN (amount)) WHERE value > 0",
            "SELECT amount FROM t WHERE EXISTS (SELECT 1 FROM t2)",
            "SELECT amount FROM t WHERE amount IN (SELECT amount FROM t2)",
            "SELECT (SELECT max(amount) FROM t2) AS amount FROM t WHERE amount > 0",
            "SELECT * FROM t WHERE amount > 0",
            "SELECT COLUMNS(name -> name = 'amount') FROM t WHERE amount > 0",
            "SELECT amount FROM t GROUP BY CUBE(amount)",
            "SELECT amount FROM t WHERE amount > 0 HAVING amount > 1",
            "SELECT row_number() OVER (ORDER BY amount) AS rn FROM t WHERE amount > 0",
        ] {
            let checker = check_query(sql)?;
            assert!(!checker.is_supported(), "should reject: {sql}");
        }
        Ok(())
    }

    #[test]
    fn test_materialized_view_checker_rejects_non_deterministic_functions() -> Result<()> {
        // Non-deterministic functions (and their aliases) must not appear in an MV definition,
        // because refresh would recompute different values than the original query. Aliases such
        // as current_timestamp/current_date resolve to canonical functions whose properties mark
        // them non-deterministic, so they must be rejected just like the canonical names.
        for sql in [
            "SELECT amount, now() AS t FROM t WHERE amount > 0",
            "SELECT amount, current_timestamp AS t FROM t WHERE amount > 0",
            "SELECT amount, current_timestamp() AS t FROM t WHERE amount > 0",
            "SELECT amount, today() AS d FROM t WHERE amount > 0",
            "SELECT amount, current_date AS d FROM t WHERE amount > 0",
            "SELECT amount, current_date() AS d FROM t WHERE amount > 0",
            "SELECT amount, rand() AS r FROM t WHERE amount > 0",
            "SELECT amount, uuid() AS u FROM t WHERE amount > 0",
            "SELECT amount FROM t WHERE created_at > now()",
            "SELECT amount FROM t WHERE created_at > current_timestamp",
        ] {
            let checker = check_query(sql)?;
            assert!(!checker.is_supported(), "should reject: {sql}");
        }
        Ok(())
    }

    #[test]
    fn test_rewrite_non_aggregate_query() -> Result<()> {
        let (query, rewriter) =
            rewrite("SELECT amount AS value, category FROM t WHERE amount > 0")?;

        assert_eq!(rewriter.physical_names(), [
            "value",
            "category",
            MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN
        ]);
        assert_eq!(rewriter.logical_names(), ["value", "category"]);
        assert_eq!(rewriter.logical_define_exprs(), ["value", "category"]);
        assert_eq!(
            query.to_string(),
            "SELECT amount AS value, category AS category, _base_row_id AS _mv_source_row_id FROM default.t WHERE amount > 0"
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_with_specified_columns() -> Result<()> {
        let (query, rewriter) = rewrite_with_columns(
            "SELECT category, avg(amount) FROM t GROUP BY category",
            vec!["kind".to_string(), "mean".to_string()],
        )?;

        assert_eq!(rewriter.logical_names(), ["kind", "mean"]);
        assert_eq!(rewriter.physical_names(), [
            "mean$sys_facade$0",
            "mean$sys_facade$1",
            "kind"
        ]);
        assert!(
            query
                .to_string()
                .contains("sum_state(amount) AS mean$sys_facade$0")
        );
        assert!(query.to_string().contains("category AS kind"));
        Ok(())
    }

    #[test]
    fn test_rewrite_rejects_invalid_output_names() {
        let error = rewrite("SELECT amount + 1 FROM t").unwrap_err();
        assert!(error.message().contains("must have aliases"));

        let error =
            rewrite_with_columns("SELECT amount, category FROM t", vec!["value".to_string()])
                .unwrap_err();
        assert!(error.message().contains("column list has 1 columns"));
    }

    #[test]
    fn test_rewrite_multiple_aggregates_with_specified_columns() -> Result<()> {
        let (query, rewriter) = rewrite_with_columns(
            "SELECT count(id) AS row_count_state, avg(amount) AS amount_avg_state, category FROM mv_mock_base GROUP BY category",
            vec![
                "row_count_state".to_string(),
                "amount_avg_state".to_string(),
                "category".to_string(),
            ],
        )?;

        assert_eq!(rewriter.physical_names(), [
            "row_count_state",
            "amount_avg_state$sys_facade$0",
            "amount_avg_state$sys_facade$1",
            "category",
        ]);
        assert_eq!(
            query.to_string(),
            "SELECT count_state(id) AS row_count_state, sum_state(amount) AS amount_avg_state$sys_facade$0, count_state(amount) AS amount_avg_state$sys_facade$1, category AS category FROM default.mv_mock_base GROUP BY category"
        );
        assert_eq!(query.to_string().matches("count_state(amount)").count(), 1);
        Ok(())
    }

    #[test]
    fn test_rewrite_reuses_avg_sum_state() -> Result<()> {
        let (query, rewriter) = rewrite_with_columns(
            "SELECT category, sum(amount), count(*), avg(amount) FROM source GROUP BY category",
            vec![
                "category_name".to_string(),
                "total_amount".to_string(),
                "row_count".to_string(),
                "average_amount".to_string(),
            ],
        )?;

        assert_eq!(rewriter.physical_names(), [
            "total_amount",
            "row_count",
            "average_amount$sys_facade$1",
            "category_name",
        ]);
        assert_eq!(
            query.to_string(),
            "SELECT sum_state(amount) AS total_amount, count_state() AS row_count, count_state(amount) AS average_amount$sys_facade$1, category AS category_name FROM default.source GROUP BY category"
        );
        assert_eq!(query.to_string().matches("sum_state(amount)").count(), 1);
        assert!(rewriter.logical_define_exprs()[3].contains("total_amount"));
        assert!(rewriter.logical_define_exprs()[3].contains("average_amount$sys_facade$1"));
        Ok(())
    }

    #[test]
    fn test_rewrite_repeated_group_output_reuses_physical_column() -> Result<()> {
        let (query, rewriter) = rewrite_with_columns(
            "SELECT category, category, count(*) FROM source WHERE active GROUP BY category",
            vec!["a".to_string(), "b".to_string(), "n".to_string()],
        )?;

        assert_eq!(rewriter.physical_names(), ["n", "a"]);
        assert_eq!(rewriter.logical_names(), ["a", "b", "n"]);
        assert_eq!(rewriter.logical_define_exprs(), ["a", "a", "n"]);
        assert_eq!(
            query.to_string(),
            "SELECT count_state() AS n, category AS a FROM default.source WHERE active GROUP BY category"
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_rejects_group_key_outside_select_list() {
        let error = rewrite_with_columns("SELECT sum(amount) FROM source GROUP BY id", vec![
            "total".to_string(),
        ])
        .unwrap_err();

        assert_eq!(
            error.message(),
            "GROUP BY key 'id' is not in the view definition's select list"
        );
    }

    #[test]
    fn test_rewrite_aggregate_query() -> Result<()> {
        let (query, rewriter) =
            rewrite("SELECT category, avg(amount) + 1 AS avg FROM t GROUP BY category")?;

        assert_eq!(rewriter.physical_names(), [
            "avg$sys_facade$0",
            "avg$sys_facade$1",
            "category"
        ]);
        assert_eq!(rewriter.logical_names(), ["category", "avg"]);
        assert_eq!(rewriter.logical_define_exprs()[0], "category");
        let avg_expr = &rewriter.logical_define_exprs()[1];
        assert!(avg_expr.contains("avg$sys_facade$0"));
        assert!(avg_expr.contains("if(avg$sys_facade$1 = 0, 1, avg$sys_facade$1)"));
        assert!(avg_expr.ends_with("+ 1"));

        let physical_query = query.to_string();
        assert!(physical_query.contains("sum_state(amount) AS avg$sys_facade$0"));
        assert!(physical_query.contains("count_state(amount) AS avg$sys_facade$1"));
        assert!(physical_query.contains("category AS category"));
        Ok(())
    }
}
