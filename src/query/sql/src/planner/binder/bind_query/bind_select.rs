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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Expr::Array;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Pivot;
use databend_common_ast::ast::PivotValues;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnpivotName;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::Walk;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use log::warn;

use crate::AsyncFunctionRewriter;
use crate::optimizer::ir::SExpr;
use crate::planner::QueryExecutor;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::ExprContext;
use crate::planner::binder::aggregate_prepass::AggregateExprInfo;
use crate::planner::binder::project::SelectInfo;
use crate::planner::binder::select::SelectAliasCatalog;
use crate::planner::binder::select::SelectClauseFact;
use crate::planner::binder::select::SelectList;
use crate::planner::binder::sort::OrderItems;
use crate::plans::ScalarExpr;

#[derive(Clone, Default)]
struct SelectClauseFacts {
    having: Option<SelectClauseFact>,
    qualify: Option<SelectClauseFact>,
    order_by: Vec<SelectClauseFact>,
    aggregate_prepass_inputs: Vec<(Expr, ExprContext)>,
}

impl SelectClauseFacts {
    fn merge_expr_info(
        &mut self,
        expr_context: ExprContext,
        aliases: &SelectAliasCatalog,
        expr_info: AggregateExprInfo,
    ) {
        let fact = SelectClauseFact {
            alias_feature: aliases.aggregate_alias_feature(&expr_info.referenced_aliases),
            expr_info,
        };

        match expr_context {
            ExprContext::QualifyClause => self.qualify = Some(fact),
            ExprContext::HavingClause => {
                if fact.contains_or_references_aggregate() {
                    self.aggregate_prepass_inputs
                        .push((fact.expr_info.ast.clone(), ExprContext::HavingClause));
                }
                self.having = Some(fact)
            }
            ExprContext::OrderByClause => {
                if fact.contains_or_references_aggregate() {
                    self.aggregate_prepass_inputs
                        .push((fact.expr_info.ast.clone(), ExprContext::OrderByClause));
                }
                self.order_by.push(fact)
            }
            _ => unreachable!("aggregate prepass only inspects HAVING/QUALIFY/ORDER BY"),
        }
    }
}

struct SelectGlobalView {
    semantic_alias: SelectAliasCatalog,
    rewritten_alias: SelectAliasCatalog,
    qualify: Option<SelectClauseFact>,
    order_by: Vec<SelectClauseFact>,
}

struct SelectPreparation<'a> {
    s_expr: SExpr,
    from_context: BindContext,
    select_list: SelectList<'a>,
    global_view: SelectGlobalView,
}

struct AnalyzedSelect {
    s_expr: SExpr,
    from_context: BindContext,
    select_info: SelectInfo,
    having: Option<ScalarExpr>,
    qualify: Option<ScalarExpr>,
    order_items: OrderItems,
}

impl Binder {
    fn bind_select_source(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &SelectStmt,
    ) -> Result<(SExpr, BindContext)> {
        if stmt.from.is_empty() {
            return self.bind_dummy_table(bind_context, &stmt.select_list);
        }

        let mut max_column_position = MaxColumnPosition::default();
        stmt.walk(&mut max_column_position)?;
        self.metadata
            .write()
            .set_max_column_position(max_column_position.max_pos);

        let cross_joins = stmt
            .from
            .iter()
            .cloned()
            .reduce(|left, right| TableReference::Join {
                span: None,
                join: Join {
                    op: JoinOperator::CrossJoin,
                    condition: JoinCondition::None,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            })
            .unwrap();
        self.bind_table_reference(bind_context, &cross_joins)
    }

    fn prepare_select_binding<'a>(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &'a SelectStmt,
        order_by: &[OrderByExpr],
    ) -> Result<SelectPreparation<'a>> {
        let (s_expr, mut from_context) = self.bind_select_source(bind_context, stmt)?;

        // Try put window definitions into bind context.
        // This operation should be before `normalize_select_list` because window functions can be used in select list.
        self.analyze_window_definition(&mut from_context, &stmt.window_list)?;

        // Generate a analyzed select list with from context
        let mut select_list = self.normalize_select_list(&mut from_context, &stmt.select_list)?;

        // analyze set returning functions
        self.analyze_project_set_select(&mut from_context, &mut select_list)?;

        // Preserve the original select-item semantics for clause alias resolution
        // after SRF analysis. WHERE / QUALIFY still need the pre-aggregate and
        // pre-window expressions behind aliases, but SRF aliases must already point
        // at the ProjectSet-produced columns instead of expanding back to raw SRFs.
        let mut semantic_alias_catalog = select_list.alias_catalog();
        let group_by_aliases = semantic_alias_catalog.bindings_for(ExprContext::GroupClaue);

        // This will potentially add some alias group items to `from_context` if find some.
        if let Some(group_by) = stmt.group_by.as_ref() {
            self.analyze_group_items(&mut from_context, &select_list, &group_by_aliases, group_by)?;
        }

        self.analyze_aggregate_select(&mut from_context, &mut select_list)?;
        let udaf_names = self.find_and_load_udaf(
            &from_context,
            &select_list,
            stmt.having.as_ref(),
            stmt.qualify.as_ref(),
            order_by,
        )?;
        semantic_alias_catalog.analyze_aggregate_prepass_exprs(
            &select_list,
            &self.name_resolution_ctx,
            &udaf_names,
        );
        let SelectClauseFacts {
            having: _,
            qualify,
            order_by,
            aggregate_prepass_inputs,
        } = self.build_select_clause_facts(
            &udaf_names,
            &semantic_alias_catalog,
            stmt.having.as_ref(),
            stmt.qualify.as_ref(),
            order_by,
        );

        let aggregate_prepass_facts = self.derive_aggregate_prepass_facts(
            &udaf_names,
            &semantic_alias_catalog,
            aggregate_prepass_inputs.into_iter(),
        );
        self.bind_aggregate_prepass_facts(
            &mut from_context,
            semantic_alias_catalog.all_aliases(),
            &aggregate_prepass_facts,
        )?;

        // `analyze_window` should behind `analyze_aggregate_select`,
        // because `analyze_window` will rewrite the aggregate functions in the window function's arguments.
        self.analyze_window(&mut from_context, &mut select_list)?;

        debug_assert!(
            select_list
                .items
                .iter()
                .all(|item| !item.scalar.is_aggregate()),
            "SELECT projection expects aggregate/UDAF calls to be rewritten before projection analysis",
        );

        let global_view = SelectGlobalView {
            semantic_alias: semantic_alias_catalog,
            rewritten_alias: select_list.alias_catalog(),
            qualify,
            order_by,
        };

        Ok(SelectPreparation {
            s_expr,
            from_context,
            select_list,
            global_view,
        })
    }

    fn analyze_select_clauses(
        &mut self,
        stmt: &SelectStmt,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
        preparation: SelectPreparation<'_>,
    ) -> Result<AnalyzedSelect> {
        let SelectPreparation {
            mut s_expr,
            mut from_context,
            select_list,
            global_view,
        } = preparation;

        // Rewrite Set-returning functions, if the argument contains aggregation function or group item,
        // set as lazy Set-returning functions.
        if !from_context.srf_info.srfs.is_empty() {
            self.rewrite_project_set_select(&mut from_context)?;
        }

        // Bind Set-returning functions before filter plan and aggregate plan.
        if !from_context.srf_info.srfs.is_empty() {
            s_expr = self.bind_project_set(&mut from_context, s_expr, false)?;
        }

        // Bind WHERE after select-list analysis so aliases are available, but
        // resolve them against the original pre-rewrite select-item semantics.
        let where_scalar = if let Some(expr) = &stmt.selection {
            let (new_expr, scalar) = self.bind_where(
                &mut from_context,
                global_view.semantic_alias.all_aliases(),
                expr,
                s_expr,
            )?;
            s_expr = new_expr;
            Some(scalar)
        } else {
            None
        };

        // `analyze_projection` should behind `analyze_aggregate_select` because `analyze_aggregate_select` will rewrite `grouping`.
        let mut select_info = self.analyze_projection(&from_context, &select_list)?;

        let having = if let Some(having) = &stmt.having {
            Some(self.analyze_aggregate_having(
                &mut from_context,
                global_view.rewritten_alias.all_aliases(),
                having,
            )?)
        } else {
            None
        };

        let qualify = if let Some(qualify) = global_view.qualify.as_ref() {
            Some(self.analyze_window_qualify(
                &mut from_context,
                global_view.semantic_alias.all_aliases(),
                &qualify.expr_info.ast,
                qualify.contains_or_references_window(),
            )?)
        } else {
            None
        };

        let order_items = self.analyze_order_items(
            &mut from_context,
            &mut select_info,
            // Keep ORDER BY alias resolution on the same read-only semantic alias
            // snapshot used by the clause prepass. This avoids binding against
            // already-rewritten select-item scalars when a later clause only
            // needs the original alias semantics.
            global_view.semantic_alias.all_aliases(),
            Some(&global_view.order_by),
            order_by,
            stmt.distinct,
        )?;
        self.refresh_select_output(&from_context, &mut select_info)?;

        // After all analysis is done.
        if from_context.srf_info.srfs.is_empty() {
            // Ignore SRFs.
            self.analyze_lazy_materialization(
                &from_context,
                stmt,
                &select_info,
                &select_list,
                &where_scalar,
                &order_items.items,
                limit.unwrap_or_default(),
            )?;
        }

        Ok(AnalyzedSelect {
            s_expr,
            from_context,
            select_info,
            having,
            qualify,
            order_items,
        })
    }

    fn build_select_clause_facts(
        &self,
        udaf_names: &HashSet<String>,
        aliases: &SelectAliasCatalog,
        having: Option<&Expr>,
        qualify: Option<&Expr>,
        order_by: &[OrderByExpr],
    ) -> SelectClauseFacts {
        let alias_names = aliases.aggregate_prepass_alias_names();
        std::iter::chain(
            having
                .into_iter()
                .map(|expr| (expr, ExprContext::HavingClause)),
            qualify
                .into_iter()
                .map(|expr| (expr, ExprContext::QualifyClause)),
        )
        .chain(
            order_by
                .iter()
                .map(|order| (&order.expr, ExprContext::OrderByClause)),
        )
        .fold(
            SelectClauseFacts::default(),
            |mut facts, (expr, expr_context)| {
                let expr_info = AggregateExprInfo::analyze(
                    &self.name_resolution_ctx,
                    udaf_names,
                    &alias_names,
                    expr,
                );
                facts.merge_expr_info(expr_context, aliases, expr_info);
                facts
            },
        )
    }

    #[async_backtrace::framed]
    pub(crate) fn bind_select(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &SelectStmt,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
    ) -> Result<(SExpr, BindContext)> {
        if let Some(hints) = &stmt.hints {
            if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                warn!(
                    "In SELECT resolve optimize hints {:?} failed, err: {:?}",
                    hints, e
                );
            }
        }

        // whether allow rewrite virtual column and pushdown
        bind_context.allow_virtual_column = self
            .ctx
            .get_settings()
            .get_enable_experimental_virtual_column()
            .unwrap_or_default()
            && LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::VirtualColumn)
                .is_ok();

        let mut rewriter =
            SelectRewriter::new(self.name_resolution_ctx.unquoted_ident_case_sensitive)
                .with_subquery_executor(self.subquery_executor.clone());
        let new_stmt = rewriter.rewrite(stmt)?;
        let stmt = new_stmt.as_ref().unwrap_or(stmt);

        let preparation = self.prepare_select_binding(bind_context, stmt, order_by)?;
        let AnalyzedSelect {
            mut s_expr,
            mut from_context,
            mut select_info,
            having,
            qualify,
            order_items,
        } = self.analyze_select_clauses(stmt, order_by, limit, preparation)?;

        if from_context.aggregate_info.has_aggregate_calls()
            || from_context.aggregate_info.has_group_items()
        {
            s_expr = self.bind_aggregate(&mut from_context, s_expr)?;
        }

        if let Some(having) = having {
            s_expr = self.bind_having(&mut from_context, having, s_expr)?;
        }

        // bind window
        // window run after the HAVING clause but before the ORDER BY clause.
        for window_info in &from_context.windows.window_functions {
            s_expr = self.bind_window_function(window_info, s_expr)?;
        }

        // Bind lazy Set-returning functions after aggregate plan.
        if !from_context.srf_info.lazy_srf_set.is_empty() {
            s_expr = self.bind_project_set(&mut from_context, s_expr, true)?;
        }

        if let Some(qualify) = qualify {
            s_expr = self.bind_qualify(&mut from_context, qualify, s_expr)?;
        }

        if stmt.distinct {
            s_expr = self.bind_distinct(stmt.span, &mut select_info, s_expr)?;
        }

        s_expr = self.bind_projection(&mut from_context, select_info, s_expr)?;

        if !order_items.items.is_empty() {
            s_expr = self.bind_order_by(order_items, s_expr)?;
        }

        if from_context.have_async_func {
            // rewrite async function to async function plan
            let mut async_func_rewriter = AsyncFunctionRewriter::new(self.metadata.clone());
            s_expr = async_func_rewriter.rewrite(&s_expr)?;
        }

        // rewrite async function and udf
        s_expr = self.rewrite_udf(&mut from_context, s_expr)?;

        // add internal column binding into expr
        s_expr = self.add_internal_column_into_expr(&mut from_context, s_expr)?;

        s_expr = self.add_virtual_column_into_expr(&mut from_context, s_expr)?;

        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context
            .cte_context
            .set_cte_context(from_context.cte_context.clone());
        output_context.columns = from_context.columns;

        Ok((s_expr, output_context))
    }
}

/// It is useful when implementing some SQL syntax sugar,
///
/// to rewrite the SelectStmt, just add a new rewrite_* function and call it in the `rewrite` function.
struct SelectRewriter {
    new_stmt: Option<SelectStmt>,
    subquery_executor: Option<Arc<dyn QueryExecutor>>,
}

// helper functions to SelectRewriter
impl SelectRewriter {
    fn parse_aggregate_function(expr: &Expr) -> Result<(&Identifier, &[Expr])> {
        match expr {
            Expr::FunctionCall {
                func: FunctionCall { name, args, .. },
                ..
            } => Ok((name, args)),
            _ => {
                Err(ErrorCode::SyntaxException("Aggregate function is required")
                    .set_span(expr.span()))
            }
        }
    }

    fn expr_eq_from_col_and_value(col: Identifier, value: Expr) -> Expr {
        Expr::BinaryOp {
            span: None,
            left: Box::new(Expr::ColumnRef {
                span: None,
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(col),
                },
            }),
            op: BinaryOperator::Eq,
            right: Box::new(value),
        }
    }

    fn target_func_from_name_args(
        name: Identifier,
        args: Vec<Expr>,
        alias: Option<Identifier>,
    ) -> SelectTarget {
        SelectTarget::AliasedExpr {
            expr: Box::new(Expr::FunctionCall {
                span: Span::default(),
                func: FunctionCall {
                    distinct: false,
                    name,
                    args,
                    params: vec![],
                    order_by: vec![],
                    window: None,
                    lambda: None,
                },
            }),
            alias,
        }
    }

    fn expr_literal_array_from_unpivot_names(names: &[UnpivotName]) -> Expr {
        Array {
            span: Span::default(),
            exprs: names
                .iter()
                .map(|name| Expr::Literal {
                    span: name.ident.span,
                    value: Literal::String(
                        name.alias.as_ref().unwrap_or(&name.ident.name).to_string(),
                    ),
                })
                .collect(),
        }
    }

    fn expr_column_ref_array_from_vec_ident(exprs: Vec<Identifier>) -> Expr {
        Array {
            span: Span::default(),
            exprs: exprs
                .into_iter()
                .map(|expr| Expr::ColumnRef {
                    span: None,
                    column: ColumnRef {
                        database: None,
                        table: None,
                        column: ColumnID::Name(expr),
                    },
                })
                .collect(),
        }
    }
}

impl SelectRewriter {
    fn new(_is_unquoted_ident_case_sensitive: bool) -> Self {
        SelectRewriter {
            new_stmt: None,
            subquery_executor: None,
        }
    }

    // For Expr::Literal, expr.to_string() is quoted, sometimes we need the raw string.
    fn raw_string_from_literal_expr(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal { value, .. } => match value {
                Literal::String(v) => Some(v.clone()),
                _ => Some(expr.to_string()),
            },
            _ => None,
        }
    }

    pub fn with_subquery_executor(
        mut self,
        subquery_executor: Option<Arc<dyn QueryExecutor>>,
    ) -> Self {
        self.subquery_executor = subquery_executor;
        self
    }

    fn rewrite(&mut self, stmt: &SelectStmt) -> Result<Option<SelectStmt>> {
        self.rewrite_pivot(stmt)?;
        self.rewrite_unpivot(stmt)?;
        Ok(self.new_stmt.take())
    }

    fn rewrite_pivot(&mut self, stmt: &SelectStmt) -> Result<()> {
        if stmt.from.len() != 1 || stmt.from[0].pivot().is_none() {
            return Ok(());
        }
        let pivot = stmt.from[0].pivot().unwrap();
        let (aggregate_name, aggregate_args) = Self::parse_aggregate_function(&pivot.aggregate)?;
        let aggregate_args_names = aggregate_args
            .iter()
            .map(|expr| match expr {
                Expr::ColumnRef {
                    column:
                        ColumnRef {
                            column: ColumnID::Name(ident),
                            ..
                        },
                    ..
                } => Ok(ident.clone()),
                _ => Err(ErrorCode::SyntaxException(
                    "The aggregate function of pivot only support column_name",
                )
                .set_span(expr.span())),
            })
            .collect::<Result<Vec<_>>>()?;
        let mut exclude_columns = aggregate_args_names.clone();
        exclude_columns.push(pivot.value_column.clone());
        let mut star_target = SelectTarget::StarColumns {
            qualified: vec![Indirection::Star(None)],
            column_filter: None,
        };
        star_target.exclude(exclude_columns);
        let mut inner_select_list = vec![star_target];
        let new_aggregate_name = Identifier {
            name: format!("{}_if", aggregate_name.name),
            ..aggregate_name.clone()
        };

        // The values of pivot are divided into three categories: Column(Vec<Expr>), Subquery, and Any.
        // For Column, it must be literal. For Subquery, it should first be executed,
        // and the processing of the result will be consistent with that of Column.
        // For Any, we need to execute a DISTINCT query on the pivot column to get all unique values.
        // Therefore, the subquery can only return one column, and only return a string type.
        match &pivot.values {
            PivotValues::ColumnValues(values) => {
                let values = values
                    .iter()
                    .map(|value| {
                        let alias = Self::raw_string_from_literal_expr(value).ok_or_else(|| {
                            ErrorCode::SyntaxException("Pivot value should be literal")
                        })?;
                        Ok((value.clone(), alias))
                    })
                    .collect::<Result<Vec<_>>>()?;

                self.process_pivot_column_values(
                    pivot,
                    &values,
                    &new_aggregate_name,
                    aggregate_args,
                    &mut inner_select_list,
                    stmt,
                )?;
            }
            PivotValues::Subquery(subquery) => {
                let query_sql = subquery.to_string();
                if let Some(subquery_executor) = &self.subquery_executor {
                    let data_blocks = databend_common_base::runtime::block_on(async move {
                        subquery_executor
                            .execute_query_with_sql_string(&query_sql)
                            .await
                    })?;
                    let mut values =
                        self.extract_column_values_from_data_blocks(&data_blocks, subquery.span)?;
                    values.sort_by(|a, b| a.1.cmp(&b.1));
                    self.process_pivot_column_values(
                        pivot,
                        &values,
                        &new_aggregate_name,
                        aggregate_args,
                        &mut inner_select_list,
                        stmt,
                    )?;
                } else {
                    return Err(ErrorCode::Internal(
                        "SelectRewriter's Subquery executor is not set",
                    ));
                };
            }
            PivotValues::Any { order_by } => {
                if let Some(subquery_executor) = &self.subquery_executor {
                    // Build a query to get all distinct values from the pivot column
                    let mut query_sql = format!(
                        "SELECT DISTINCT {} FROM ({}) AS pivot_source",
                        pivot.value_column.name,
                        self.build_pivot_source_query(stmt)?
                    );

                    // Add ORDER BY if specified
                    if let Some(order_by_exprs) = order_by {
                        query_sql.push_str(" ORDER BY ");
                        for (i, order_expr) in order_by_exprs.iter().enumerate() {
                            if i > 0 {
                                query_sql.push_str(", ");
                            }
                            query_sql.push_str(&order_expr.to_string());
                        }
                    }

                    let data_blocks = databend_common_base::runtime::block_on(async move {
                        subquery_executor
                            .execute_query_with_sql_string(&query_sql)
                            .await
                    })?;
                    let values =
                        self.extract_column_values_from_data_blocks(&data_blocks, stmt.span)?;
                    self.process_pivot_column_values(
                        pivot,
                        &values,
                        &new_aggregate_name,
                        aggregate_args,
                        &mut inner_select_list,
                        stmt,
                    )?;
                } else {
                    return Err(ErrorCode::Internal(
                        "SelectRewriter's Subquery executor is not set",
                    ));
                };
            }
        }

        let mut inner_from = stmt.from[0].clone();
        Self::strip_pivot(&mut inner_from);

        let inner_stmt = SelectStmt {
            span: stmt.span,
            hints: None,
            distinct: false,
            top_n: None,
            select_list: inner_select_list,
            from: vec![inner_from],
            selection: None,
            group_by: Some(GroupBy::All),
            having: None,
            window_list: None,
            qualify: None,
        };

        let inner_query = Query {
            span: stmt.span,
            with: None,
            body: SetExpr::Select(Box::new(inner_stmt)),
            order_by: vec![],
            limit: vec![],
            offset: None,
            ignore_result: false,
        };

        let subquery_ref = TableReference::Subquery {
            span: Self::table_ref_span(&stmt.from[0]),
            lateral: false,
            subquery: Box::new(inner_query),
            alias: Some(Self::table_ref_alias(&stmt.from[0])),
            pivot: None,
            unpivot: None,
        };

        let mut outer_stmt = stmt.clone();
        outer_stmt.from = vec![subquery_ref];

        self.new_stmt = Some(outer_stmt);
        Ok(())
    }

    fn process_pivot_column_values(
        &self,
        pivot: &Pivot,
        values: &[(Expr, String)],
        new_aggregate_name: &Identifier,
        aggregate_args: &[Expr],
        new_select_list: &mut Vec<SelectTarget>,
        stmt: &SelectStmt,
    ) -> Result<()> {
        for (value, alias) in values {
            let mut args = aggregate_args.to_vec();
            args.push(Self::expr_eq_from_col_and_value(
                pivot.value_column.clone(),
                value.clone(),
            ));
            new_select_list.push(Self::target_func_from_name_args(
                new_aggregate_name.clone(),
                args,
                Some(Identifier::from_name(stmt.span, alias)),
            ));
        }
        Ok(())
    }

    fn extract_column_values_from_data_blocks(
        &self,
        data_blocks: &[DataBlock],
        span: Span,
    ) -> Result<Vec<(Expr, String)>> {
        let mut values = vec![];
        for block in data_blocks {
            if block.num_columns() != 1 {
                return Err(ErrorCode::SemanticError(
                    "The subquery of `pivot in` must return one column",
                )
                .set_span(span));
            }
            let columns = block.columns();
            // TODO: support more scalar into expr types
            for row in 0..block.num_rows() {
                let s = columns[0].index(row).unwrap();
                let data_type = columns[0].data_type();
                match s {
                    ScalarRef::String(s) => {
                        let literal = Expr::Literal {
                            span,
                            value: Literal::String(s.to_string()),
                        };
                        values.push((literal, s.to_string()));
                    }
                    ScalarRef::Null => {
                        let literal = Expr::Literal {
                            span,
                            value: Literal::Null,
                        };
                        values.push((literal, "NULL".to_string()));
                    }
                    other => {
                        let e = Expr::Cast {
                            span,
                            expr: Box::new(Expr::Literal {
                                span,
                                value: Literal::String(other.to_string()),
                            }),
                            target_type: data_type.to_type_name()?,
                            pg_style: false,
                        };
                        values.push((e, other.to_string()));
                    }
                }
            }
        }
        Ok(values)
    }

    fn strip_pivot(table_ref: &mut TableReference) {
        match table_ref {
            TableReference::Table { pivot, .. } => {
                *pivot = None;
            }
            TableReference::Subquery { pivot, .. } => {
                *pivot = None;
            }
            _ => {}
        }
    }

    fn table_ref_span(table_ref: &TableReference) -> Span {
        match table_ref {
            TableReference::Table { span, .. } => *span,
            TableReference::TableFunction { span, .. } => *span,
            TableReference::Subquery { span, .. } => *span,
            TableReference::Join { span, .. } => *span,
            TableReference::Location { span, .. } => *span,
        }
    }

    fn table_ref_alias(table_ref: &TableReference) -> TableAlias {
        match table_ref {
            TableReference::Table { table, alias, .. } => {
                alias.clone().unwrap_or_else(|| TableAlias {
                    name: table.table.clone(),
                    columns: vec![],
                    keep_database_name: true,
                })
            }
            TableReference::Subquery { alias, .. } => alias.clone().unwrap_or_else(|| TableAlias {
                name: Identifier::from_name(Self::table_ref_span(table_ref), "__pivot_subquery"),
                columns: vec![],
                keep_database_name: false,
            }),
            _ => TableAlias {
                name: Identifier::from_name(Self::table_ref_span(table_ref), "__pivot_subquery"),
                columns: vec![],
                keep_database_name: false,
            },
        }
    }

    fn build_pivot_source_query(&self, stmt: &SelectStmt) -> Result<String> {
        // Build the source query for the pivot table without the pivot clause
        // This is used to get distinct values for ANY pivot
        let mut source_query = String::new();

        // Start with SELECT clause
        // Add FROM clause (without pivot)
        if !stmt.from.is_empty() {
            source_query.push_str("SELECT *  FROM ");
            for (i, from_item) in stmt.from.iter().enumerate() {
                if i > 0 {
                    source_query.push_str(", ");
                }
                // Remove pivot from the from clause
                match from_item {
                    TableReference::Table {
                        span: _,
                        table,
                        alias,
                        temporal,
                        with_options,
                        pivot: _,
                        unpivot,
                        sample,
                    } => {
                        if let Some(catalog) = &table.catalog {
                            source_query.push_str(&catalog.name);
                            source_query.push('.');
                        }
                        if let Some(database) = &table.database {
                            source_query.push_str(&database.name);
                            source_query.push('.');
                        }
                        source_query.push_str(&table.table.name);
                        if let Some(branch) = &table.branch {
                            source_query.push('/');
                            source_query.push_str(&branch.name);
                        }

                        if let Some(temporal) = temporal {
                            source_query.push(' ');
                            source_query.push_str(&temporal.to_string());
                        }
                        if let Some(with_options) = with_options {
                            source_query.push(' ');
                            source_query.push_str(&with_options.to_string());
                        }
                        if let Some(alias) = alias {
                            source_query.push_str(" AS ");
                            source_query.push_str(&alias.to_string());
                        }
                        if let Some(unpivot) = unpivot {
                            source_query.push(' ');
                            source_query.push_str(&unpivot.to_string());
                        }
                        if let Some(sample) = sample {
                            source_query.push(' ');
                            source_query.push_str(&sample.to_string());
                        }
                    }
                    _ => {
                        source_query.push_str(&from_item.to_string());
                    }
                }
            }
        } else {
            return Err(ErrorCode::SemanticError(
                "The pivot source query must have a FROM clause",
            ));
        }

        // Add WHERE clause if present
        if let Some(where_clause) = &stmt.selection {
            source_query.push_str(" WHERE ");
            source_query.push_str(&where_clause.to_string());
        }

        // Add GROUP BY clause if present
        if let Some(group_by) = &stmt.group_by {
            source_query.push_str(" GROUP BY ");
            source_query.push_str(&group_by.to_string());
        }

        // Add HAVING clause if present
        if let Some(having) = &stmt.having {
            source_query.push_str(" HAVING ");
            source_query.push_str(&having.to_string());
        }

        Ok(source_query)
    }

    fn rewrite_unpivot(&mut self, stmt: &SelectStmt) -> Result<()> {
        if stmt.from.len() != 1 {
            return Ok(());
        }
        let Some(unpivot) = stmt.from[0].unpivot() else {
            return Ok(());
        };
        let mut new_select_list = stmt.select_list.clone();
        let columns = unpivot
            .column_names
            .iter()
            .map(|name| name.ident.to_owned())
            .collect::<Vec<_>>();
        if let Some(star) = new_select_list.iter_mut().find(|target| target.is_star()) {
            star.exclude(columns.clone());
        };
        new_select_list.push(Self::target_func_from_name_args(
            Identifier::from_name(stmt.span, "unnest"),
            vec![Self::expr_literal_array_from_unpivot_names(
                &unpivot.column_names,
            )],
            Some(unpivot.unpivot_column.clone()),
        ));
        new_select_list.push(Self::target_func_from_name_args(
            Identifier::from_name(stmt.span, "unnest"),
            vec![Self::expr_column_ref_array_from_vec_ident(columns)],
            Some(unpivot.value_column.clone()),
        ));

        if let Some(ref mut new_stmt) = self.new_stmt {
            new_stmt.select_list = new_select_list;
        } else {
            self.new_stmt = Some(SelectStmt {
                select_list: new_select_list,
                ..stmt.clone()
            });
        };
        Ok(())
    }
}

#[derive(Default)]
pub struct MaxColumnPosition {
    pub max_pos: usize,
}

impl Visitor for MaxColumnPosition {
    fn visit_expr(&mut self, expr: &Expr) -> std::result::Result<VisitControl, !> {
        if let Expr::ColumnRef {
            column:
                ColumnRef {
                    column: ColumnID::Position(pos),
                    ..
                },
            ..
        } = expr
            && pos.pos > self.max_pos
        {
            self.max_pos = pos.pos;
        }
        Ok(VisitControl::Continue)
    }
}
