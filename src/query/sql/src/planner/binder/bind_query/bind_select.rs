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
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::LambdaArgument;
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
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::Walk;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::display::scalar_ref_to_string;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use unicase::Ascii;

use crate::AsyncFunctionRewriter;
use crate::NameResolutionContext;
use crate::optimizer::ir::SExpr;
use crate::planner::QueryExecutor;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::GroupedSelectItems;
use crate::planner::binder::ScalarFoldingPolicy;
use crate::planner::binder::SelectItems;
use crate::planner::binder::aggregate::ExpandedGroup;
use crate::planner::binder::project::SelectInfo;
use crate::planner::binder::select::SelectAliasCatalog;
use crate::planner::binder::select::SelectList;
use crate::planner::binder::select_aggregates::SelectAggregates;
use crate::planner::binder::sort::OrderItems;
use crate::planner::binder::window::WindowInputColumns;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut as _;

struct SelectGlobalView {
    semantic_alias: SelectAliasCatalog,
    rewritten_alias: SelectAliasCatalog,
}

impl SelectGlobalView {
    fn where_aliases(&self) -> &[(String, ScalarExpr)] {
        self.semantic_alias.all_aliases()
    }

    fn having_aliases(&self) -> &[(String, ScalarExpr)] {
        self.rewritten_alias.all_aliases()
    }

    fn qualify_aliases(&self) -> &[(String, ScalarExpr)] {
        self.semantic_alias.all_aliases()
    }

    fn order_by_aliases(&self) -> &[(String, ScalarExpr)] {
        self.semantic_alias.all_aliases()
    }
}

struct SelectPreparation<'a> {
    s_expr: SExpr,
    from_context: BindContext,
    select_list: SelectList<'a>,
    global_view: SelectGlobalView,
    /// The WHERE predicate if it is bound before the SELECT list, as under
    /// GROUPING SETS.
    where_predicate: Option<ScalarExpr>,
    /// The SELECT items of a grouped query.
    select_items: Option<GroupedSelectItems<'a>>,
}

/// The SELECT list, and for grouped queries the WHERE predicate and shared
/// pre-aggregate item forms bound with it.
pub(crate) struct BoundSelectList<'a> {
    pub(crate) select_list: SelectList<'a>,
    pub(crate) semantic_alias: SelectAliasCatalog,
    where_predicate: Option<ScalarExpr>,
    select_items: Option<GroupedSelectItems<'a>>,
}

struct AnalyzedSelect {
    s_expr: SExpr,
    /// Whether the SELECT is grouped, i.e. an Aggregate is built.
    grouped: bool,
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

        let mut max_column_position = MaxColumnPosition::new(self.name_resolution_ctx.clone());
        stmt.walk(&mut max_column_position)?;
        self.metadata.write().set_stage_column_references(
            max_column_position.max_pos,
            max_column_position.has_name_ref,
        );

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
        from_context.definition_scope = self.definition_scope.clone();

        // Try put window definitions into bind context.
        // This operation should be before `normalize_select_list` because window functions can be used in select list.
        self.analyze_window_definition(&mut from_context, &stmt.window_list)?;

        // Whether the SELECT is grouped depends on all of its clauses, which are
        // bound later, so it is decided from their lowered forms.
        let SelectAggregates {
            udaf_names,
            contains_aggregate,
        } = self.analyze_select_aggregates(
            &from_context,
            &stmt.select_list,
            stmt.having.as_ref(),
            stmt.qualify.as_ref(),
            order_by,
        )?;
        let grouped = stmt.group_by.is_some() || contains_aggregate;

        let BoundSelectList {
            mut select_list,
            semantic_alias,
            where_predicate,
            select_items,
        } = if grouped {
            self.bind_grouped_select_list(
                &mut from_context,
                &stmt.select_list,
                stmt.group_by.as_ref(),
                stmt.selection.as_ref(),
                &udaf_names,
            )?
        } else {
            // Generate a analyzed select list with from context
            let mut select_list =
                self.normalize_select_list(&mut from_context, &stmt.select_list)?;

            // analyze set returning functions
            self.analyze_project_set_select(&mut from_context, &mut select_list)?;

            // Preserve the original select-item semantics for clause alias resolution
            // after SRF analysis. WHERE / QUALIFY still need the pre-aggregate and
            // pre-window expressions behind aliases, but SRF aliases must already point
            // at the ProjectSet-produced columns instead of expanding back to raw SRFs.
            let semantic_alias = select_list.alias_catalog();

            BoundSelectList {
                select_list,
                semantic_alias,
                where_predicate: None,
                select_items: None,
            }
        };

        self.analyze_aggregate_select(&mut from_context, &mut select_list)?;

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
            semantic_alias,
            rewritten_alias: select_list.alias_catalog(),
        };

        Ok(SelectPreparation {
            s_expr,
            from_context,
            select_list,
            global_view,
            where_predicate,
            select_items,
        })
    }

    /// Bind keys and WHERE against input forms, then bind the SELECT list
    /// against key outputs. Ordinary keys keep their original types; grouping
    /// sets expose dedicated nullable outputs. A global aggregate has no keys.
    pub(crate) fn bind_grouped_select_list<'a>(
        &mut self,
        from_context: &mut BindContext,
        select_targets: &'a [SelectTarget],
        group_by: Option<&GroupBy>,
        selection: Option<&Expr>,
        udaf_names: &HashSet<String>,
    ) -> Result<BoundSelectList<'a>> {
        // The items are lowered into an arena that every clause shares, see
        // `grouped_select.rs`.
        let exprs = self.lower_grouped_select_list(from_context, select_targets)?;
        let group_by = group_by
            .map(|group_by| self.expand_group_by(group_by, &exprs, udaf_names, from_context))
            .transpose()?;
        // The keys, and every pre-aggregate form matched against them, are
        // bound with this policy.
        let folding_policy = group_by
            .as_ref()
            .map_or(ScalarFoldingPolicy::Normal, ExpandedGroup::folding_policy);
        let mut items = GroupedSelectItems::new(exprs, folding_policy);
        if let Some(group_by) = &group_by {
            self.analyze_group_items(
                from_context,
                SelectItems::new(&items.exprs, &mut items.forms),
                &items.exprs.group_by_bindings(),
                group_by,
            )?;
        }

        // WHERE is evaluated before the aggregate. Bind it before the SELECT
        // list, so that whatever its items register, e.g. set-returning
        // functions, is registered before the aggregate.
        let where_predicate = selection
            .map(|expr| self.bind_grouped_where(from_context, items.lend(), expr))
            .transpose()?;

        Self::prepare_grouping_scope(from_context, folding_policy);
        let mut select_list =
            self.normalize_grouped_select_list(from_context, select_targets, items.lend())?;
        self.analyze_project_set_select(from_context, &mut select_list)?;
        let semantic_alias = select_list.alias_catalog();
        Ok(BoundSelectList {
            select_list,
            semantic_alias,
            where_predicate,
            select_items: Some(items),
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
            where_predicate,
            mut select_items,
        } = preparation;
        let grouped = select_items.is_some();

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
        let where_scalar = if let Some(predicate) = where_predicate {
            s_expr = Self::where_filter(&predicate, s_expr);
            Some(predicate)
        } else if let Some(expr) = &stmt.selection {
            let alias_exprs =
                select_list
                    .items
                    .iter()
                    .filter_map(|item| match item.select_target {
                        SelectTarget::AliasedExpr { expr, .. } => {
                            Some((item.alias.clone(), expr.as_ref().clone()))
                        }
                        _ => None,
                    });
            let (new_expr, scalar) = self.bind_select_where(
                &mut from_context,
                global_view.where_aliases(),
                alias_exprs,
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
                global_view.having_aliases(),
                select_items.as_mut().map(GroupedSelectItems::lend),
                having,
            )?)
        } else {
            None
        };

        let qualify = if let Some(qualify) = &stmt.qualify {
            Some(self.analyze_window_qualify(
                &mut from_context,
                global_view.qualify_aliases(),
                select_items.as_mut().map(GroupedSelectItems::lend),
                qualify,
            )?)
        } else {
            None
        };

        let order_items = self.analyze_order_items(
            &mut from_context,
            &mut select_info,
            // ORDER BY aliases resolve to the SELECT items as bound, before
            // their aggregates and windows are rewritten to outputs.
            global_view.order_by_aliases(),
            select_items.as_mut().map(GroupedSelectItems::lend),
            order_by,
            stmt.distinct,
        )?;
        self.refresh_select_output(&from_context, &mut select_info)?;
        // All post-aggregate expressions are bound.
        from_context.grouping_key_scope = None;

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
            grouped,
            from_context,
            select_info,
            having,
            qualify,
            order_items,
        })
    }

    #[async_backtrace::framed]
    pub(crate) fn bind_select(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &SelectStmt,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
    ) -> Result<(SExpr, BindContext)> {
        self.debug_assert_definition_scope(bind_context);
        // whether allow rewrite virtual column and pushdown
        bind_context.allow_virtual_column = self.is_virtual_column_rewrite_enabled();

        let mut rewriter =
            SelectRewriter::new(self.name_resolution_ctx.unquoted_ident_case_sensitive)
                .with_subquery_executor(self.subquery_executor.clone());
        let new_stmt = rewriter.rewrite(stmt)?;
        let stmt = new_stmt.as_ref().unwrap_or(stmt);

        let preparation = self.prepare_select_binding(bind_context, stmt, order_by)?;
        let AnalyzedSelect {
            mut s_expr,
            grouped,
            mut from_context,
            mut select_info,
            having,
            qualify,
            order_items,
        } = self.analyze_select_clauses(stmt, order_by, limit, preparation)?;

        if grouped {
            s_expr = self.bind_aggregate(&mut from_context, s_expr)?;
        }

        if let Some(having) = having {
            s_expr = self.bind_having(&mut from_context, having, s_expr)?;
        }

        // bind window
        // window run after the HAVING clause but before the ORDER BY clause.
        let mut window_inputs = WindowInputColumns::default();
        if !from_context.windows.window_functions.is_empty() {
            (s_expr, window_inputs) =
                self.bind_window_functions(&from_context.windows.window_functions, s_expr)?;
            for item in select_info.projection_scalars.values_mut() {
                window_inputs.visit(&mut item.scalar)?;
            }
        }

        // Bind lazy Set-returning functions after aggregate plan.
        if from_context.srf_info.has_lazy() {
            s_expr = self.bind_project_set(&mut from_context, s_expr, true)?;
            // Preserve the existing reuse boundary: extending QUALIFY reuse
            // across ProjectSet row expansion is outside this refactor's scope.
            window_inputs = WindowInputColumns::default();
        }

        if let Some(qualify) = qualify {
            s_expr = self.bind_qualify(&mut from_context, qualify, s_expr, &mut window_inputs)?;
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

        // add internal and virtual column bindings into expr
        s_expr = self.add_bound_columns_into_expr(&mut from_context, s_expr)?;

        let mut output_context = BindContext::new();
        output_context.binding_depth = from_context.depth();
        output_context.definition_scope = from_context.definition_scope;
        output_context.binding_views = from_context.binding_views;
        output_context.parent = from_context.parent;
        output_context
            .cte_context
            .set_cte_context(from_context.cte_context.clone());
        output_context.allow_virtual_column = from_context.allow_virtual_column;
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
                func: FunctionCall {
                    name, args, filter, ..
                },
                ..
            } => {
                if filter.is_some() {
                    return Err(ErrorCode::Unimplemented(
                        "PIVOT aggregate FILTER is not supported yet",
                    )
                    .set_span(expr.span()));
                }
                Ok((name, args))
            }
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
                    filter: None,
                    window: None,
                    lambda: None,
                },
            }),
            alias,
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
                        pivot.value_column,
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
            for row in 0..block.num_rows() {
                let s = columns[0].index(row).unwrap();
                let data_type = columns[0].data_type();
                let value = scalar_ref_to_string(&s);
                match s {
                    ScalarRef::Null => {
                        let literal = Expr::Literal {
                            span,
                            value: Literal::Null,
                        };
                        values.push((literal, value));
                    }
                    _ => {
                        let e = Expr::Cast {
                            span,
                            expr: Box::new(Expr::Literal {
                                span,
                                value: Literal::String(value.clone()),
                            }),
                            target_type: data_type.to_type_name()?,
                            pg_style: false,
                        };
                        values.push((e, value));
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
                let mut from_item = from_item.clone();
                Self::strip_pivot(&mut from_item);
                source_query.push_str(&from_item.to_string());
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
}

#[derive(Default)]
pub struct MaxColumnPosition {
    pub max_pos: usize,
    pub has_name_ref: bool,
    name_resolution_ctx: NameResolutionContext,
    lambda_params: Vec<HashSet<String>>,
}

impl MaxColumnPosition {
    pub fn new(name_resolution_ctx: NameResolutionContext) -> Self {
        Self {
            name_resolution_ctx,
            ..Default::default()
        }
    }
}

impl Visitor for MaxColumnPosition {
    fn visit_expr(&mut self, expr: &Expr) -> std::result::Result<VisitControl, !> {
        if let Expr::ColumnRef { column, .. } = expr {
            match &column.column {
                ColumnID::Position(pos) if pos.pos > self.max_pos => {
                    self.max_pos = pos.pos;
                }
                ColumnID::Name(name)
                    if column.database.is_none()
                        && column.table.is_none()
                        && self.lambda_params.iter().rev().any(|params| {
                            params
                                .contains(&self.name_resolution_ctx.normalize_identifier(name).name)
                        }) =>
                {
                    // Lambda parameters are resolved locally and do not refer
                    // to columns in the stage schema.
                }
                ColumnID::Name(_) => {
                    self.has_name_ref = true;
                }
                _ => {}
            }
        }
        Ok(VisitControl::Continue)
    }

    fn visit_function_call(&mut self, func: &FunctionCall) -> std::result::Result<VisitControl, !> {
        let Some(lambda_arg) = &func.lambda else {
            return Ok(VisitControl::Continue);
        };

        let semantic_lambda = match lambda_arg {
            LambdaArgument::Lambda(lambda) => Some((func.args.as_slice(), lambda)),
            LambdaArgument::Ambiguous(lambda)
                if GENERAL_LAMBDA_FUNCTIONS.contains(&Ascii::new(func.name.name.as_str())) =>
            {
                let Some((_, args)) = func.args.split_last() else {
                    return Ok(VisitControl::Continue);
                };
                Some((args, lambda))
            }
            // For non-lambda functions the trailing arrow remains an ordinary
            // JSON expression, so let the normal AST walk visit `args`.
            LambdaArgument::Ambiguous(_) => None,
        };
        let Some((args, lambda)) = semantic_lambda else {
            return Ok(VisitControl::Continue);
        };

        for arg in args {
            if let VisitControl::Break(value) = arg.walk(self)? {
                return Ok(VisitControl::Break(value));
            }
        }

        self.lambda_params.push(
            lambda
                .params
                .iter()
                .map(|param| self.name_resolution_ctx.normalize_identifier(param).name)
                .collect(),
        );
        let result = lambda.expr.walk(self);
        self.lambda_params.pop();
        match result? {
            VisitControl::Break(value) => Ok(VisitControl::Break(value)),
            VisitControl::Continue | VisitControl::SkipChildren => Ok(VisitControl::SkipChildren),
        }
    }
}

#[cfg(test)]
mod max_column_position_tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_expr;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    fn has_column_name_ref(sql: &str) -> bool {
        let tokens = tokenize_sql(sql).unwrap();
        let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
        let mut visitor = MaxColumnPosition::default();
        expr.walk(&mut visitor).unwrap();
        visitor.has_name_ref
    }

    #[test]
    fn ignores_lambda_params_but_preserves_ordinary_json_arrow_columns() {
        assert!(!has_column_name_ref("array_transform([1], x -> 1)"));
        assert!(!has_column_name_ref("array_transform([1], x -> x)"));
        assert!(!has_column_name_ref("array_transform([1], X -> x)"));
        assert!(has_column_name_ref(
            "array_transform([1], x -> x + stage_column)"
        ));
        assert!(has_column_name_ref("concat(1, 2, doc -> 'key')"));
    }
}
