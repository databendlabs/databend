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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::expand_aggregate_arg_exprs;
use common_planners::find_aggregate_exprs;
use common_planners::find_aggregate_exprs_in_expr;
use common_planners::rebase_expr;
use common_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::ast::Offset;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::SelectItem;
use sqlparser::ast::TableWithJoins;
use tracing::debug;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::statements::analyzer_statement::QueryAnalyzeState;
use crate::sql::statements::query::JoinedSchema;
use crate::sql::statements::query::JoinedSchemaAnalyzer;
use crate::sql::statements::query::JoinedTableDesc;
use crate::sql::statements::query::QualifiedRewriter;
use crate::sql::statements::query::QueryASTIR;
use crate::sql::statements::query::QueryCollectPushDowns;
use crate::sql::statements::query::QueryNormalizer;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::QueryRelation;
use crate::storages::ToReadDataSourcePlan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DfQueryStatement {
    pub distinct: bool,
    pub from: Vec<TableWithJoins>,
    pub projection: Vec<SelectItem>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Offset>,
    pub format: Option<String>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfQueryStatement {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let analyzer = JoinedSchemaAnalyzer::create(ctx.clone());
        let mut joined_schema = analyzer.analyze(self).await?;

        let mut ir = QueryNormalizer::normalize(ctx.clone(), self).await?;

        QualifiedRewriter::rewrite(&joined_schema, ctx.clone(), &mut ir)?;

        let has_aggregation = !find_aggregate_exprs(&ir.projection_expressions).is_empty();
        QueryCollectPushDowns::collect_extras(&mut ir, &mut joined_schema, has_aggregation)?;

        let analyze_state = self.analyze_query(ir).await?;
        debug!("analyze state is:\n{:?}", analyze_state);

        self.check_and_finalize(joined_schema, analyze_state, ctx)
            .await
    }
}

impl DfQueryStatement {
    async fn analyze_query(&self, ir: QueryASTIR) -> Result<QueryAnalyzeState> {
        let limit = ir.limit;
        let offset = ir.offset;
        let mut analyze_state = QueryAnalyzeState {
            limit,
            offset,
            ..Default::default()
        };

        if let Some(predicate) = &ir.filter_predicate {
            Self::verify_no_aggregate(predicate, "filter")?;
            analyze_state.filter = Some(predicate.clone());
        }

        Self::analyze_projection(&ir.projection_expressions, &mut analyze_state)?;

        // Allow `SELECT name FROM system.databases HAVING name = 'xxx'`
        if let Some(predicate) = &ir.having_predicate {
            analyze_state.having = Some(rebase_expr(predicate, &analyze_state.expressions)?);
        }

        for item in &ir.order_by_expressions {
            match item {
                Expression::Sort {
                    expr,
                    asc,
                    nulls_first,
                    origin_expr,
                } => {
                    analyze_state.add_expression(expr);
                    analyze_state.order_by_expressions.push(Expression::Sort {
                        expr: Box::new(rebase_expr(expr, &analyze_state.expressions)?),
                        asc: *asc,
                        nulls_first: *nulls_first,
                        origin_expr: Box::new(rebase_expr(
                            origin_expr,
                            &analyze_state.expressions,
                        )?),
                    });
                }
                _ => {
                    return Err(ErrorCode::SyntaxException(
                        "Order by must be sort expression. it's a bug.",
                    ));
                }
            }
        }

        if !ir.aggregate_expressions.is_empty() || !ir.group_by_expressions.is_empty() {
            // Rebase expressions using aggregate expressions and group by expressions
            let mut expressions = Vec::with_capacity(analyze_state.expressions.len());
            for expression in &analyze_state.expressions {
                let expression = rebase_expr(expression, &ir.aggregate_expressions)?;
                expressions.push(rebase_expr(&expression, &ir.group_by_expressions)?);
            }

            analyze_state.expressions = expressions;

            for group_expression in &ir.group_by_expressions {
                analyze_state.add_before_group_expression(group_expression);
                let base_exprs = &analyze_state.before_group_by_expressions;
                analyze_state
                    .group_by_expressions
                    .push(rebase_expr(group_expression, base_exprs)?);
            }

            Self::analyze_aggregate(&ir.aggregate_expressions, &mut analyze_state)?;
        }

        if !ir.window_expressions.is_empty() {
            Self::analyze_window(&ir.window_expressions, &mut analyze_state)?;
        }

        if ir.distinct {
            Self::analyze_distinct(&ir.projection_expressions, &mut analyze_state)?;
        }

        Ok(analyze_state)
    }

    fn analyze_aggregate(exprs: &[Expression], state: &mut QueryAnalyzeState) -> Result<()> {
        let aggregate_functions = find_aggregate_exprs(exprs);
        let aggregate_functions_args = expand_aggregate_arg_exprs(&aggregate_functions);

        for aggregate_function_arg in &aggregate_functions_args {
            state.add_before_group_expression(aggregate_function_arg);
        }

        for aggr_expression in exprs {
            let base_exprs = &state.before_group_by_expressions;
            state
                .aggregate_expressions
                .push(rebase_expr(aggr_expression, base_exprs)?);
        }

        Ok(())
    }

    fn analyze_distinct(
        projection_exprs: &[Expression],
        state: &mut QueryAnalyzeState,
    ) -> Result<()> {
        for item in projection_exprs {
            let distinct_expr = match item {
                Expression::Alias(_, expr) => *expr.clone(),
                _ => item.clone(),
            };

            let distinct_expr = rebase_expr(&distinct_expr, &state.expressions)?;
            let distinct_expr = rebase_expr(&distinct_expr, &state.group_by_expressions)?;
            let distinct_expr = rebase_expr(&distinct_expr, &state.aggregate_expressions)?;
            let distinct_expr = rebase_expr(&distinct_expr, &state.window_expressions)?;
            state.distinct_expressions.push(distinct_expr);
        }

        Ok(())
    }

    fn analyze_window(window_exprs: &[Expression], state: &mut QueryAnalyzeState) -> Result<()> {
        for expr in window_exprs {
            match expr {
                Expression::WindowFunction {
                    args,
                    partition_by,
                    order_by,
                    ..
                } => {
                    for arg in args {
                        state.add_expression(arg);
                    }
                    for partition_by_expr in partition_by {
                        state.add_expression(partition_by_expr);
                    }
                    for order_by_expr in order_by {
                        match order_by_expr {
                            Expression::Sort { expr, .. } => state.add_expression(expr),
                            _ => {
                                return Err(ErrorCode::LogicalError(format!(
                                    "Found non-sort expression {:?} while analyzing order by expressions of window expressions",
                                    order_by_expr
                                )));
                            }
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::LogicalError(format!(
                        "Found non-window expression {:?} while analyzing window expressions!",
                        expr
                    )));
                }
            }
        }

        for expr in window_exprs {
            let base_exprs = &state.expressions;
            state
                .window_expressions
                .push(rebase_expr(expr, base_exprs)?);
        }

        Ok(())
    }

    fn analyze_projection(exprs: &[Expression], state: &mut QueryAnalyzeState) -> Result<()> {
        for item in exprs {
            let expr = match item {
                Expression::Alias(_, expr) => expr,
                _ => item,
            };
            if !matches!(expr, Expression::WindowFunction { .. }) {
                state.add_expression(expr);
            }

            let rebased_expr = rebase_expr(item, &state.expressions)?;
            state.projection_expressions.push(rebased_expr);
        }

        Ok(())
    }

    fn verify_no_aggregate(expr: &Expression, info: &str) -> Result<()> {
        match find_aggregate_exprs_in_expr(expr).is_empty() {
            true => Ok(()),
            false => Err(ErrorCode::SyntaxException(format!(
                "{} cannot contain aggregate functions",
                info
            ))),
        }
    }
}

impl DfQueryStatement {
    pub async fn check_and_finalize(
        &self,
        schema: JoinedSchema,
        mut state: QueryAnalyzeState,
        ctx: Arc<QueryContext>,
    ) -> Result<AnalyzedResult> {
        let dry_run_res = Self::verify_with_dry_run(&schema, &state)?;
        state.finalize_schema = dry_run_res.schema().clone();
        debug!(
            "QueryAnalyzeState finalized schema:\n{}",
            state.finalize_schema
        );

        let mut tables_desc = schema.take_tables_desc();

        if tables_desc.len() != 1 {
            return Err(ErrorCode::UnImplement("Select join unimplemented yet."));
        }

        match tables_desc.remove(0) {
            JoinedTableDesc::Table {
                table,
                push_downs,
                name_parts,
                ..
            } => {
                let catalog_name = Self::resolve_catalog(&ctx, &name_parts)?;
                let source_plan = table
                    .read_plan_with_catalog(ctx.clone(), catalog_name, *push_downs)
                    .await?;
                state.relation = QueryRelation::FromTable(Box::new(source_plan));
            }
            JoinedTableDesc::Subquery {
                state: subquery_state,
                ..
            } => {
                // TODO: maybe need reanalyze subquery.
                state.relation = QueryRelation::Nested(subquery_state);
            }
        }

        Ok(AnalyzedResult::SelectQuery(Box::new(state)))
    }

    fn resolve_catalog(ctx: &QueryContext, idents: &[String]) -> Result<String> {
        match idents.len() {
            // for table_functions, idents.len() == 0
            0 | 1 | 2 => Ok(ctx.get_current_catalog()),
            3 => Ok(idents[0].clone()),
            _ => Err(ErrorCode::SyntaxException(
                "table name should be [`catalog`].[`db`].`table` in statement",
            )),
        }
    }

    fn verify_with_dry_run(schema: &JoinedSchema, state: &QueryAnalyzeState) -> Result<DataBlock> {
        let mut data_block = DataBlock::empty_with_schema(schema.to_data_schema());

        if let Some(predicate) = &state.filter {
            if let Err(cause) = Self::dry_run_expr(predicate, &data_block) {
                return Err(cause.add_message_back(" (while in select filter)"));
            }
        }

        if !state.before_group_by_expressions.is_empty() {
            match Self::dry_run_exprs(&state.before_group_by_expressions, &data_block) {
                Ok(res) => {
                    data_block = res;
                }
                Err(cause) => {
                    return Err(cause.add_message_back(" (while in select before group by)"));
                }
            }
        }

        if !state.group_by_expressions.is_empty() || !state.aggregate_expressions.is_empty() {
            let new_len = state.aggregate_expressions.len() + state.group_by_expressions.len();
            let mut new_expression = Vec::with_capacity(new_len);

            for group_by_expression in &state.group_by_expressions {
                new_expression.push(group_by_expression);
            }

            for aggregate_expression in &state.aggregate_expressions {
                new_expression.push(aggregate_expression);
            }

            match Self::dry_run_exprs_ref(&new_expression, &data_block) {
                Ok(res) => {
                    data_block = res;
                }
                Err(cause) => {
                    return Err(cause.add_message_back(" (while in select group by)"));
                }
            }
        }

        if !state.expressions.is_empty() {
            match Self::dry_run_exprs(&state.expressions, &data_block) {
                Ok(res) => {
                    data_block = res;
                }
                Err(cause) if state.order_by_expressions.is_empty() => {
                    return Err(cause.add_message_back(" (while in select before projection)"));
                }
                Err(cause) => {
                    return Err(cause.add_message_back(" (while in select before order by)"));
                }
            }
        }

        if let Some(predicate) = &state.having {
            if let Err(cause) = Self::dry_run_expr(predicate, &data_block) {
                return Err(cause.add_message_back(" (while in select having)"));
            }
        }

        if !state.window_expressions.is_empty() {
            let new_len = state.window_expressions.len() + state.expressions.len();
            let mut new_expression = Vec::with_capacity(new_len);

            for expr in &state.window_expressions {
                new_expression.push(expr);
            }

            for expr in &state.expressions {
                new_expression.push(expr);
            }

            match Self::dry_run_exprs_ref(&new_expression, &data_block) {
                Ok(res) => {
                    data_block = res;
                }
                Err(cause) => {
                    return Err(cause.add_message_back(" (while in select window func)"));
                }
            }
        }

        if !state.order_by_expressions.is_empty() {
            if let Err(cause) = Self::dry_run_exprs(&state.order_by_expressions, &data_block) {
                return Err(cause.add_message_back(" (while in select order by)"));
            }
        }

        if !state.projection_expressions.is_empty() {
            match Self::dry_run_exprs(&state.projection_expressions, &data_block) {
                Ok(res) => {
                    data_block = res;
                }
                Err(cause) => {
                    return Err(cause.add_message_back(" (while in select projection)"));
                }
            }
        }

        Ok(data_block)
    }

    fn dry_run_expr(expr: &Expression, data: &DataBlock) -> Result<DataBlock> {
        let schema = data.schema();
        let data_field = expr.to_data_field(schema)?;
        Ok(DataBlock::empty_with_schema(DataSchemaRefExt::create(
            vec![data_field],
        )))
    }

    fn dry_run_exprs(exprs: &[Expression], data: &DataBlock) -> Result<DataBlock> {
        let schema = data.schema();
        let mut new_data_fields = Vec::with_capacity(exprs.len());

        for expr in exprs {
            new_data_fields.push(expr.to_data_field(schema)?);
        }

        Ok(DataBlock::empty_with_schema(DataSchemaRefExt::create(
            new_data_fields,
        )))
    }

    fn dry_run_exprs_ref(exprs: &[&Expression], data: &DataBlock) -> Result<DataBlock> {
        let schema = data.schema();
        let mut new_data_fields = Vec::with_capacity(exprs.len());

        for expr in exprs {
            new_data_fields.push(expr.to_data_field(schema)?);
        }

        Ok(DataBlock::empty_with_schema(DataSchemaRefExt::create(
            new_data_fields,
        )))
    }
}
