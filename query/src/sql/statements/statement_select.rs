use std::collections::HashMap;
use sqlparser::ast::{Expr, Offset, OrderByExpr, SelectItem, TableWithJoins};

use common_exception::{ErrorCode, Result};
use common_planners::{expand_aggregate_arg_exprs, expr_as_column_expr, Expression, extract_aliases, find_aggregate_exprs, find_aggregate_exprs_in_expr, ReadDataSourcePlan, rebase_expr, resolve_aliases_to_exprs};

use crate::sessions::{DatabendQueryContextRef};
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sql::statements::analyzer_expr::{ExpressionAnalyzer};
use crate::sql::statements::analyzer_statement::QueryAnalyzeState;
use crate::sql::statements::query::{AnalyzeQuerySchema, AnalyzeQueryColumnDesc, FromAnalyzer, QualifiedRewriter};
use crate::sql::statements::query::{QueryNormalizerData, QueryNormalizer};

#[derive(Debug, Clone, PartialEq)]
pub struct DfQueryStatement {
    pub from: Vec<TableWithJoins>,
    pub projection: Vec<SelectItem>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Offset>,
}

pub enum QueryRelation {
    FromTable(ReadDataSourcePlan),
    Nested(Box<QueryNormalizerData>),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfQueryStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let from_analyzer = FromAnalyzer::create(ctx.clone());
        let analyzed_from_schema = from_analyzer.analyze(self).await?;

        let normal_transform = QueryNormalizer::create(ctx.clone());
        let normalized_result = normal_transform.transform(self).await?;

        let schema = analyzed_from_schema.clone();
        let qualified_rewriter = QualifiedRewriter::create(schema, ctx.clone());
        let normalized_result = qualified_rewriter.rewrite(normalized_result).await?;

        self.analyze_query(normalized_result).await
    }
}

impl DfQueryStatement {
    async fn analyze_query(&self, data: QueryNormalizerData) -> Result<AnalyzedResult> {
        let mut analyze_state = QueryAnalyzeState::default();

        if let Some(predicate) = &data.filter_predicate {
            Self::verify_no_aggregate(predicate, "filter")?;
            analyze_state.filter = Some(predicate.clone());
        }

        Self::analyze_projection(&data.projection_expressions, &mut analyze_state)?;

        // Allow `SELECT name FROM system.databases HAVING name = 'xxx'`
        if let Some(predicate) = &data.having_predicate {
            // TODO: We can also push having into expressions, which helps:
            //     - SELECT number + 5 AS number FROM numbers(100) HAVING number = 5;
            //     - SELECT number FROM numbers(100) HAVING number + 5 > 5 ORDER BY number + 5 > 5 (bad sql)
            analyze_state.having = Some(predicate.clone());
        }

        for item in &data.order_by_expressions {
            match item {
                Expression::Sort { expr, asc, nulls_first } => {
                    analyze_state.add_expression(&expr);
                    analyze_state.order_by_expressions.push(Expression::Sort {
                        expr: Box::new(rebase_expr(&expr, &analyze_state.expressions)?),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    });
                }
                _ => { return Err(ErrorCode::SyntaxException("Order by must be sort expression. it's a bug.")); }
            }
        }

        if !data.aggregate_expressions.is_empty() || !data.group_by_expressions.is_empty() {
            // Rebase expressions using aggregate expressions and group by expressions
            let mut expressions = Vec::with_capacity(analyze_state.expressions.len());
            for expression in &analyze_state.expressions {
                let expression = rebase_expr(expression, &data.group_by_expressions)?;
                expressions.push(rebase_expr(&expression, &data.aggregate_expressions)?);
            }

            analyze_state.expressions = expressions;

            for group_expression in &data.group_by_expressions {
                analyze_state.add_before_group_expression(group_expression);
                let base_exprs = &analyze_state.before_group_by_expressions;
                analyze_state.group_by_expressions.push(rebase_expr(group_expression, base_exprs)?);
            }

            Self::analyze_aggregate(&data.aggregate_expressions, &mut analyze_state)?;
        }

        Ok(AnalyzedResult::SelectQuery(analyze_state))
    }

    fn analyze_aggregate(exprs: &[Expression], state: &mut QueryAnalyzeState) -> Result<()> {
        let aggregate_functions = find_aggregate_exprs(exprs);
        let aggregate_functions_args = expand_aggregate_arg_exprs(&aggregate_functions);

        for aggregate_function_arg in &aggregate_functions_args {
            state.add_before_group_expression(aggregate_function_arg);
        }

        for aggr_expression in exprs {
            let base_exprs = &state.before_group_by_expressions;
            state.aggregate_expressions.push(rebase_expr(aggr_expression, base_exprs)?);
        }

        Ok(())
    }

    fn analyze_projection(exprs: &[Expression], state: &mut QueryAnalyzeState) -> Result<()> {
        for item in exprs {
            match item {
                Expression::Alias(_, expr) => state.add_expression(expr),
                _ => state.add_expression(item),
            }

            let rebased_expr = rebase_expr(item, &state.expressions)?;
            state.projection_expressions.push(rebased_expr);
        }

        Ok(())
    }

    fn verify_no_aggregate(expr: &Expression, info: &str) -> Result<()> {
        match find_aggregate_exprs_in_expr(expr).is_empty() {
            true => Ok(()),
            false => Err(ErrorCode::SyntaxException(format!("{} cannot contain aggregate functions", info))),
        }
    }
}

