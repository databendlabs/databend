use sqlparser::ast::{Expr, Offset, OrderByExpr, SelectItem, TableWithJoins};

use common_exception::{ErrorCode, Result};
use common_planners::{expand_aggregate_arg_exprs, expr_as_column_expr, Expression, extract_aliases, find_aggregate_exprs_in_expr, ReadDataSourcePlan, rebase_expr, resolve_aliases_to_exprs};

use crate::sessions::{DatabendQueryContextRef};
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sql::statements::analyzer_expr::{ExpressionAnalyzer};
use crate::sql::statements::analyzer_statement::QueryAnalyzeState;
use crate::sql::statements::query::{AnalyzeQuerySchema, AnalyzeQueryColumnDesc, FromAnalyzer};
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
        let normal_transform = QueryNormalizer::create(ctx.clone());
        let normalized_result = normal_transform.transform(self).await?;

        let from_analyzer = FromAnalyzer::create(ctx.clone());
        let query_schema = from_analyzer.analyze(self).await?;

        match normalized_result.group_by_expressions.is_empty() && normalized_result.aggregate_expressions.is_empty() {
            true => self.analyze_without_aggr(query_schema, normalized_result).await,
            false => self.analyze_with_aggr(query_schema, normalized_result).await
        }
    }
}

impl DfQueryStatement {
    async fn analyze_with_aggr(&self, schema: AnalyzeQuerySchema, data: QueryNormalizerData) -> Result<AnalyzedResult> {
        unimplemented!()
    }

    async fn analyze_without_aggr(&self, schema: AnalyzeQuerySchema, data: QueryNormalizerData) -> Result<AnalyzedResult> {
        let mut analyze_state = QueryAnalyzeState::default();

        if let Some(predicate) = &data.filter_predicate {
            Self::verify_no_aggregate(predicate, "filter")?;
            analyze_state.filter = Some(predicate.clone());
        }

        for item in &data.projection_expressions {
            // TODO:
            match item {
                Expression::Alias(_, expr) => analyze_state.add_expression(expr),
                _ => analyze_state.add_expression(item),
            }
        }

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
                    // TODO: 需要注意如果推送的expression与原有的expression产生冲突, 我们需要rename order by
                    analyze_state.add_expression(&expr);
                    analyze_state.order_by_expression.push(Expression::Sort {
                        expr: Box::new(rebase_expr(&expr, &analyze_state.expressions)?),
                        asc: *asc,
                        nulls_first: *nulls_first,
                    });
                }
                _ => { return Err(ErrorCode::SyntaxException("Order by must be sort expression. it's a bug.")); }
            }
        }

        Ok(AnalyzedResult::SelectQuery(analyze_state))
    }

    fn verify_no_aggregate(expr: &Expression, info: &str) -> Result<()> {
        unimplemented!()
    }
}

