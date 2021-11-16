use sqlparser::ast::{Expr, Offset, OrderByExpr, SelectItem, TableWithJoins};

use common_exception::{ErrorCode, Result};
use common_planners::{expand_aggregate_arg_exprs, expand_wildcard, expr_as_column_expr, Expression, extract_aliases, find_aggregate_exprs_in_expr, ReadDataSourcePlan, rebase_expr, resolve_aliases_to_exprs};
use common_planners::PlanNode::Expression;

use crate::sessions::{DatabendQueryContextRef};
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sql::statements::analyzer_expr::{ExpressionAnalyzer};
use crate::sql::statements::analyzer_schema::{AnalyzeQuerySchema, AnalyzeQueryColumnDesc};
use crate::sql::statements::statement_select_analyze_data::AnalyzeQueryState;

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
    Nested(Box<AnalyzeQueryState>),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfQueryStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let mut data = AnalyzeQueryState::create(ctx, &self.from).await?;

        if let Err(cause) = self.analyze_filter(&mut data).await {
            return Err(cause.add_message_back(" (while in analyze select filter)."));
        }

        // We will analyze the projection before GROUP BY, HAVING, and ORDER BY,
        // because they may access the columns in the projection. Such as:
        // SELECT a + 1 AS b, SUM(c) FROM table_a GROUP BY b;
        if let Err(cause) = self.analyze_projection(&mut data).await {
            return Err(cause.add_message_back(" (while in analyze select projection)."));
        }

        if let Err(cause) = self.analyze_group_by(&mut data).await {
            return Err(cause.add_message_back(" (while in analyze select group by)."));
        }

        if let Err(cause) = self.analyze_having(&mut data).await {
            return Err(cause.add_message_back(" (while in analyze select having)."));
        }

        if let Err(cause) = self.analyze_order_by(&mut data).await {
            return Err(cause.add_message_back(" (while in analyze select order by)."));
        }

        if let Err(cause) = self.analyze_limit(&mut data).await {
            return Err(cause.add_message_back(" (while in analyze select limit)."));
        }


        // TODO: check and finalize
        Ok(AnalyzedResult::SelectQuery(data))
    }
}

impl DfQueryStatement {
    pub async fn analyze_filter(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        if let Some(predicate) = &self.selection {
            // TODO: collect pushdown predicates
            let analyze_schema = data.joined_schema.clone();
            let expr_analyzer = data.create_analyzer(analyze_schema, false);
            data.filter_predicate = Some(expr_analyzer.analyze(predicate).await?);
        }

        Ok(())
    }

    /// Expand wildcard and create columns alias map(alias -> expression) for named projection item
    pub async fn analyze_projection(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        let projection_expressions = self.projection_exprs(data).await?;
        data.before_aggr_schema = data.joined_schema.clone();
        data.projection_aliases = extract_aliases(&projection_expressions);

        let from_schema = data.joined_schema.to_data_schema();
        for projection_expression in &projection_expressions {
            if !data.add_aggregate_function(projection_expression)? {
                match projection_expression {
                    Expression::Alias(alias, expr) => {
                        if !data.before_group_by_expressions.contains(&expr) {
                            data.before_group_by_expressions.push(expr.as_ref().clone());
                        }

                        let field = expr.to_data_field(&from_schema)?;

                        let nullable = field.is_nullable();
                        let data_type = field.data_type().clone();
                        let column_desc = AnalyzeQueryColumnDesc::create(alias, data_type, nullable);
                        data.before_aggr_schema.add_projection(column_desc, true)?;
                    }
                    _ => {
                        if !data.before_group_by_expressions.contains(&projection_expression) {
                            data.before_group_by_expressions.push(projection_expression.clone());
                        }
                    }
                }
            }
        }

        data.projection_expressions = projection_expressions;
        Ok(())
    }

    /// Push group by exprs and aggregate function inputs into before_group_by_expressions
    pub async fn analyze_group_by(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        let analyze_schema = data.before_aggr_schema.clone();
        let expr_analyzer = data.create_analyzer(analyze_schema, false);
        let projection_aliases = &data.projection_aliases;

        for group_by_expr in &self.group_by {
            let analyzed_expr = expr_analyzer.analyze(group_by_expr).await?;
            let analyzed_expr = resolve_aliases_to_exprs(&analyzed_expr, projection_aliases)?;

            if !data.group_by_expressions.contains(&analyzed_expr) {
                // The expr completed in before_group_by_expressions.
                data.group_by_expressions.push(analyzed_expr.clone());
            }

            if !data.before_group_by_expressions.contains(&analyzed_expr) {
                data.before_group_by_expressions.push(analyzed_expr.clone());
            }
        }

        Ok(())
    }

    pub async fn analyze_having(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        if let Some(predicate) = &self.having {
            let schema = data.before_aggr_schema.clone();
            let analyzer = ExpressionAnalyzer::with_source(data.ctx.clone(), schema, true);
            let expression = analyzer.analyze(predicate).await?;
            let projection_aliases = &data.projection_aliases;
            let expr = resolve_aliases_to_exprs(&expression, projection_aliases)?;
            let expr = Self::after_group_by_expr(&expr, data)?;

            data.add_aggregate_function(&expr)?;
            data.having_predicate = Some(Self::after_group_by_expr(&expr, data)?);
        }
        Ok(())
    }

    pub async fn analyze_order_by(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        for order_by_expr in &self.order_by {
            let expr = Self::analyze_expr_with_alias(&order_by_expr.expr, data).await?;

            data.add_aggregate_function(&expr)?;
            let after_group_by_expr = Self::after_group_by_expr(&expr, data)?;
            let order_by_column_expr = expr_as_column_expr(&after_group_by_expr)?;

            data.before_having_expressions.push(after_group_by_expr);
            data.order_by_expressions.push(Expression::Sort {
                expr: Box::new(order_by_column_expr),
                asc: order_by_expr.asc.unwrap_or(true),
                nulls_first: order_by_expr.asc.unwrap_or(true),
            });
        }

        Ok(())
    }

    pub async fn analyze_limit(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        // TODO: analyze limit
        Ok(())
    }

    fn after_group_by_expr(expr: &Expression, data: &mut AnalyzeQueryState) -> Result<Expression> {
        let aggr_exprs = &data.aggregate_expressions;
        let rebased_expr = rebase_expr(&expr, aggr_exprs)?;
        rebase_expr(&rebased_expr, &data.group_by_expressions)
    }

    async fn analyze_expr_with_alias(expr: &Expr, data: &mut AnalyzeQueryState) -> Result<Expression> {
        let analyze_schema = data.after_aggr_schema.clone();
        let expr_analyzer = data.create_analyzer(analyze_schema, true);
        let analyzed_expr = expr_analyzer.analyze(expr).await?;
        let projection_aliases = &data.projection_aliases;
        resolve_aliases_to_exprs(&analyzed_expr, projection_aliases)
    }

    async fn projection_exprs(&self, data: &mut AnalyzeQueryState) -> Result<Vec<Expression>> {
        // XXX: We do not allow projection to be used in projection, such as:
        // SELECT column_a + 1 AS alias_b, alias_b + 1 AS alias_c FROM table_name_1;
        // This is also not supported in MySQL, but maybe we should to support it?

        let source = data.joined_schema.clone();
        let query_context = data.ctx.clone();
        let expr_analyzer = ExpressionAnalyzer::with_source(query_context, source, true);

        let mut output_columns = Vec::with_capacity(self.projection.len());
        for item in &self.projection {
            match item {
                SelectItem::Wildcard => {
                    output_columns.extend(self.extend_wildcard_exprs(&source)?);
                }
                SelectItem::UnnamedExpr(expr) => {
                    output_columns.push(expr_analyzer.analyze(expr).await?);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr_alias = alias.value.clone();
                    let expr = Box::new(expr_analyzer.analyze(expr).await?);
                    output_columns.push(Expression::Alias(expr_alias, expr));
                }
                _ => { return Err(ErrorCode::SyntaxException(format!("SelectItem: {:?} are not supported", item))); }
            };
        }

        Ok(output_columns)
    }

    fn extend_wildcard_exprs(&self, schema: &AnalyzeQuerySchema) -> Result<Vec<Expression>> {
        let data_schema = schema.to_data_schema();

        let columns_field = data_schema.fields();
        let mut expressions = Vec::with_capacity(columns_field.len());
        for column_field in columns_field {
            expressions.push(Expression::Column(column_field.name().clone()));
        }

        Ok(expressions)
    }
}


impl AnalyzeQueryState {
    pub fn create_analyzer(&self, schema: AnalyzeQuerySchema, aggr: bool) -> ExpressionAnalyzer {
        let query_context = self.ctx.clone();
        ExpressionAnalyzer::with_source(query_context, schema, aggr)
    }

    pub fn add_aggregate_function(&mut self, expr: &Expression) -> Result<bool> {
        let aggregate_exprs = find_aggregate_exprs_in_expr(&expr);
        let aggregate_exprs_require_expression = expand_aggregate_arg_exprs(&aggregate_exprs);

        if !aggregate_exprs.is_empty() {
            self.add_before_having_exprs(expr, &aggregate_exprs)?;
        }

        for require_expression in &aggregate_exprs_require_expression {
            if !self.before_group_by_expressions.contains(require_expression) {
                self.before_group_by_expressions.push(require_expression.clone())
            }
        }

        for aggregate_expr in &aggregate_exprs {
            if !self.aggregate_expressions.contains(aggregate_expr) {
                self.aggregate_expressions.push(aggregate_expr.clone());
            }
        }

        Ok(!aggregate_exprs.is_empty())
    }

    fn add_before_having_exprs(&mut self, expr: &Expression, aggr: &[Expression]) -> Result<()> {
        match expr {
            Expression::Alias(_, inner) => {
                if !self.before_having_expressions.contains(&inner) {
                    let rebased_expr = rebase_expr(&inner, aggr)?;
                    self.before_having_expressions.push(rebased_expr);
                }
            }
            expr => {
                if !self.before_having_expressions.contains(&expr) {
                    let rebased_expr = rebase_expr(&expr, aggr)?;
                    self.before_having_expressions.push(rebased_expr);
                }
            }
        };

        Ok(())
    }
}

