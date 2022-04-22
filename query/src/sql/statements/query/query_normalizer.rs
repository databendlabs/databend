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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::extract_aliases;
use common_planners::find_aggregate_exprs_in_expr;
use common_planners::resolve_aliases_to_exprs;
use common_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::ast::OffsetRows;
use sqlparser::ast::SelectItem;

use crate::sessions::QueryContext;
use crate::sql::statements::analyzer_expr::ExpressionAnalyzer;
use crate::sql::statements::query::QueryASTIR;
use crate::sql::statements::DfQueryStatement;

pub struct QueryNormalizer {
    query_ast_ir: QueryASTIR,
    expression_analyzer: ExpressionAnalyzer,
    aliases_map: HashMap<String, Expression>,
}

/// Replace alias in query and collect aggregate functions
impl QueryNormalizer {
    fn create(ctx: Arc<QueryContext>) -> QueryNormalizer {
        QueryNormalizer {
            expression_analyzer: ExpressionAnalyzer::create(ctx),
            aliases_map: HashMap::new(),
            query_ast_ir: QueryASTIR {
                filter_predicate: None,
                group_by_expressions: vec![],
                having_predicate: None,
                aggregate_expressions: vec![],
                order_by_expressions: vec![],
                projection_expressions: vec![],
                limit: None,
                offset: None,
            },
        }
    }

    pub async fn normalize(ctx: Arc<QueryContext>, v: &DfQueryStatement) -> Result<QueryASTIR> {
        let query_normalizer = QueryNormalizer::create(ctx);
        query_normalizer.transform(v).await
    }

    pub async fn transform(mut self, query: &DfQueryStatement) -> Result<QueryASTIR> {
        if let Err(cause) = self.visit_filter(query).await {
            return Err(cause.add_message_back(" (while in analyze select filter)"));
        }

        if let Err(cause) = self.analyze_projection(query).await {
            return Err(cause.add_message_back(" (while in analyze select projection)"));
        }

        if let Err(cause) = self.analyze_group_by(query).await {
            return Err(cause.add_message_back(" (while in analyze select group by)"));
        }

        if let Err(cause) = self.analyze_having(query).await {
            return Err(cause.add_message_back(" (while in analyze select having)"));
        }

        if let Err(cause) = self.analyze_order_by(query).await {
            return Err(cause.add_message_back(" (while in analyze select order by)"));
        }

        if let Err(cause) = self.analyze_limit(query).await {
            return Err(cause.add_message_back(" (while in analyze select limit)"));
        }

        Ok(self.query_ast_ir)
    }

    async fn visit_filter(&mut self, query: &DfQueryStatement) -> Result<()> {
        if let Some(predicate) = &query.selection {
            let analyzer = &self.expression_analyzer;
            self.query_ast_ir.filter_predicate = Some(analyzer.analyze(predicate).await?);
        }

        Ok(())
    }

    async fn analyze_projection(&mut self, query: &DfQueryStatement) -> Result<()> {
        let projection_expressions = self.projection_exprs(query).await?;
        self.aliases_map = extract_aliases(&projection_expressions);

        for projection_expression in &projection_expressions {
            self.add_aggregate_function(projection_expression)?;
        }

        self.query_ast_ir.projection_expressions = projection_expressions;
        Ok(())
    }

    async fn analyze_group_by(&mut self, query: &DfQueryStatement) -> Result<()> {
        for group_by_expr in &query.group_by {
            let expression = self.resolve_aliases(group_by_expr).await?;
            self.query_ast_ir.group_by_expressions.push(expression);
        }

        Ok(())
    }

    async fn analyze_having(&mut self, query: &DfQueryStatement) -> Result<()> {
        if let Some(predicate) = &query.having {
            let expression = self.resolve_aliases(predicate).await?;

            self.add_aggregate_function(&expression)?;
            self.query_ast_ir.having_predicate = Some(expression);
        }
        Ok(())
    }

    async fn analyze_order_by(&mut self, query: &DfQueryStatement) -> Result<()> {
        for order_by_expr in &query.order_by {
            let expression = self.resolve_aliases(&order_by_expr.expr).await?;

            self.add_aggregate_function(&expression)?;
            self.query_ast_ir
                .order_by_expressions
                .push(Expression::Sort {
                    expr: Box::new(expression.clone()),
                    asc: order_by_expr.asc.unwrap_or(true),
                    nulls_first: order_by_expr.asc.unwrap_or(true),
                    origin_expr: Box::new(expression),
                });
        }

        Ok(())
    }

    async fn analyze_limit(&mut self, query: &DfQueryStatement) -> Result<()> {
        if let Some(limit) = &query.limit {
            let expression_analyzer = &self.expression_analyzer;
            let limit_literal = match expression_analyzer.analyze(limit).await? {
                Expression::Literal { value, .. } => Ok(value.as_u64()? as usize),
                _ => Err(ErrorCode::SyntaxException(format!(
                    "Unexpected expression for LIMIT clause: {:?}",
                    limit
                ))),
            }?;
            self.query_ast_ir.limit = Some(limit_literal);
        }

        if let Some(offset) = &query.offset {
            if !matches!(offset.rows, OffsetRows::None) {
                return Err(ErrorCode::SyntaxException("Unimplemented limit n ROW|ROWS"));
            }

            let expression_analyzer = &self.expression_analyzer;
            let offset_literal = match expression_analyzer.analyze(&offset.value).await? {
                Expression::Literal { value, .. } => Ok(value.as_u64()? as usize),
                _ => Err(ErrorCode::SyntaxException(format!(
                    "Unexpected expression for LIMIT clause: {:?}",
                    offset
                ))),
            }?;
            self.query_ast_ir.offset = Some(offset_literal);
        }

        Ok(())
    }

    async fn projection_exprs(&self, query: &DfQueryStatement) -> Result<Vec<Expression>> {
        let mut output_columns = Vec::with_capacity(query.projection.len());

        let expr_analyzer = &self.expression_analyzer;
        for item in &query.projection {
            match item {
                SelectItem::Wildcard => {
                    if query.from.is_empty() {
                        return Err(ErrorCode::LogicalError(
                            "SELECT * with no tables specified is not valid",
                        ));
                    }
                    output_columns.push(Expression::Wildcard);
                }
                SelectItem::UnnamedExpr(expr) => {
                    output_columns.push(expr_analyzer.analyze(expr).await?);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr_alias = alias.value.clone();
                    let expr = Box::new(expr_analyzer.analyze(expr).await?);
                    output_columns.push(Expression::Alias(expr_alias, expr));
                }
                _ => {
                    return Err(ErrorCode::SyntaxException(format!(
                        "SelectItem: {:?} are not supported",
                        item
                    )));
                }
            };
        }

        Ok(output_columns)
    }

    async fn resolve_aliases(&self, expr: &Expr) -> Result<Expression> {
        let aliases_map = &self.aliases_map;
        let expression_analyzer = &self.expression_analyzer;
        resolve_aliases_to_exprs(&expression_analyzer.analyze(expr).await?, aliases_map)
    }

    fn add_aggregate_function(&mut self, expr: &Expression) -> Result<()> {
        for aggregate_expr in find_aggregate_exprs_in_expr(expr) {
            if !self
                .query_ast_ir
                .aggregate_expressions
                .contains(&aggregate_expr)
            {
                self.query_ast_ir.aggregate_expressions.push(aggregate_expr);
            }
        }

        Ok(())
    }
}
