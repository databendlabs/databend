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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::EmptyPlan;
use common_planners::ExplainPlan;
use common_planners::Expression;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_tracing::tracing;

use super::statements::ExpressionSyncAnalyzer;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::QueryAnalyzeState;
use crate::sql::statements::QueryRelation;
use crate::sql::DfHint;
use crate::sql::DfParser;
use crate::sql::DfStatement;

pub struct PlanParser;

impl PlanParser {
    pub async fn parse(ctx: Arc<QueryContext>, query: &str) -> Result<PlanNode> {
        let (statements, _) = DfParser::parse_sql(query, ctx.get_current_session().get_type())?;
        PlanParser::build_plan(statements, ctx).await
    }

    pub async fn parse_with_format(
        ctx: Arc<QueryContext>,
        query: &str,
    ) -> Result<(PlanNode, Option<String>)> {
        let (statements, _) = DfParser::parse_sql(query, ctx.get_current_session().get_type())?;
        let mut format = None;
        if !statements.is_empty() {
            match &statements[0] {
                DfStatement::Query(q) => {
                    format = q.format.clone();
                }
                DfStatement::InsertQuery(q) => {
                    format = q.format.clone();
                }
                _ => {}
            }
        };
        let plan = PlanParser::build_plan(statements, ctx).await?;
        Ok((plan, format))
    }

    pub fn parse_expr(expr: &str) -> Result<Expression> {
        let expr = DfParser::parse_expr(expr)?;
        let analyzer = ExpressionSyncAnalyzer::create();
        analyzer.analyze(&expr)
    }

    pub fn parse_exprs(expr: &str) -> Result<Vec<Expression>> {
        let exprs = DfParser::parse_exprs(expr)?;
        let analyzer = ExpressionSyncAnalyzer::create();

        let results = exprs
            .iter()
            .map(|expr| analyzer.analyze(expr))
            .collect::<Result<Vec<_>>>();
        results
    }

    pub async fn parse_with_hint(
        query: &str,
        ctx: Arc<QueryContext>,
    ) -> (Result<PlanNode>, Vec<DfHint>) {
        match DfParser::parse_sql(query, ctx.get_current_session().get_type()) {
            Err(cause) => (Err(cause), vec![]),
            Ok((statements, hints)) => (PlanParser::build_plan(statements, ctx).await, hints),
        }
    }

    pub async fn build_plan(
        statements: Vec<DfStatement<'_>>,
        ctx: Arc<QueryContext>,
    ) -> Result<PlanNode> {
        if statements.is_empty() {
            return Ok(PlanNode::Empty(EmptyPlan::create()));
        } else if statements.len() > 1 {
            return Err(ErrorCode::SyntaxException("Only support single query"));
        }

        match statements[0].analyze(ctx.clone()).await? {
            AnalyzedResult::SimpleQuery(plan) => Ok(*plan),
            AnalyzedResult::SelectQuery(data) => Self::build_query_plan(&data),
            AnalyzedResult::ExplainQuery((typ, data)) => {
                let res = Self::build_query_plan(&data)?;
                Ok(PlanNode::Explain(ExplainPlan {
                    typ,
                    input: Arc::new(res),
                }))
            }
        }
    }

    pub fn build_query_plan(data: &QueryAnalyzeState) -> Result<PlanNode> {
        let from = Self::build_from_plan(data)?;
        tracing::debug!("Build from plan:\n{:?}", from);

        let filter = Self::build_filter_plan(from, data)?;
        tracing::debug!("Build filter plan:\n{:?}", filter);

        let group_by = Self::build_group_by_plan(filter, data)?;
        tracing::debug!("Build group_by plan:\n{:?}", group_by);

        let before_order = Self::build_before_order(group_by, data)?;
        tracing::debug!("Build before_order plan:\n{:?}", before_order);

        let having = Self::build_having_plan(before_order, data)?;
        tracing::debug!("Build having plan:\n{:?}", having);

        let window = Self::build_window_plan(having, data)?;
        tracing::debug!("Build window plan node:\n{:?}", window);

        let distinct = Self::build_distinct_plan(window, data)?;
        tracing::debug!("Build distinct plan:\n{:?}", distinct);

        let order_by = Self::build_order_by_plan(distinct, data)?;
        tracing::debug!("Build order_by plan:\n{:?}", order_by);

        let projection = Self::build_projection_plan(order_by, data)?;
        tracing::debug!("Build projection plan:\n{:?}", projection);

        let limit = Self::build_limit_plan(projection, data)?;
        tracing::debug!("Build limit plan:\n{:?}", limit);

        Ok(PlanNode::Select(SelectPlan {
            input: Arc::new(limit),
        }))
    }

    fn build_from_plan(data: &QueryAnalyzeState) -> Result<PlanNode> {
        match &data.relation {
            QueryRelation::None => Err(ErrorCode::LogicalError("Not from in select query")),
            QueryRelation::Nested(data) => Self::build_query_plan(data),
            QueryRelation::FromTable(plan) => Ok(PlanNode::ReadSource(plan.as_ref().clone())),
        }
    }

    /// Apply a filter to the plan
    fn build_filter_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match &data.filter {
            None => Ok(plan),
            Some(predicate) => {
                let predicate = predicate.clone();
                let builder = PlanBuilder::from(&plan).filter(predicate)?;
                builder.build()
            }
        }
    }

    fn build_group_by_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        // S0: Apply a partial aggregator plan.
        // S1: Apply a fragment plan for distributed planners split.
        // S2: Apply a final aggregator plan.
        match data.aggregate_expressions.is_empty() && data.group_by_expressions.is_empty() {
            true => Ok(plan),
            false => {
                let input_plan = Self::build_before_group_by(plan, data)?;

                let schema = input_plan.schema();
                let group_by_exprs = &data.group_by_expressions;
                let aggregate_exprs = &data.aggregate_expressions;
                PlanBuilder::from(&input_plan)
                    .aggregate_partial(aggregate_exprs, group_by_exprs)?
                    .aggregate_final(schema, aggregate_exprs, group_by_exprs)?
                    .build()
            }
        }
    }

    fn build_before_group_by(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        fn is_all_column(exprs: &[Expression]) -> bool {
            exprs
                .iter()
                .all(|expr| matches!(expr, Expression::Column(_)))
        }

        match data.before_group_by_expressions.is_empty() {
            true => Ok(plan),
            // if all expression is column expression expression, we skip this expression
            false if is_all_column(&data.before_group_by_expressions) => Ok(plan),
            false => PlanBuilder::from(&plan)
                .expression(&data.before_group_by_expressions, "Before GroupBy")?
                .build(),
        }
    }

    fn build_having_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match &data.having {
            None => Ok(plan),
            Some(predicate) => PlanBuilder::from(&plan).having(predicate.clone())?.build(),
        }
    }

    fn build_distinct_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match data.distinct_expressions.is_empty() {
            false => {
                let group_by_exprs = &data.distinct_expressions;
                let aggr_exprs = vec![];
                PlanBuilder::from(&plan)
                    .aggregate_partial(&aggr_exprs, group_by_exprs)?
                    .aggregate_final(plan.schema(), &aggr_exprs, group_by_exprs)?
                    .build()
            }
            true => Ok(plan),
        }
    }

    fn build_order_by_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match data.order_by_expressions.is_empty() {
            true => Ok(plan),
            false => PlanBuilder::from(&plan)
                .sort(&data.order_by_expressions)?
                .build(),
        }
    }

    fn build_before_order(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        fn is_all_column(exprs: &[Expression]) -> bool {
            exprs
                .iter()
                .all(|expr| matches!(expr, Expression::Column(_)))
        }

        match data.expressions.is_empty() {
            true => Ok(plan),
            // if all expression is column expression expression, we skip this expression
            false if is_all_column(&data.expressions) => Ok(plan),
            false => match data.order_by_expressions.is_empty() {
                true => PlanBuilder::from(&plan)
                    .expression(&data.expressions, "Before Projection")?
                    .build(),
                false => PlanBuilder::from(&plan)
                    .expression(&data.expressions, "Before OrderBy")?
                    .build(),
            },
        }
    }

    fn build_window_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match data.window_expressions.is_empty() {
            true => Ok(plan),
            false => {
                let exprs = data.window_expressions.to_vec();
                exprs.into_iter().try_fold(plan, |input, window_func| {
                    PlanBuilder::from(&input).window_func(window_func)?.build()
                })
            }
        }
    }

    fn build_projection_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        PlanBuilder::from(&plan)
            .project(&data.projection_expressions)?
            .build()
    }

    fn build_limit_plan(input: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match (&data.limit, &data.offset) {
            (None, None) => Ok(input),
            (limit, offset) => PlanBuilder::from(&input)
                .limit_offset(*limit, offset.unwrap_or(0))?
                .build(),
        }
    }
}
