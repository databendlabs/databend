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
use common_planners::ExplainPlan;
use common_planners::Expression;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;

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
        let (statements, _) = DfParser::parse_sql(query)?;
        PlanParser::build_plan(statements, ctx).await
    }

    pub async fn parse_with_hint(
        query: &str,
        ctx: Arc<QueryContext>,
    ) -> (Result<PlanNode>, Vec<DfHint>) {
        match DfParser::parse_sql(query) {
            Err(cause) => (Err(cause), vec![]),
            Ok((statements, hints)) => (PlanParser::build_plan(statements, ctx).await, hints),
        }
    }

    pub async fn build_plan(
        statements: Vec<DfStatement>,
        ctx: Arc<QueryContext>,
    ) -> Result<PlanNode> {
        if statements.len() != 1 {
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
        let filter = Self::build_filter_plan(from, data)?;
        let group_by = Self::build_group_by_plan(filter, data)?;
        let before_order = Self::build_before_order(group_by, data)?;
        let having = Self::build_having_plan(before_order, data)?;
        let order_by = Self::build_order_by_plan(having, data)?;
        let projection = Self::build_projection_plan(order_by, data)?;
        let limit = Self::build_limit_plan(projection, data)?;

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

    fn build_order_by_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        match data.order_by_expressions.is_empty() {
            true => Ok(plan),
            false => PlanBuilder::from(&plan)
                .sort(&data.order_by_expressions)?
                .build(),
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
