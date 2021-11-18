// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanBuilder;
use common_planners::PlanNode;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::{QueryAnalyzeState, QueryNormalizerData, QueryRelation};
use crate::sql::DfHint;
use crate::sql::DfParser;
use crate::sql::DfStatement;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};

pub struct PlanParser;

impl PlanParser {
    pub async fn parse(query: &str, ctx: DatabendQueryContextRef) -> Result<PlanNode> {
        let (statements, _) = DfParser::parse_sql(query)?;
        PlanParser::build_plan(statements, ctx).await
    }

    pub async fn parse_with_hint(query: &str, ctx: DatabendQueryContextRef) -> (Result<PlanNode>, Vec<DfHint>) {
        match DfParser::parse_sql(query) {
            Err(cause) => (Err(cause), vec![]),
            Ok((statements, hints)) => (PlanParser::build_plan(statements, ctx).await, hints)
        }
    }

    async fn build_plan(statements: Vec<DfStatement>, ctx: DatabendQueryContextRef) -> Result<PlanNode> {
        if statements.len() != 1 {
            return Err(ErrorCode::SyntaxException("Only support single query"));
        }

        match statements[0].analyze(ctx.clone()).await? {
            AnalyzedResult::SimpleQuery(plan) => Ok(plan),
            AnalyzedResult::SelectQuery(data) => Self::build_query_plan(&data),
            AnalyzedResult::ExplainQuery(data) => Self::build_explain_plan(&data, &ctx),
        }
    }

    pub fn build_query_plan(data: &QueryAnalyzeState) -> Result<PlanNode> {
        let from = Self::build_from_plan(data)?;
        let filter = Self::build_filter_plan(from, data)?;
        let group_by = Self::build_group_by_plan(filter, data)?;
        let having = Self::build_having_plan(group_by, data)?;
        let order_by = Self::build_order_by_plan(having, data)?;
        let projection = Self::build_projection_plan(order_by, data)?;

        Self::build_limit_plan(projection, data)
    }

    fn build_explain_plan(data: &QueryNormalizerData, ctx: &DatabendQueryContextRef) -> Result<PlanNode> {
        // let query_plan = Self::build_query_plan(data)?;
        // Ok(PlanNode::Explain(ExplainPlan {}))
        unimplemented!("")
    }

    fn build_from_plan(data: &QueryAnalyzeState) -> Result<PlanNode> {
        unimplemented!()
        // match &data.relation {
        //     None => Err(ErrorCode::LogicalError("Not from in select query")),
        //     Some(relation) => match relation.as_ref() {
        //         QueryRelation::Nested(data) => Self::build_query_plan(&data),
        //         QueryRelation::FromTable(plan) => Ok(PlanNode::ReadSource(plan.clone())),
        //     }
        // }
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

        // let mut builder = PlanBuilder::from(&plan);
        //
        // if !data.aggregate_expressions.is_empty() || !data.group_by_expressions.is_empty() {
        //     let schema = plan.schema();
        //     let aggr_expr = &data.aggregate_expressions;
        //     let group_by_expr = &data.group_by_expressions;
        //     let before_group_by_expr = &data.before_group_by_expressions;
        //     builder = builder.expression(before_group_by_expr, "Before group by")?;
        //     builder = builder.aggregate_partial(aggr_expr, group_by_expr)?;
        //     builder = builder.aggregate_final(schema, aggr_expr, group_by_expr)?;
        // }
        //
        // builder.build()
        unimplemented!()
    }

    fn build_having_plan(plan: PlanNode, data: &QueryAnalyzeState) -> Result<PlanNode> {
        let mut builder = PlanBuilder::from(&plan);

        if !data.expressions.is_empty() {
            let before_expressions = &data.expressions;
            match data.order_by_expressions.is_empty() {
                true => {
                    builder = builder.expression(before_expressions, "Before order")?;
                }
                false => {
                    builder = builder.expression(before_expressions, "Before projection")?;
                }
            };
        }

        if let Some(having_predicate) = &data.having {
            builder = builder.having(having_predicate.clone())?;
        }

        builder.build()
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
        unimplemented!("")
    }
}
