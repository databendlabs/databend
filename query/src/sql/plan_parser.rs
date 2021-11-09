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

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_infallible::Mutex;
use common_meta_types::TableMeta;
use common_planners::expand_aggregate_arg_exprs;
use common_planners::expand_wildcard;
use common_planners::expr_as_column_expr;
use common_planners::extract_aliases;
use common_planners::find_aggregate_exprs;
use common_planners::find_columns_not_satisfy_exprs;
use common_planners::rebase_expr;
use common_planners::rebase_expr_from_input;
use common_planners::resolve_aliases_to_exprs;
use common_planners::sort_to_inner_expr;
use common_planners::unwrap_alias_exprs;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DescribeTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ExplainPlan;
use common_planners::Expression;
use common_planners::InsertIntoPlan;
use common_planners::KillPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SettingPlan;
use common_planners::ShowCreateTablePlan;
use common_planners::TableScanInfo;
use common_planners::TruncateTablePlan;
use common_planners::UseDatabasePlan;
use common_planners::VarValue;
use common_streams::Source;
use common_streams::ValueSource;
use common_tracing::tracing;
use nom::FindSubstring;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use sqlparser::ast::UnaryOperator;

use crate::catalogs::ToReadDataSourcePlan;
use crate::functions::ContextFunction;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::{DfCreateTable, AnalyzeData, QueryRelation};
use crate::sql::statements::DfDropDatabase;
use crate::sql::statements::DfUseDatabase;
use crate::sql::statements::DfCreateDatabase;
use crate::sql::statements::DfDescribeTable;
use crate::sql::statements::DfDropTable;
use crate::sql::statements::DfExplain;
use crate::sql::DfHint;
use crate::sql::statements::DfKillStatement;
use crate::sql::DfParser;
use crate::sql::statements::DfShowCreateTable;
use crate::sql::statements::DfShowDatabases;
use crate::sql::statements::DfShowTables;
use crate::sql::DfStatement;
use crate::sql::statements::DfTruncateTable;
use crate::sql::SQLCommon;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};

pub struct PlanParser {
    ctx: DatabendQueryContextRef,
}

impl PlanParser {
    pub fn create(ctx: DatabendQueryContextRef) -> Self {
        Self { ctx }
    }

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

    pub fn build_query_plan(data: &AnalyzeData) -> Result<PlanNode> {
        let from = Self::build_from_plan(data)?;
        let filter = Self::build_filter_plan(from, data)?;
        let group_by = Self::build_group_by_plan(filter, data)?;
        let having = Self::build_having_plan(group_by, data)?;
        let order_by = Self::build_order_by_plan(having, data)?;
        let projection = Self::build_projection_plan(order_by, data)?;

        Self::build_limit_plan(projection, data)
    }

    fn build_explain_plan(data: &AnalyzeData, ctx: &DatabendQueryContextRef) -> Result<PlanNode> {
        let query_plan = Self::build_query_plan(data)?;
        // Ok(PlanNode::Explain(ExplainPlan {}))
        unimplemented!("")
    }

    fn build_from_plan(data: &AnalyzeData) -> Result<PlanNode> {
        match data.relation.map(AsRef::as_ref) {
            None => Err(ErrorCode::LogicalError("Not from in select query")),
            Some(QueryRelation::Nested(data)) => Self::build_query_plan(data),
            Some(QueryRelation::FromTable(plan)) => Ok(PlanNode::ReadSource(plan.clone()))
        }
    }

    /// Apply a filter to the plan
    fn build_filter_plan(plan: PlanNode, data: &AnalyzeData) -> Result<PlanNode> {
        match &data.filter_predicate {
            None => Ok(plan),
            Some(predicate) => {
                let predicate = predicate.clone();
                let builder = PlanBuilder::from(&plan).filter(predicate)?;
                builder.build()
            }
        }
    }

    fn build_group_by_plan(plan: PlanNode, data: &AnalyzeData) -> Result<PlanNode> {
        // S0: Apply a partial aggregator plan.
        // S1: Apply a fragment plan for distributed planners split.
        // S2: Apply a final aggregator plan.
        let mut builder = PlanBuilder::from(&plan);

        if !data.aggregate_expressions.is_empty() || !data.group_by_expressions.is_empty() {
            let schema = plan.schema();
            let aggr_expr = &data.aggregate_expressions;
            let group_by_expr = &data.group_by_expressions;
            let before_group_by_expr = &data.before_group_by_expressions;
            builder = builder.expression(before_group_by_expr, "Before group by")?;
            builder = builder.aggregate_partial(aggr_expr, group_by_expr)?;
            builder = builder.aggregate_final(schema, aggr_expr, group_by_expr)?;
        }

        builder.build()
    }

    fn build_having_plan(plan: PlanNode, data: &AnalyzeData) -> Result<PlanNode> {
        let mut builder = PlanBuilder::from(&plan);

        if !data.before_having_expressions.is_empty() {
            let before_having_exprs = &data.before_having_expressions;
            match data.order_by_expressions.is_empty() {
                true => {
                    builder = builder.expression(before_having_exprs, "Before order")?;
                }
                false => {
                    builder = builder.expression(before_having_exprs, "Before projection")?;
                }
            };
        }

        if let Some(having_predicate) = &data.having_predicate {
            builder = builder.having(having_predicate.clone())?;
        }

        builder.build()
    }

    fn build_order_by_plan(plan: PlanNode, data: &AnalyzeData) -> Result<PlanNode> {
        match data.order_by_expressions.is_empty() {
            true => Ok(plan),
            false => PlanBuilder::from(&plan)
                .sort(&data.order_by_expressions)?
                .build(),
        }
    }

    fn build_projection_plan(plan: PlanNode, data: &AnalyzeData) -> Result<PlanNode> {
        PlanBuilder::from(&plan)
            .project(&data.projection_expressions)?
            .build()
    }

    fn build_limit_plan(input: PlanNode, data: &AnalyzeData) -> Result<PlanNode> {
        unimplemented!("")
    }
}
