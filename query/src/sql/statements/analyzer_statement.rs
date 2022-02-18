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

use std::fmt::Debug;
use std::sync::Arc;

use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::fmt::Formatter;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_planners::ExplainType;
use common_planners::Expression;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;

use crate::sessions::QueryContext;
use crate::sql::DfStatement;

#[allow(clippy::enum_variant_names)]
pub enum AnalyzedResult {
    SimpleQuery(Box<PlanNode>),
    SelectQuery(Box<QueryAnalyzeState>),
    ExplainQuery((ExplainType, Box<QueryAnalyzeState>)),
}

#[derive(Clone)]
pub enum QueryRelation {
    None,
    FromTable(Box<ReadDataSourcePlan>),
    Nested(Box<QueryAnalyzeState>),
}

#[derive(Clone)]
pub struct QueryAnalyzeState {
    pub filter: Option<Expression>,
    pub having: Option<Expression>,
    pub order_by_expressions: Vec<Expression>,
    // before order or before projection expression plan
    pub expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,

    pub group_by_expressions: Vec<Expression>,
    pub aggregate_expressions: Vec<Expression>,
    pub before_group_by_expressions: Vec<Expression>,

    pub limit: Option<usize>,
    pub offset: Option<usize>,

    pub relation: QueryRelation,
    pub finalize_schema: DataSchemaRef,
}

impl QueryAnalyzeState {
    pub fn add_expression(&mut self, expr: &Expression) {
        if !self.expressions.contains(expr) {
            self.expressions.push(expr.clone());
        }
    }

    pub fn add_before_group_expression(&mut self, expr: &Expression) {
        if !self.before_group_by_expressions.contains(expr) {
            self.before_group_by_expressions.push(expr.clone());
        }
    }
}

impl Default for QueryAnalyzeState {
    fn default() -> Self {
        QueryAnalyzeState {
            filter: None,
            having: None,
            order_by_expressions: vec![],
            expressions: vec![],
            projection_expressions: vec![],
            group_by_expressions: vec![],
            aggregate_expressions: vec![],
            before_group_by_expressions: vec![],
            limit: None,
            offset: None,
            relation: QueryRelation::None,
            finalize_schema: Arc::new(DataSchema::empty()),
        }
    }
}

impl Debug for QueryAnalyzeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let mut debug_struct = f.debug_struct("QueryAnalyzeState");

        if let Some(predicate) = &self.filter {
            debug_struct.field("filter", predicate);
        }

        if !self.before_group_by_expressions.is_empty() {
            debug_struct.field("before_group_by", &self.before_group_by_expressions);
        }

        if !self.group_by_expressions.is_empty() {
            debug_struct.field("aggregator", &self.group_by_expressions);
        }

        if !self.aggregate_expressions.is_empty() {
            debug_struct.field("aggregate", &self.aggregate_expressions);
        }

        if !self.expressions.is_empty() {
            match self.order_by_expressions.is_empty() {
                true => debug_struct.field("before_projection", &self.expressions),
                false => debug_struct.field("before_order_by", &self.expressions),
            };
        }

        if let Some(predicate) = &self.having {
            debug_struct.field("having", predicate);
        }

        if !self.order_by_expressions.is_empty() {
            debug_struct.field("order_by", &self.order_by_expressions);
        }

        if !self.projection_expressions.is_empty() {
            debug_struct.field("projection", &self.projection_expressions);
        }

        debug_struct.finish()
    }
}

#[async_trait::async_trait]
pub trait AnalyzableStatement {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult>;
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfStatement {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        match self {
            DfStatement::Query(v) => v.analyze(ctx).await,
            DfStatement::Explain(v) => v.analyze(ctx).await,
            DfStatement::ShowDatabases(v) => v.analyze(ctx).await,
            DfStatement::ShowCreateDatabase(v) => v.analyze(ctx).await,
            DfStatement::CreateDatabase(v) => v.analyze(ctx).await,
            DfStatement::DropDatabase(v) => v.analyze(ctx).await,
            DfStatement::CreateTable(v) => v.analyze(ctx).await,
            DfStatement::DescribeTable(v) => v.analyze(ctx).await,
            DfStatement::DescribeStage(v) => v.analyze(ctx).await,
            DfStatement::DropTable(v) => v.analyze(ctx).await,
            DfStatement::TruncateTable(v) => v.analyze(ctx).await,
            DfStatement::OptimizeTable(v) => v.analyze(ctx).await,
            DfStatement::UseDatabase(v) => v.analyze(ctx).await,
            DfStatement::UseTenant(v) => v.analyze(ctx).await,
            DfStatement::ShowCreateTable(v) => v.analyze(ctx).await,
            DfStatement::ShowTables(v) => v.analyze(ctx).await,
            DfStatement::ShowSettings(v) => v.analyze(ctx).await,
            DfStatement::ShowProcessList(v) => v.analyze(ctx).await,
            DfStatement::ShowMetrics(v) => v.analyze(ctx).await,
            DfStatement::ShowGrants(v) => v.analyze(ctx).await,
            DfStatement::KillStatement(v) => v.analyze(ctx).await,
            DfStatement::InsertQuery(v) => v.analyze(ctx).await,
            DfStatement::SetVariable(v) => v.analyze(ctx).await,
            DfStatement::CreateUser(v) => v.analyze(ctx).await,
            DfStatement::AlterUser(v) => v.analyze(ctx).await,
            DfStatement::ShowUsers(v) => v.analyze(ctx).await,
            DfStatement::GrantPrivilege(v) => v.analyze(ctx).await,
            DfStatement::RevokePrivilege(v) => v.analyze(ctx).await,
            DfStatement::DropUser(v) => v.analyze(ctx).await,
            DfStatement::Copy(v) => v.analyze(ctx).await,
            DfStatement::CreateStage(v) => v.analyze(ctx).await,
            DfStatement::ShowFunctions(v) => v.analyze(ctx).await,
            DfStatement::DropStage(v) => v.analyze(ctx).await,
            DfStatement::CreateUDF(v) => v.analyze(ctx).await,
            DfStatement::DropUDF(v) => v.analyze(ctx).await,
            DfStatement::AlterUDF(v) => v.analyze(ctx).await,
            DfStatement::ShowEngines(v) => v.analyze(ctx).await,
        }
    }
}
