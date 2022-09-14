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
use std::fmt::Formatter;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_legacy_planners::ExplainType;
use common_legacy_planners::Expression;
use common_legacy_planners::PlanNode;
use common_legacy_planners::ReadDataSourcePlan;

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

impl Default for QueryRelation {
    fn default() -> Self {
        QueryRelation::None
    }
}

#[derive(Clone, Default)]
pub struct QueryAnalyzeState {
    pub filter: Option<Expression>,
    pub having: Option<Expression>,

    pub before_group_by_expressions: Vec<Expression>,
    pub group_by_expressions: Vec<Expression>,
    pub aggregate_expressions: Vec<Expression>,

    pub window_expressions: Vec<Expression>,

    // rebase on projection expressions without aliases, aggregate and group by expressions
    pub distinct_expressions: Vec<Expression>,

    // before order or before projection expression plan
    pub expressions: Vec<Expression>,
    pub order_by_expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,

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

        if !self.window_expressions.is_empty() {
            debug_struct.field("window_func", &self.window_expressions);
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

        if !self.distinct_expressions.is_empty() {
            debug_struct.field("distinct", &self.distinct_expressions);
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
impl<'a> AnalyzableStatement for DfStatement<'a> {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        match self {
            DfStatement::Query(v) => v.analyze(ctx).await,
            DfStatement::Explain(v) => v.analyze(ctx).await,
            DfStatement::InsertQuery(v) => v.analyze(ctx).await,
            DfStatement::Delete(v) => v.analyze(ctx).await,
            // We could never see old planner again.
            // Sad, but that's true.
            DfStatement::SeeYouAgain => {
                unreachable!("query should be forward to new planner")
            }
        }
    }
}
