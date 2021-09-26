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

use std::collections::HashSet;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::EmptyPlan;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::ExpressionVisitor;
use common_planners::FilterPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::Recursion;
use common_planners::SortPlan;

use crate::optimizers::Optimizer;
use crate::sessions::DatabendQueryContextRef;

pub struct ProjectionPushDownOptimizer {}

struct ProjectionPushDownImpl {
    pub required_columns: HashSet<String>,
    pub has_projection: bool,
    pub before_group_by_schema: Option<DataSchemaRef>,
}

impl PlanRewriter for ProjectionPushDownImpl {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(&plan.group_expr)?;
        self.collect_column_names_from_expr_vec(&plan.aggr_expr)?;
        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema {
            Some(_) => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be None",
            )),
            None => {
                self.before_group_by_schema = Some(new_input.schema());
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&plan.aggr_expr, &plan.group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(&plan.group_expr)?;
        self.collect_column_names_from_expr_vec(&plan.aggr_expr)?;
        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema.take() {
            None => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be Some",
            )),
            Some(schema_before_group_by) => PlanBuilder::from(&new_input)
                .aggregate_final(schema_before_group_by, &plan.aggr_expr, &plan.group_expr)?
                .build(),
        }
    }

    fn rewrite_empty(&mut self, plan: &EmptyPlan) -> Result<PlanNode> {
        Ok(PlanNode::Empty(plan.clone()))
    }

    fn rewrite_projection(&mut self, plan: &ProjectionPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(plan.expr.as_slice())?;
        self.has_projection = true;
        let new_input = self.rewrite_plan_node(&plan.input)?;
        PlanBuilder::from(&new_input)
            .project(&self.rewrite_exprs(&new_input.schema(), &plan.expr)?)?
            .build()
    }

    fn rewrite_expression(&mut self, plan: &ExpressionPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(&plan.exprs)?;
        let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
        PlanBuilder::from(&new_input)
            .expression(&plan.exprs, &plan.desc)?
            .build()
    }

    fn rewrite_filter(&mut self, plan: &FilterPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr(&plan.predicate)?;
        let new_input = self.rewrite_plan_node(&plan.input)?;
        PlanBuilder::from(&new_input)
            .filter(self.rewrite_expr(&new_input.schema(), &plan.predicate)?)?
            .build()
    }

    fn rewrite_sort(&mut self, plan: &SortPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(plan.order_by.as_slice())?;
        let new_input = self.rewrite_plan_node(&plan.input)?;
        PlanBuilder::from(&new_input)
            .sort(&self.rewrite_exprs(&new_input.schema(), &plan.order_by)?)?
            .build()
    }

    fn rewrite_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        // TODO: rewrite scan
        self.get_projected_schema(plan.schema.as_ref())
            .map(|projected_schema| {
                PlanNode::ReadSource(ReadDataSourcePlan {
                    db: plan.db.to_string(),
                    table: plan.table.to_string(),
                    table_id: plan.table_id,
                    table_version: plan.table_version,
                    schema: projected_schema,
                    parts: plan.parts.clone(),
                    statistics: plan.statistics.clone(),
                    description: plan.description.to_string(),
                    scan_plan: plan.scan_plan.clone(),
                    remote: plan.remote,
                    tbl_args: plan.tbl_args.clone(),
                    push_downs: plan.push_downs.clone(),
                })
            })
    }
}

struct RequireColumnsVisitor {
    required_columns: HashSet<String>,
}
impl RequireColumnsVisitor {
    pub fn new() -> Self {
        Self {
            required_columns: HashSet::new(),
        }
    }
}

impl ExpressionVisitor for RequireColumnsVisitor {
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>> {
        match expr {
            Expression::Column(c) => {
                let mut v = self;
                v.required_columns.insert(c.clone());
                Ok(Recursion::Continue(v))
            }
            _ => Ok(Recursion::Continue(self)),
        }
    }
}

impl ProjectionPushDownImpl {
    pub fn new() -> ProjectionPushDownImpl {
        ProjectionPushDownImpl {
            required_columns: HashSet::new(),
            has_projection: false,
            before_group_by_schema: None,
        }
    }

    // Recursively walk a list of expression trees, collecting the unique set of column
    // names referenced in the expression
    fn collect_column_names_from_expr_vec(&mut self, exprs: &[Expression]) -> Result<()> {
        for expr in exprs {
            self.collect_column_names_from_expr(expr)?;
        }
        Ok(())
    }

    // Recursively walk an expression tree, collecting the unique set of column names
    // referenced in the expression
    fn collect_column_names_from_expr(&mut self, expr: &Expression) -> Result<()> {
        let mut visitor = RequireColumnsVisitor::new();
        visitor = expr.accept(visitor)?;
        for k in visitor.required_columns {
            self.required_columns.insert(k);
        }
        Ok(())
    }

    fn get_projected_schema(&self, schema: &DataSchema) -> Result<DataSchemaRef> {
        // Discard non-existing columns, e.g. when the column derives from aggregation
        let mut projection: Vec<usize> = self
            .required_columns
            .iter()
            .map(|name| schema.index_of(name))
            .filter_map(Result::ok)
            .collect();

        if projection.is_empty() {
            if self.has_projection {
                // Ensure reading at lease one column
                projection.push(0);
            } else {
                // for table scan without projection
                // just return all columns
                projection = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, _)| i)
                    .collect::<Vec<usize>>();
            }
        }
        // sort the projection to get deterministic behavior
        projection.sort_unstable();

        // create the projected schema
        let mut projected_fields: Vec<DataField> = Vec::with_capacity(projection.len());
        for i in &projection {
            projected_fields.push(schema.fields()[*i].clone());
        }
        Ok(DataSchemaRefExt::create(projected_fields))
    }
}

impl Optimizer for ProjectionPushDownOptimizer {
    fn name(&self) -> &str {
        "ProjectionPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ProjectionPushDownImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ProjectionPushDownOptimizer {
    pub fn create(_ctx: DatabendQueryContextRef) -> ProjectionPushDownOptimizer {
        ProjectionPushDownOptimizer {}
    }
}
