// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::EmptyPlan;
use common_planners::Expression;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RewriteHelper;
use common_planners::SortPlan;

use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ProjectionPushDownOptimizer {}

struct ProjectionPushDownImpl {
    pub required_columns: HashSet<String>,
    pub has_projection: bool,
}

impl<'plan> PlanRewriter<'plan> for ProjectionPushDownImpl {
    fn rewrite_projection(&mut self, plan: &ProjectionPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(plan.expr.as_slice())?;
        self.has_projection = true;
        let mut new_plan = plan.clone();
        new_plan.input = Arc::new(self.rewrite_plan_node(&plan.input)?);
        Ok(PlanNode::Projection(new_plan))
    }

    fn rewrite_filter(&mut self, plan: &FilterPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr(&plan.predicate)?;
        let mut new_plan = plan.clone();
        new_plan.input = Arc::new(self.rewrite_plan_node(&plan.input)?);
        Ok(PlanNode::Filter(new_plan))
    }

    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(&plan.group_expr)
            .and_then(|_| self.collect_column_names_from_expr_vec(&plan.aggr_expr))?;
        let mut new_plan = plan.clone();
        new_plan.input = Arc::new(self.rewrite_plan_node(&plan.input)?);
        Ok(PlanNode::AggregatorPartial(new_plan))
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(&plan.group_expr)
            .and_then(|_| self.collect_column_names_from_expr_vec(&plan.aggr_expr))?;
        let mut new_plan = plan.clone();
        new_plan.input = Arc::new(self.rewrite_plan_node(&plan.input)?);
        Ok(PlanNode::AggregatorFinal(new_plan))
    }

    fn rewrite_sort(&mut self, plan: &SortPlan) -> Result<PlanNode> {
        self.collect_column_names_from_expr_vec(plan.order_by.as_slice())?;
        let mut new_plan = plan.clone();
        new_plan.input = Arc::new(self.rewrite_plan_node(&plan.input)?);
        Ok(PlanNode::Sort(new_plan))
    }

    fn rewrite_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        self.get_projected_schema(plan.schema.as_ref())
            .map(|projected_schema| {
                PlanNode::ReadSource(ReadDataSourcePlan {
                    db: plan.db.to_string(),
                    table: plan.table.to_string(),
                    schema: projected_schema,
                    parts: plan.parts.clone(),
                    statistics: plan.statistics.clone(),
                    description: plan.description.to_string(),
                    scan_plan: plan.scan_plan.clone(),
                    remote: plan.remote,
                })
            })
    }

    fn rewrite_empty(&mut self, plan: &EmptyPlan) -> Result<PlanNode> {
        Ok(PlanNode::Empty(plan.clone()))
    }
}

impl ProjectionPushDownImpl {
    pub fn new() -> ProjectionPushDownImpl {
        ProjectionPushDownImpl {
            required_columns: HashSet::new(),
            has_projection: false,
        }
    }

    // Recursively walk a list of expression trees, collecting the unique set of column
    // names referenced in the expression
    fn collect_column_names_from_expr_vec(&mut self, expr: &[Expression]) -> Result<()> {
        expr.iter().fold(Ok(()), |acc, e| {
            acc.and_then(|_| self.collect_column_names_from_expr(e))
        })
    }

    // Recursively walk an expression tree, collecting the unique set of column names
    // referenced in the expression
    fn collect_column_names_from_expr(&mut self, expr: &Expression) -> Result<()> {
        RewriteHelper::expression_plan_children(expr)?
            .iter()
            .fold(Ok(()), |acc, e| {
                acc.and_then(|_| self.collect_column_names_from_expr(e))
            })?;

        if let Expression::Column(name) = expr {
            self.required_columns.insert(name.clone());
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

#[async_trait::async_trait]
impl Optimizer for ProjectionPushDownOptimizer {
    fn name(&self) -> &str {
        "ProjectionPushDown"
    }

    async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ProjectionPushDownImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ProjectionPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> ProjectionPushDownOptimizer {
        ProjectionPushDownOptimizer {}
    }
}
