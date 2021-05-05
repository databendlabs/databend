// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;

use common_arrow::arrow::error::Result as ArrowResult;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::ProjectionPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::SortPlan;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ProjectionPushDownOptimizer {}

impl ProjectionPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ProjectionPushDownOptimizer {}
    }
}

// Recursively walk an expression tree, collecting the unique set of column names
// referenced in the expression
fn expr_to_column_names(expr: &ExpressionPlan, accum: &mut HashSet<String>) -> Result<()> {
    let expressions = PlanRewriter::expression_plan_children(expr)?;

    let _expressions = expressions
        .iter()
        .map(|e| expr_to_column_names(e, accum))
        .collect::<Result<Vec<_>>>()?;

    if let ExpressionPlan::Column(name) = expr {
        accum.insert(name.clone());
    }
    Ok(())
}

// Recursively walk a list of expression trees, collecting the unique set of column
// names referenced in the expression
fn exprvec_to_column_names(expr: &[ExpressionPlan], accum: &mut HashSet<String>) -> Result<()> {
    for e in expr {
        expr_to_column_names(e, accum)?;
    }
    Ok(())
}

fn get_projected_schema(
    schema: &DataSchema,
    required_columns: &HashSet<String>,
    has_projection: bool
) -> Result<DataSchemaRef> {
    // Discard non-existing columns, e.g. when the column derives from aggregation
    let mut projection: Vec<usize> = required_columns
        .iter()
        .map(|name| schema.index_of(name))
        .filter_map(ArrowResult::ok)
        .collect();
    if projection.is_empty() {
        if has_projection {
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
    Ok(DataSchemaRefExt::create_with_metadata(projected_fields))
}

fn optimize_plan(
    optimizer: &ProjectionPushDownOptimizer,
    plan: &PlanNode,
    required_columns: &HashSet<String>,
    has_projection: bool
) -> Result<PlanNode> {
    let mut new_required_columns = required_columns.clone();
    match plan {
        PlanNode::Projection(ProjectionPlan {
            expr,
            schema: _,
            input
        }) => {
            exprvec_to_column_names(expr, &mut new_required_columns)?;
            let new_input = optimize_plan(optimizer, &input, &new_required_columns, true)?;
            let mut cloned_plan = plan.clone();
            cloned_plan.set_input(&new_input)?;
            Ok(cloned_plan)
        }
        PlanNode::Filter(FilterPlan { predicate, input }) => {
            expr_to_column_names(predicate, &mut new_required_columns)?;
            let new_input =
                optimize_plan(optimizer, &input, &new_required_columns, has_projection)?;
            let mut cloned_plan = plan.clone();
            cloned_plan.set_input(&new_input)?;
            Ok(cloned_plan)
        }
        PlanNode::Sort(SortPlan { order_by, input }) => {
            exprvec_to_column_names(order_by, &mut new_required_columns)?;
            let new_input =
                optimize_plan(optimizer, &input, &new_required_columns, has_projection)?;
            let mut cloned_plan = plan.clone();
            cloned_plan.set_input(&new_input)?;
            Ok(cloned_plan)
        }
        PlanNode::AggregatorFinal(AggregatorFinalPlan {
            aggr_expr,
            group_expr,
            schema: _,
            input
        }) => {
            // final aggregate:
            exprvec_to_column_names(group_expr, &mut new_required_columns)?;
            exprvec_to_column_names(aggr_expr, &mut new_required_columns)?;
            let new_input = optimize_plan(optimizer, &input, &new_required_columns, true)?;
            let mut cloned_plan = plan.clone();
            cloned_plan.set_input(&new_input)?;
            Ok(cloned_plan)
        }
        PlanNode::AggregatorPartial(AggregatorPartialPlan {
            aggr_expr,
            group_expr,
            input
        }) => {
            // Partial aggregate:
            exprvec_to_column_names(group_expr, &mut new_required_columns)?;
            exprvec_to_column_names(aggr_expr, &mut new_required_columns)?;
            let new_input = optimize_plan(optimizer, &input, &new_required_columns, true)?;
            let mut cloned_plan = plan.clone();
            cloned_plan.set_input(&new_input)?;
            Ok(cloned_plan)
        }
        PlanNode::ReadSource(ReadDataSourcePlan {
            db,
            table,
            schema,
            partitions,
            statistics,
            description
        }) => {
            let projected_schema = get_projected_schema(schema, required_columns, has_projection)?;

            Ok(PlanNode::ReadSource(ReadDataSourcePlan {
                db: db.to_string(),
                table: table.to_string(),
                schema: projected_schema,
                partitions: partitions.clone(),
                statistics: statistics.clone(),
                description: description.to_string()
            }))
        }
        PlanNode::Empty(_) => Ok(plan.clone()),
        _ => {
            let input = plan.input();
            let new_input = optimize_plan(optimizer, &input, &required_columns, has_projection)?;
            let mut cloned_plan = plan.clone();
            cloned_plan.set_input(&new_input)?;
            Ok(cloned_plan)
        }
    }
}

impl IOptimizer for ProjectionPushDownOptimizer {
    fn name(&self) -> &str {
        "ProjectionPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let required_columns = HashSet::new();
        let new_node = optimize_plan(self, plan, &required_columns, false)?;
        Ok(new_node)
    }
}
