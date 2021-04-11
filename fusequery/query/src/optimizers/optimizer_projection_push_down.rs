// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use common_datavalues::DataSchema;
use common_planners::EmptyPlan;
use common_planners::ExpressionPlan;
use common_planners::PlanNode;
use common_planners::ProjectionPlan;

use crate::optimizers::IOptimizer;
use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ProjectionPushDownOptimizer {}

impl ProjectionPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ProjectionPushDownOptimizer {}
    }
}

/// Recursively walk an expression tree, collecting the unique set of column names
/// referenced in the expression
pub fn expr_to_column_names(expr: &ExpressionPlan, accum: &mut HashSet<String>) -> Result<()> {
    let expressions = Optimizer::expression_plan_children(expr)?;

    let _expressions = expressions
        .iter()
        .map(|e| expr_to_column_names(e, accum))
        .collect::<Result<Vec<_>>>()?;

    if let ExpressionPlan::Column(name) = expr {
        accum.insert(name.clone());
    }
    Ok(())
}

fn optimize_plan(
    optimizer: &ProjectionPushDownOptimizer,
    plan: &PlanNode,
    required_columns: &HashSet<String>,
    _has_projection: bool,
) -> Result<PlanNode> {
    let mut new_required_columns = required_columns.clone();
    match plan {
        PlanNode::Projection(ProjectionPlan {
            expr,
            schema,
            input,
        }) => {
            // projection:
            // * remove any expression that is not required
            // * construct the new set of required columns
            let mut new_expr = Vec::new();
            let mut new_fields = Vec::new();
            // Gather all columns needed for expressions in this projection
            schema
                .fields()
                .iter()
                .enumerate()
                .try_for_each(|(i, field)| {
                    if required_columns.contains(field.name()) {
                        new_expr.push(expr[i].clone());
                        new_fields.push(field.clone());
                        // gather the new set of required columns
                        expr_to_column_names(&expr[i], &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;

            let new_input = optimize_plan(optimizer, &input, &new_required_columns, true)?;
            if new_fields.is_empty() {
                // no need for an expression at all
                Ok(new_input)
            } else {
                Ok(PlanNode::Projection(ProjectionPlan {
                    expr: new_expr,
                    input: Arc::new(new_input),
                    schema: Arc::new(DataSchema::new(new_fields)),
                }))
            }
        }
        _ => Ok(plan.clone()),
    }
}

impl IOptimizer for ProjectionPushDownOptimizer {
    fn name(&self) -> &str {
        "ProjectionPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty()),
        });

        // set of all columns referred by the plan
        let required_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<HashSet<String>>();

        plan.walk_postorder(|node| {
            let mut new_node = optimize_plan(self, node, &required_columns, true)?;
            new_node.set_input(&rewritten_node)?;
            rewritten_node = new_node;
            Ok(true)
        })?;
        Ok(rewritten_node)
    }
}
