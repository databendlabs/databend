// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::ExpressionAction;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ConstantFoldingOptimizer {}

fn is_boolean_type(schema: &DataSchemaRef, expr: &ExpressionAction) -> Result<bool> {
    if let DataType::Boolean = expr.to_data_field(schema)?.data_type() {
        return Ok(true);
    }
    Ok(false)
}

struct ConstantFoldingImpl {}

fn constant_folding(schema: &DataSchemaRef, expr: ExpressionAction) -> Result<ExpressionAction> {
    let new_expr = match expr {
        ExpressionAction::BinaryExpression { left, op, right } => match op.as_str() {
            "=" => match (left.as_ref(), right.as_ref()) {
                (
                    ExpressionAction::Literal(DataValue::Boolean(l)),
                    ExpressionAction::Literal(DataValue::Boolean(r))
                ) => match (l, r) {
                    (Some(l), Some(r)) => {
                        ExpressionAction::Literal(DataValue::Boolean(Some(l == r)))
                    }
                    _ => ExpressionAction::Literal(DataValue::Boolean(None))
                },
                (ExpressionAction::Literal(DataValue::Boolean(b)), _)
                    if is_boolean_type(schema, &right)? =>
                {
                    match b {
                        Some(true) => *right,
                        // Fix this after we implement NOT
                        Some(false) => ExpressionAction::BinaryExpression { left, op, right },
                        None => ExpressionAction::Literal(DataValue::Boolean(None))
                    }
                }
                (_, ExpressionAction::Literal(DataValue::Boolean(b)))
                    if is_boolean_type(schema, &left)? =>
                {
                    match b {
                        Some(true) => *left,
                        // Fix this after we implement NOT
                        Some(false) => ExpressionAction::BinaryExpression { left, op, right },
                        None => ExpressionAction::Literal(DataValue::Boolean(None))
                    }
                }
                _ => ExpressionAction::BinaryExpression {
                    left,
                    op: "=".to_string(),
                    right
                }
            },
            "!=" => match (left.as_ref(), right.as_ref()) {
                (
                    ExpressionAction::Literal(DataValue::Boolean(l)),
                    ExpressionAction::Literal(DataValue::Boolean(r))
                ) => match (l, r) {
                    (Some(l), Some(r)) => {
                        ExpressionAction::Literal(DataValue::Boolean(Some(l != r)))
                    }
                    _ => ExpressionAction::Literal(DataValue::Boolean(None))
                },
                (ExpressionAction::Literal(DataValue::Boolean(b)), _)
                    if is_boolean_type(schema, &right)? =>
                {
                    match b {
                        Some(true) => ExpressionAction::BinaryExpression { left, op, right },
                        Some(false) => *right,
                        None => ExpressionAction::Literal(DataValue::Boolean(None))
                    }
                }
                (_, ExpressionAction::Literal(DataValue::Boolean(b)))
                    if is_boolean_type(schema, &left)? =>
                {
                    match b {
                        Some(true) => ExpressionAction::BinaryExpression { left, op, right },
                        Some(false) => *left,
                        None => ExpressionAction::Literal(DataValue::Boolean(None))
                    }
                }
                _ => ExpressionAction::BinaryExpression {
                    left,
                    op: "!=".to_string(),
                    right
                }
            },
            _ => ExpressionAction::BinaryExpression { left, op, right }
        },
        expr => {
            // do nothing
            expr
        }
    };
    Ok(new_expr)
}

impl<'plan> PlanRewriter<'plan> for ConstantFoldingImpl {
    fn rewrite_filter(&mut self, plan: &FilterPlan) -> Result<PlanNode> {
        let schema = plan.schema();
        let mut new_plan = plan.clone();
        new_plan.predicate = constant_folding(&schema, plan.predicate.clone())?;
        new_plan.input = Arc::new(self.rewrite_plan_node(&plan.input)?);
        Ok(PlanNode::Filter(new_plan))
    }

    fn rewrite_expression(&mut self, plan: &ExpressionPlan) -> Result<PlanNode> {
        let mut new_exprs = Vec::new();
        let schema = plan.schema();

        for e in plan.exprs.as_slice() {
            let new_expr = constant_folding(&schema, e.clone())?;
            new_exprs.push(new_expr);
        }
        let mut new_plan = plan.clone();
        new_plan.exprs = new_exprs;
        Ok(PlanNode::Expression(new_plan))
    }
}

impl ConstantFoldingImpl {
    pub fn new() -> ConstantFoldingImpl {
        ConstantFoldingImpl {}
    }
}

impl IOptimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ConstantFoldingImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}
