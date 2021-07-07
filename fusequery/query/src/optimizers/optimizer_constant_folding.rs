// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ConstantFoldingOptimizer {}

fn is_boolean_type(schema: &DataSchemaRef, expr: &Expression) -> Result<bool> {
    if let DataType::Boolean = expr.to_data_field(schema)?.data_type() {
        return Ok(true);
    }
    Ok(false)
}

struct ConstantFoldingImpl {}

fn constant_folding(schema: &DataSchemaRef, expr: Expression) -> Result<Expression> {
    let new_expr = match expr {
        Expression::BinaryExpression { left, op, right } => match op.as_str() {
            "=" => match (left.as_ref(), right.as_ref()) {
                (
                    Expression::Literal(DataValue::Boolean(l)),
                    Expression::Literal(DataValue::Boolean(r)),
                ) => match (l, r) {
                    (Some(l), Some(r)) => Expression::Literal(DataValue::Boolean(Some(l == r))),
                    _ => Expression::Literal(DataValue::Boolean(None)),
                },
                (Expression::Literal(DataValue::Boolean(b)), _)
                    if is_boolean_type(schema, &right)? =>
                {
                    match b {
                        Some(true) => *right,
                        // Fix this after we implement NOT
                        Some(false) => Expression::BinaryExpression { left, op, right },
                        None => Expression::Literal(DataValue::Boolean(None)),
                    }
                }
                (_, Expression::Literal(DataValue::Boolean(b)))
                    if is_boolean_type(schema, &left)? =>
                {
                    match b {
                        Some(true) => *left,
                        // Fix this after we implement NOT
                        Some(false) => Expression::BinaryExpression { left, op, right },
                        None => Expression::Literal(DataValue::Boolean(None)),
                    }
                }
                _ => Expression::BinaryExpression {
                    left,
                    op: "=".to_string(),
                    right,
                },
            },
            "!=" => match (left.as_ref(), right.as_ref()) {
                (
                    Expression::Literal(DataValue::Boolean(l)),
                    Expression::Literal(DataValue::Boolean(r)),
                ) => match (l, r) {
                    (Some(l), Some(r)) => Expression::Literal(DataValue::Boolean(Some(l != r))),
                    _ => Expression::Literal(DataValue::Boolean(None)),
                },
                (Expression::Literal(DataValue::Boolean(b)), _)
                    if is_boolean_type(schema, &right)? =>
                {
                    match b {
                        Some(true) => Expression::BinaryExpression { left, op, right },
                        Some(false) => *right,
                        None => Expression::Literal(DataValue::Boolean(None)),
                    }
                }
                (_, Expression::Literal(DataValue::Boolean(b)))
                    if is_boolean_type(schema, &left)? =>
                {
                    match b {
                        Some(true) => Expression::BinaryExpression { left, op, right },
                        Some(false) => *left,
                        None => Expression::Literal(DataValue::Boolean(None)),
                    }
                }
                _ => Expression::BinaryExpression {
                    left,
                    op: "!=".to_string(),
                    right,
                },
            },
            _ => Expression::BinaryExpression { left, op, right },
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

#[async_trait::async_trait]
impl Optimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ConstantFoldingImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}
