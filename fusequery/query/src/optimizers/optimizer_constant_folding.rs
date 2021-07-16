// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::PlanBuilder;
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

struct ConstantFoldingImpl {
    before_group_by_schema: Option<DataSchemaRef>,
}

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

impl PlanRewriter for ConstantFoldingImpl {
    fn rewrite_expr(&mut self, schema: &DataSchemaRef, expr: &Expression) -> Result<Expression> {
        /* TODO: Recursively optimize constant expressions.
         *  such as:
         *      subquery,
         *      a + (1 + (2 + 3)) => a + 6
         */
        constant_folding(schema, expr.clone())
    }

    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
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
}

impl ConstantFoldingImpl {
    pub fn new() -> ConstantFoldingImpl {
        ConstantFoldingImpl {
            before_group_by_schema: None,
        }
    }
}

impl Optimizer for ConstantFoldingOptimizer {
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
