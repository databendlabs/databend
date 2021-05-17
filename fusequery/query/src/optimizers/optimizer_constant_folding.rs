// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::EmptyPlan;
use common_planners::PlanNode;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ConstantFoldingOptimizer {}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}

fn is_boolean_type(schema: DataSchemaRef, expr: &ExpressionAction) -> bool {
    let data_type = expr.to_data_field(schema)?.unwrap().data_type();
    if let Ok(DataType::Boolean) = data_type {
        return true; 
    }
    false
}

fn constant_folding(schema: DataSchemaRef, expr: ExpressionAction) -> Result<ExpressionAction> {
    let new_expr = match expr {
        ExpressionAction::BinaryExpression { left, op, right } => match op.as_str() {
            "=" => match (left.as_ref(), right.as_ref()) {
                (
                    ExpressionAction::Literal(DataType::Boolean(l)),
                    ExpressionAction::Literal(DataType::Boolean(r))
                ) => match (l, r) {
                    (Some(l), Some(r)) => ExpressionAction::Literal(DataType::Boolean(Some(l == r))),
                    _ => ExpressionAction::Literal(DataType::Boolean(None))
                },
                (ExpressionAction::Literal(DataType::Boolean(b)), _) if is_boolean_type(schema, &right) => {
                    match b {
                        Some(true) => *right,
                        Some(false) => ExpressionAction::Not(right),
                        None => ExpressionAction::Literal(DataType::Boolean(None))
                    }
                }
                (_, ExpressionAction::Literal(DataType::Boolean(b))) if self.is_boolean_type(schema, &left) => {
                    match b {
                        Some(true) => *left,
                        Some(false) => ExpressionAction::Not(left),
                        None => ExpressionAction::Literal(DataType::Boolean(None))
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
                    ExpressionAction::Literal(DataType::Boolean(l)),
                    ExpressionAction::Literal(DataType::Boolean(r))
                ) => match (l, r) {
                    (Some(l), Some(r)) => ExpressionAction::Literal(DataType::Boolean(Some(l != r))),
                    _ => ExpressionAction::Literal(DataType::Boolean(None))
                },
                (ExpressionAction::Literal(DataType::Boolean(b)), _) if self.is_boolean_type(schema, &right) => {
                    match b {
                        Some(true) => ExpressionAction::Not(right),
                        Some(false) => *right,
                        None => ExpressionAction::Literal(DataType::Boolean(None))
                    }
                }
                (_, ExpressionAction::Literal(DataType::Boolean(b))) if self.is_boolean_type(schema, &left) => {
                    match b {
                        Some(true) => ExpressionAction::Not(left),
                        Some(false) => *left,
                        None => ExpressionAction::Literal(DataType::Boolean(None))
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

impl IOptimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        plan.walk_postorder(|node| -> Result<bool> {
            if let PlanNode::Expression(ExpressionPlan { exprs, schema, input, desc }) = node {
                let mut new_exprs = Vec::new();
                let schema = node.schema();
                for e in exprs {
                    let new_expr = constant_folding(scheme, e)?;
                    new_exprs.push(new_expr);
                }
                let new_node = PlanNode::Expression(ExpressionPlan {
                    exprs: new_exprs,
                    schema: schema,
                    input: input,
                    desc: desc
                });
                new_node.set_input(&rewritten_node);
                rewritten_node = new_node;
            } else {
                let mut clone_node = node.clone();
                clone_node.set_input(&rewritten_node);
                rewritten_node = clone_node;
            }
            Ok(true)
        })?;

        Ok(rewritten_node)
    }
}
