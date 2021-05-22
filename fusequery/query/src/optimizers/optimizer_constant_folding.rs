// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::EmptyPlan;
use common_planners::ExpressionAction;
use common_planners::ExpressionPlan;
use common_planners::PlanNode;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ConstantFoldingOptimizer {}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}

fn is_boolean_type(schema: &DataSchemaRef, expr: &ExpressionAction) -> bool {
    if let DataType::Boolean = expr.to_data_field(schema).unwrap().data_type() {
        return true;
    }
    false
}

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
                    if is_boolean_type(schema, &right) =>
                {
                    match b {
                        Some(true) => *right,
                        // Fix this after we implement NOT
                        Some(false) => ExpressionAction::BinaryExpression { left, op, right },
                        None => ExpressionAction::Literal(DataValue::Boolean(None))
                    }
                }
                (_, ExpressionAction::Literal(DataValue::Boolean(b)))
                    if is_boolean_type(schema, &left) =>
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
                    if is_boolean_type(schema, &right) =>
                {
                    match b {
                        Some(true) => ExpressionAction::BinaryExpression { left, op, right },
                        Some(false) => *right,
                        None => ExpressionAction::Literal(DataValue::Boolean(None))
                    }
                }
                (_, ExpressionAction::Literal(DataValue::Boolean(b)))
                    if is_boolean_type(schema, &left) =>
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

impl IOptimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        plan.walk_postorder(|node| -> Result<bool> {
            if let PlanNode::Expression(ExpressionPlan {
                exprs,
                schema: _,
                input: _,
                desc
            }) = node
            {
                let mut new_exprs = Vec::new();
                let schema = node.schema();
                for e in exprs {
                    let new_expr = constant_folding(&schema, e.clone())?;
                    new_exprs.push(new_expr);
                }
                let mut new_node = PlanNode::Expression(ExpressionPlan {
                    exprs: new_exprs,
                    schema: schema,
                    input: Arc::from(PlanNode::Empty(EmptyPlan::create())),
                    desc: desc.to_string()
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
