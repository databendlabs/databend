// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use crate::contexts::Context;
use crate::datatypes::DataValue;
use crate::error::{Error, Result};

use crate::planners::{FormatterSettings, IPlanNode};

#[derive(Clone)]
pub enum ExpressionPlan {
    /// Column field name in String.
    Field(String),
    /// Constant value in DataType.
    Constant(DataValue),
    BinaryExpression {
        left: Box<ExpressionPlan>,
        op: String,
        right: Box<ExpressionPlan>,
    },
}

impl ExpressionPlan {
    pub fn build_plan(ctx: Context, expr: &ast::Expr) -> Result<ExpressionPlan> {
        match expr {
            ast::Expr::Identifier(ref v) => Ok(ExpressionPlan::Field(v.clone().value)),
            ast::Expr::Value(ast::Value::Number(n)) => match n.parse::<i64>() {
                Ok(n) => Ok(ExpressionPlan::Constant(DataValue::Int64(n))),
                Err(_) => Ok(ExpressionPlan::Constant(DataValue::Float64(
                    n.parse::<f64>()?,
                ))),
            },
            ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                Ok(ExpressionPlan::Constant(DataValue::String(s.clone())))
            }
            ast::Expr::BinaryOp { left, op, right } => Ok(ExpressionPlan::BinaryExpression {
                left: Box::new(Self::build_plan(ctx.clone(), left)?),
                op: format!("{}", op),
                right: Box::new(Self::build_plan(ctx, right)?),
            }),
            ast::Expr::Nested(e) => Self::build_plan(ctx, e),

            _ => Err(Error::Unsupported(format!(
                "Unsupported ExpressionPlan Expression: {}",
                expr
            ))),
        }
    }
}

impl IPlanNode for ExpressionPlan {
    fn name(&self) -> &'static str {
        "ExpressionPlan"
    }

    fn describe(&self, f: &mut fmt::Formatter, _setting: &mut FormatterSettings) -> fmt::Result {
        write!(f, "")
    }
}

impl fmt::Debug for ExpressionPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionPlan::Field(ref v) => write!(f, "{}", v),
            ExpressionPlan::Constant(ref v) => write!(f, "{:?}", v),
            ExpressionPlan::BinaryExpression { left, op, right } => {
                write!(f, "{:?} {} {:?}", left, op, right,)
            }
        }
    }
}

/// SQL.SelectItem to ExpressionStep.
pub fn item_to_expression_step(ctx: Context, item: &ast::SelectItem) -> Result<ExpressionPlan> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) => ExpressionPlan::build_plan(ctx, expr),
        _ => Err(Error::Unsupported(format!(
            "Unsupported SelectItem {} in item_to_expression_step",
            item
        ))),
    }
}
