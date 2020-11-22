// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::FormatterSettings;

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
    pub fn build_plan(
        ctx: Arc<FuseQueryContext>,
        expr: &ast::Expr,
    ) -> FuseQueryResult<ExpressionPlan> {
        match expr {
            ast::Expr::Identifier(ref v) => Ok(ExpressionPlan::Field(v.clone().value)),
            ast::Expr::Value(ast::Value::Number(n)) => match n.parse::<i64>() {
                Ok(n) => Ok(ExpressionPlan::Constant(DataValue::Int64(Some(n)))),
                Err(_) => Ok(ExpressionPlan::Constant(DataValue::Float64(Some(
                    n.parse::<f64>()?,
                )))),
            },
            ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                Ok(ExpressionPlan::Constant(DataValue::String(Some(s.clone()))))
            }
            ast::Expr::BinaryOp { left, op, right } => Ok(ExpressionPlan::BinaryExpression {
                left: Box::new(Self::build_plan(ctx.clone(), left)?),
                op: format!("{}", op),
                right: Box::new(Self::build_plan(ctx, right)?),
            }),
            ast::Expr::Nested(e) => Self::build_plan(ctx, e),

            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported ExpressionPlan Expression: {}",
                expr
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        "ExpressionPlan"
    }

    pub fn format(&self, f: &mut fmt::Formatter, _setting: &mut FormatterSettings) -> fmt::Result {
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
pub fn item_to_expression_step(
    ctx: Arc<FuseQueryContext>,
    item: &ast::SelectItem,
) -> FuseQueryResult<ExpressionPlan> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) => ExpressionPlan::build_plan(ctx, expr),
        _ => Err(FuseQueryError::Unsupported(format!(
            "Unsupported SelectItem {} in item_to_expression_step",
            item
        ))),
    }
}
