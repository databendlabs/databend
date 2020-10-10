// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct LimitPlan {
    pub limit: usize,
}

impl LimitPlan {
    pub fn build_plan(limit: &Option<ast::Expr>) -> Result<PlanNode> {
        match limit {
            Some(ref expr) => {
                let limit = match ExpressionPlan::build_plan(expr)? {
                    ExpressionPlan::Constant(DataValue::Int64(n)) => Ok(n as usize),
                    _ => Err(Error::Unsupported(format!(
                        "Unsupported LimitPlan Expr: {}",
                        expr
                    ))),
                }?;
                Ok(PlanNode::Limit(LimitPlan { limit }))
            }
            None => Ok(PlanNode::Empty(EmptyPlan {})),
        }
    }
    pub fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> fmt::Result {
        let indent = setting.indent;

        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "{}", setting.indent_char)?;
            }
        }
        write!(f, "{} Limit: {}", setting.prefix, self.limit)
    }
}
