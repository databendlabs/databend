// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct FilterPlan {
    pub predicate: ExpressionPlan,
}

impl FilterPlan {
    pub fn build_plan(ctx: Context, limit: &Option<ast::Expr>) -> Result<PlanNode> {
        match limit {
            Some(ref expr) => Ok(PlanNode::Filter(FilterPlan {
                predicate: ExpressionPlan::build_plan(ctx, expr)?,
            })),
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
        write!(f, "{} Filter: {}", setting.prefix, self.predicate)
    }
}
