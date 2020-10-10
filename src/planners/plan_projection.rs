// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct ProjectionPlan {
    pub expr: Vec<ExpressionPlan>,
}

impl ProjectionPlan {
    pub fn build_plan(items: &[ast::SelectItem]) -> Result<PlanNode> {
        let expr = items
            .iter()
            .map(|expr| item_to_expression_step(expr))
            .collect::<Result<Vec<ExpressionPlan>>>()?;

        Ok(PlanNode::Projection(ProjectionPlan { expr }))
    }
    pub fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> fmt::Result {
        write!(f, "{} Projection: ", setting.prefix)?;
        for i in 0..self.expr.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", self.expr[i])?;
        }
        write!(f, "")
    }
}
