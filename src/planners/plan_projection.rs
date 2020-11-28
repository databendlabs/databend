// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::error::FuseQueryResult;
use crate::planners::{
    plan_expression::item_to_expression_plan, ExpressionPlan, FormatterSettings, PlanNode,
};

#[derive(Clone)]
pub struct ProjectionPlan {
    description: String,
    pub expr: Vec<ExpressionPlan>,
}

impl ProjectionPlan {
    pub fn try_create(
        ctx: Arc<FuseQueryContext>,
        items: &[ast::SelectItem],
    ) -> FuseQueryResult<PlanNode> {
        let expr = items
            .iter()
            .map(|expr| item_to_expression_plan(ctx.clone(), expr))
            .collect::<FuseQueryResult<Vec<ExpressionPlan>>>()?;

        Ok(PlanNode::Projection(ProjectionPlan {
            description: "".to_string(),
            expr,
        }))
    }

    pub fn name(&self) -> &'static str {
        "ProjectionPlan"
    }

    pub fn format(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        write!(f, "{} Projection: ", setting.prefix)?;
        for i in 0..self.expr.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", self.expr[i])?;
        }
        write!(f, "")
    }
}
