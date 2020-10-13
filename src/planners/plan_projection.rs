// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::Result;

use crate::planners::plan_expression::item_to_expression_step;
use crate::planners::{ExpressionPlan, FormatterSettings, IPlanNode};

pub struct ProjectionPlan {
    pub expr: Vec<ExpressionPlan>,
}

impl ProjectionPlan {
    pub fn build_plan(ctx: Context, items: &[ast::SelectItem]) -> Result<Arc<dyn IPlanNode>> {
        let expr = items
            .iter()
            .map(|expr| item_to_expression_step(ctx.clone(), expr))
            .collect::<Result<Vec<ExpressionPlan>>>()?;

        Ok(Arc::new(ProjectionPlan { expr }))
    }
}

impl IPlanNode for ProjectionPlan {
    fn name(&self) -> &'static str {
        "ProjectionPlan"
    }

    fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> fmt::Result {
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

impl fmt::Debug for ProjectionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.expr)
    }
}
