// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{EmptyPlan, ExpressionPlan, FormatterSettings, PlanNode};

#[derive(Clone)]
pub struct LimitPlan {
    description: String,
    pub limit: usize,
}

impl LimitPlan {
    pub fn build_plan(
        ctx: Arc<FuseQueryContext>,
        limit: &Option<ast::Expr>,
    ) -> FuseQueryResult<PlanNode> {
        match limit {
            Some(ref expr) => {
                let limit = match ExpressionPlan::build_plan(ctx, expr)? {
                    ExpressionPlan::Constant(DataValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(FuseQueryError::Unsupported(format!(
                        "Unsupported LimitPlan Expr: {}",
                        expr
                    ))),
                }?;
                Ok(PlanNode::Limit(LimitPlan {
                    description: "".to_string(),
                    limit,
                }))
            }
            None => Ok(PlanNode::Empty(EmptyPlan {})),
        }
    }

    pub fn name(&self) -> &'static str {
        "LimitPlan"
    }

    pub fn set_description(&mut self, description: &str) {
        self.description = format!(" ({})", description);
    }

    pub fn format(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        let indent = setting.indent;

        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "{}", setting.indent_char)?;
            }
        }
        write!(
            f,
            "{} Limit: {}{}",
            setting.prefix, self.limit, self.description
        )
    }
}
