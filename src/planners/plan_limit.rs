// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::datatypes::DataValue;
use crate::error::{Error, Result};

use crate::planners::{EmptyPlan, ExpressionPlan, FormatterSettings, IPlanNode};

pub struct LimitPlan {
    pub limit: usize,
}

impl LimitPlan {
    pub fn build_plan(ctx: Context, limit: &Option<ast::Expr>) -> Result<Arc<dyn IPlanNode>> {
        match limit {
            Some(ref expr) => {
                let limit = match ExpressionPlan::build_plan(ctx, expr)? {
                    ExpressionPlan::Constant(DataValue::Int64(n)) => Ok(n as usize),
                    _ => Err(Error::Unsupported(format!(
                        "Unsupported LimitPlan Expr: {}",
                        expr
                    ))),
                }?;
                Ok(Arc::new(LimitPlan { limit }))
            }
            None => Ok(Arc::new(EmptyPlan {})),
        }
    }
}

impl IPlanNode for LimitPlan {
    fn name(&self) -> &'static str {
        "LimitPlan"
    }

    fn describe(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
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

impl fmt::Debug for LimitPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.limit)
    }
}
