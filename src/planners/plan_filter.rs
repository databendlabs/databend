// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::Result;
use crate::planners::{EmptyPlan, ExpressionPlan, FormatterSettings, IPlanNode};

pub struct FilterPlan {
    pub predicate: ExpressionPlan,
}

impl FilterPlan {
    pub fn build_plan(ctx: Context, limit: &Option<ast::Expr>) -> Result<Arc<dyn IPlanNode>> {
        match limit {
            Some(ref expr) => Ok(Arc::new(FilterPlan {
                predicate: ExpressionPlan::build_plan(ctx, expr)?,
            })),
            None => Ok(Arc::new(EmptyPlan {})),
        }
    }
}

impl IPlanNode for FilterPlan {
    fn name(&self) -> &'static str {
        "FilterPlan"
    }

    fn describe(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        let indent = setting.indent;

        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "{}", setting.indent_char)?;
            }
        }
        write!(f, "{} Filter: {:?}", setting.prefix, self.predicate)
    }
}

impl fmt::Debug for FilterPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.predicate)
    }
}
