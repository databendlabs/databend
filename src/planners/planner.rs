// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::{Error, Result};

use crate::planners::SelectPlan;

pub struct Planner;

impl Planner {
    /// Creates a new planner.
    pub fn new() -> Self {
        Self {}
    }

    /// Builds plan from AST statement.
    pub fn build(&self, ctx: Context, statement: &ast::Statement) -> Result<Arc<dyn IPlanNode>> {
        match statement {
            ast::Statement::Query(query) => SelectPlan::build_plan(ctx, query.as_ref()),
            _ => Err(Error::Unsupported(format!(
                "Unsupported statement: {} in planner.build()",
                statement
            ))),
        }
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}

/// Formatter settings for PlanStep debug.
pub struct FormatterSettings {
    pub indent: usize,
    pub indent_char: &'static str,
    pub prefix: &'static str,
}

pub trait IPlanNode: fmt::Debug {
    fn name(&self) -> &'static str;
    fn describe(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result;
}
