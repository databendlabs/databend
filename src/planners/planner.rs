// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

pub struct Planner;

impl Planner {
    /// Creates a new planner.
    pub fn new() -> Self {
        Self {}
    }

    /// Builds plan from AST statement.
    pub fn build(&self, ctx: Context, statement: &ast::Statement) -> Result<PlanNode> {
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

/// A PlanNode represents as a algebra node from AST.
#[derive(Clone, PartialEq)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Expression(ExpressionPlan),
    Filter(FilterPlan),
    Limit(LimitPlan),
    Projection(ProjectionPlan),
    Scan(ScanPlan),
    Select(SelectPlan),
    ReadDataSource(ReadDataSourcePlan),
}

impl PlanNode {
    pub fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> fmt::Result {
        match self {
            PlanNode::Empty(plan) => plan.describe_node(f, setting),
            PlanNode::Expression(plan) => plan.describe_node(f, setting),
            PlanNode::Filter(plan) => plan.describe_node(f, setting),
            PlanNode::Limit(plan) => plan.describe_node(f, setting),
            PlanNode::Scan(plan) => plan.describe_node(f, setting),
            PlanNode::Select(plan) => plan.describe_node(f, setting),
            PlanNode::Projection(plan) => plan.describe_node(f, setting),
            PlanNode::ReadDataSource(plan) => plan.describe_node(f, setting),
        }
    }
}

impl fmt::Debug for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let setting = &mut FormatterSettings {
            indent: 0,
            indent_char: "  ",
            prefix: "└─",
        };
        self.describe_node(f, setting)
    }
}
