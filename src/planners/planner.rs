// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{
    DFExplainPlan, DFParser, DFStatement, EmptyPlan, ExplainPlan, ExpressionPlan, FilterPlan,
    LimitPlan, ProjectionPlan, ReadDataSourcePlan, ScanPlan, SelectPlan,
};

pub struct Planner;

impl Planner {
    /// Creates a new planner.
    pub fn new() -> Self {
        Self {}
    }

    pub fn build_from_sql(
        &self,
        ctx: Arc<FuseQueryContext>,
        query: &str,
    ) -> FuseQueryResult<PlanNode> {
        let statements = DFParser::parse_sql(query)?;
        if statements.len() != 1 {
            return Err(FuseQueryError::Unsupported(
                "Only support single query".to_string(),
            ));
        }
        self.build(ctx, &statements[0])
    }

    /// Builds plan from AST statement.
    pub fn build(
        &self,
        ctx: Arc<FuseQueryContext>,
        statement: &DFStatement,
    ) -> FuseQueryResult<PlanNode> {
        match statement {
            DFStatement::Statement(s) => self.sql_statement_to_plan(ctx, &s),
            DFStatement::Explain(s) => self.explain_statement_to_plan(ctx, &s),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported statement: {:?} in planner.build()",
                statement
            ))),
        }
    }

    pub fn sql_statement_to_plan(
        &self,
        ctx: Arc<FuseQueryContext>,
        sql: &ast::Statement,
    ) -> FuseQueryResult<PlanNode> {
        match sql {
            ast::Statement::Query(query) => SelectPlan::build_plan(ctx, query),
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported statement {:?} for planner.statement_to_plan",
                sql
            ))),
        }
    }

    pub fn explain_statement_to_plan(
        &self,
        ctx: Arc<FuseQueryContext>,
        explain_plan: &DFExplainPlan,
    ) -> FuseQueryResult<PlanNode> {
        let plan = self.build(ctx.clone(), &explain_plan.statement)?;
        ExplainPlan::build_plan(ctx, plan)
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

#[derive(Clone)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Expression(ExpressionPlan),
    Filter(FilterPlan),
    Limit(LimitPlan),
    Projection(ProjectionPlan),
    ReadSource(ReadDataSourcePlan),
    Scan(ScanPlan),
    Select(SelectPlan),
    Explain(Box<ExplainPlan>),
}

impl PlanNode {
    pub fn format(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        match self {
            PlanNode::Empty(_) => write!(f, ""),
            PlanNode::Expression(v) => v.format(f, setting),
            PlanNode::Filter(v) => v.format(f, setting),
            PlanNode::Limit(v) => v.format(f, setting),
            PlanNode::Projection(v) => v.format(f, setting),
            PlanNode::ReadSource(v) => v.format(f, setting),
            PlanNode::Scan(v) => v.format(f, setting),
            PlanNode::Select(v) => v.format(f, setting),
            PlanNode::Explain(v) => v.format(f, setting),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PlanNode::Empty(v) => v.name(),
            PlanNode::Expression(v) => v.name(),
            PlanNode::Filter(v) => v.name(),
            PlanNode::Limit(v) => v.name(),
            PlanNode::Projection(v) => v.name(),
            PlanNode::ReadSource(v) => v.name(),
            PlanNode::Scan(v) => v.name(),
            PlanNode::Select(v) => v.name(),
            PlanNode::Explain(v) => v.name(),
        }
    }

    pub fn set_description(&mut self, desc: &str) {
        match self {
            PlanNode::Empty(ref mut v) => v.set_description(desc),
            PlanNode::Filter(ref mut v) => v.set_description(desc),
            PlanNode::Limit(ref mut v) => v.set_description(desc),
            PlanNode::ReadSource(ref mut v) => v.set_description(desc),
            PlanNode::Scan(ref mut v) => v.set_description(desc),
            _ => unimplemented!("{}", &self.name()),
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
        self.format(f, setting)
    }
}
