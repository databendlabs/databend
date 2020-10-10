// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct SelectPlan {
    pub nodes: Vec<PlanNode>,
}

impl SelectPlan {
    pub fn build_plan(query: &ast::Query) -> Result<PlanNode> {
        let mut builder = PlanBuilder::default();

        match &query.body {
            ast::SetExpr::Select(sel) => {
                builder
                    .add(ProjectionPlan::build_plan(&sel.projection)?)
                    .add(LimitPlan::build_plan(&query.limit)?)
                    .add(FilterPlan::build_plan(&sel.selection)?)
                    .add(ScanPlan::build_plan(&sel.from)?);
            }
            _ => return Err(Error::Unsupported(format!("SelectPlan query: {}", query))),
        };

        let select = SelectPlan {
            nodes: builder.build()?,
        };
        Ok(PlanNode::Select(select))
    }

    pub fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> fmt::Result {
        for node in self.nodes.iter() {
            node.describe_node(f, setting)?;
            setting.indent += 1;
        }
        write!(f, "")
    }
}
