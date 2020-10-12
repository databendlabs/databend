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
    pub fn build_plan(ctx: Context, query: &ast::Query) -> Result<PlanNode> {
        let mut builder = PlanBuilder::default();

        match &query.body {
            ast::SetExpr::Select(sel) => {
                let project = ProjectionPlan::build_plan(ctx.clone(), &sel.projection)?;
                builder.add(project);

                let limit = LimitPlan::build_plan(ctx.clone(), &query.limit)?;
                builder.add(limit);

                let filter = FilterPlan::build_plan(ctx.clone(), &sel.selection)?;
                builder.add(filter);

                let scan = ScanPlan::build_plan(ctx, &sel.from, builder.build()?)?;
                builder.add(scan);
            }
            _ => {
                return Err(Error::Unsupported(format!(
                    "Unsupported SelectPlan query: {}",
                    query
                )))
            }
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
