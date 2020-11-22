// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{
    FilterPlan, FormatterSettings, LimitPlan, PlanBuilder, PlanNode, ProjectionPlan,
    ReadDataSourcePlan, ScanPlan,
};

#[derive(Clone)]
pub struct SelectPlan {
    pub nodes: Vec<PlanNode>,
}

impl SelectPlan {
    pub fn build_plan(ctx: Arc<FuseQueryContext>, query: &ast::Query) -> FuseQueryResult<PlanNode> {
        let mut builder = PlanBuilder::default();

        match &query.body {
            ast::SetExpr::Select(sel) => {
                let project = ProjectionPlan::build_plan(ctx.clone(), &sel.projection)?;
                builder.add(project);

                let mut limit = LimitPlan::build_plan(ctx.clone(), &query.limit)?;
                limit.set_description("preliminary LIMIT");
                builder.add(limit);

                let mut filter = FilterPlan::build_plan(ctx.clone(), &sel.selection)?;
                filter.set_description("WHERE");
                builder.add(filter);

                let scan = ScanPlan::build_plan(ctx.clone(), &sel.from)?;
                let scan_ref = &scan;
                builder.add(scan.clone());

                let read_from_source =
                    ReadDataSourcePlan::build_plan(ctx, scan_ref, builder.build()?)?;
                builder.add(read_from_source);

                Ok(PlanNode::Select(SelectPlan {
                    nodes: builder.build()?,
                }))
            }
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported SelectPlan query: {}",
                query
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        "SelectPlan"
    }

    pub fn format(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        for node in self.nodes.iter() {
            node.format(f, setting)?;
            setting.indent += 1;
        }
        write!(f, "")
    }
}
