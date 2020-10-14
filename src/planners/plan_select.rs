// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::{Error, Result};

use crate::planners::{
    FilterPlan, FormatterSettings, IPlanNode, LimitPlan, PlanBuilder, ProjectionPlan,
    ReadDataSourcePlan, ScanPlan,
};

#[derive(Clone)]
pub struct SelectPlan {
    pub nodes: Vec<Arc<dyn IPlanNode>>,
}

impl SelectPlan {
    pub fn build_plan(ctx: Context, query: &ast::Query) -> Result<Arc<dyn IPlanNode>> {
        let mut builder = PlanBuilder::default();

        match &query.body {
            ast::SetExpr::Select(sel) => {
                let project = ProjectionPlan::build_plan(ctx.clone(), &sel.projection)?;
                builder.add(project);

                let limit = LimitPlan::build_plan(ctx.clone(), &query.limit)?;
                builder.add(limit);

                let filter = FilterPlan::build_plan(ctx.clone(), &sel.selection)?;
                builder.add(filter);

                let scan = ScanPlan::build_plan(ctx.clone(), &sel.from)?;
                builder.add(scan.clone());

                let read_from_source = ReadDataSourcePlan::build_plan(ctx, scan, builder.build()?)?;
                builder.add(read_from_source);
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
        Ok(Arc::new(select))
    }
}

impl IPlanNode for SelectPlan {
    fn name(&self) -> &'static str {
        "SelectPlan"
    }

    fn describe(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        for node in self.nodes.iter() {
            node.describe(f, setting)?;
            setting.indent += 1;
        }
        write!(f, "")
    }
}

impl fmt::Debug for SelectPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let setting = &mut FormatterSettings {
            indent: 0,
            indent_char: "  ",
            prefix: "└─",
        };
        self.describe(f, setting)
    }
}
