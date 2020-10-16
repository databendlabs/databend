// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::contexts::Context;
use crate::error::{Error, Result};

use crate::planners::{FormatterSettings, PlanNode};

#[derive(Clone)]
pub struct ReadDataSourcePlan {
    pub(crate) description: String,
    pub(crate) table_type: &'static str,
    pub(crate) read_parts: usize,
}

impl ReadDataSourcePlan {
    pub fn build_plan(ctx: Context, scan: &PlanNode, pushdowns: Vec<PlanNode>) -> Result<PlanNode> {
        match scan {
            PlanNode::Scan(v) => {
                let table = ctx.table("", v.table_name.as_str())?;
                Ok(PlanNode::ReadSource(table.read_plan(pushdowns)?))
            }

            _ => Err(Error::Unsupported(format!(
                "Expected ScanPlan, but got: {:?}",
                scan
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        "ReadDataSourcePlan"
    }

    pub fn set_description(&mut self, description: &str) {
        self.description = format!("({})", description);
    }

    pub fn format(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        let indent = setting.indent;
        let prefix = setting.indent_char;

        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "{}", prefix)?;
            }
        }
        write!(
            f,
            "{} ReadDataSource: scan parts [{}] {}",
            setting.prefix, self.read_parts, self.description
        )
    }
}
