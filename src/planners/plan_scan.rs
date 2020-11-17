// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use sqlparser::ast;
use std::fmt;

use crate::contexts::Context;
use crate::error::{Error, Result};
use crate::planners::{FormatterSettings, PlanNode};

#[derive(Clone)]
pub struct ScanPlan {
    description: String,
    pub table_name: String,
}

impl ScanPlan {
    pub fn build_plan(_ctx: Context, from: &[ast::TableWithJoins]) -> Result<PlanNode> {
        if from.is_empty() {
            return Ok(PlanNode::Scan(ScanPlan {
                description: "".to_string(),
                table_name: "".to_string(),
            }));
        }

        let relation = &from[0].relation;
        match relation {
            ast::TableFactor::Table { name, .. } => Ok(PlanNode::Scan(ScanPlan {
                description: "".to_string(),
                table_name: name.to_string(),
            })),
            _ => Err(Error::Unsupported(format!(
                "Unsupported ScanPlan from: {}",
                relation
            ))),
        }
    }
    pub fn name(&self) -> &'static str {
        "ScanPlan"
    }

    pub fn set_description(&mut self, description: &str) {
        self.description = format!(" ({})", description);
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
        write!(f, "{} Scan: {}", setting.prefix, self.table_name)
    }
}
