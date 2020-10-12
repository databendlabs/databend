// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct ScanPlan {
    pub table_name: String,
}

impl ScanPlan {
    pub fn build_plan(_ctx: Context, from: &[ast::TableWithJoins]) -> Result<PlanNode> {
        if from.is_empty() {
            return Ok(PlanNode::Empty(EmptyPlan {}));
        }

        let relation = &from[0].relation;
        match relation {
            ast::TableFactor::Table { name, .. } => Ok(PlanNode::Scan(ScanPlan {
                table_name: name.to_string(),
            })),
            _ => Err(Error::Unsupported(format!(
                "Unsupported ScanPlan from: {}",
                relation
            ))),
        }
    }

    pub fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> fmt::Result {
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
