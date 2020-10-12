// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct ScanPlan {
    pub table_name: String,
    pub read_from_source: ReadDataSourcePlan,
}

impl ScanPlan {
    pub fn build_plan(
        ctx: Context,
        from: &[ast::TableWithJoins],
        nodes: Vec<PlanNode>,
    ) -> Result<PlanNode> {
        if from.is_empty() {
            return Ok(PlanNode::Empty(EmptyPlan {}));
        }

        let relation = &from[0].relation;
        match relation {
            ast::TableFactor::Table { name, .. } => {
                let table = ctx.table("", name.to_string().as_str())?;
                Ok(PlanNode::Scan(ScanPlan {
                    table_name: name.to_string(),
                    read_from_source: table.read_plan(nodes)?,
                }))
            }
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
