// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::{Error, Result};

use crate::planners::{FormatterSettings, IPlanNode};

#[derive(Clone)]
pub struct ScanPlan {
    pub table_name: String,
}

impl ScanPlan {
    pub fn build_plan(_ctx: Context, from: &[ast::TableWithJoins]) -> Result<Arc<ScanPlan>> {
        if from.is_empty() {
            return Ok(Arc::new(ScanPlan {
                table_name: "".to_string(),
            }));
        }

        let relation = &from[0].relation;
        match relation {
            ast::TableFactor::Table { name, .. } => Ok(Arc::new(ScanPlan {
                table_name: name.to_string(),
            })),
            _ => Err(Error::Unsupported(format!(
                "Unsupported ScanPlan from: {}",
                relation
            ))),
        }
    }
}

impl IPlanNode for ScanPlan {
    fn name(&self) -> &'static str {
        "ScanPlan"
    }

    fn describe(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
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

impl fmt::Debug for ScanPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.table_name)
    }
}
