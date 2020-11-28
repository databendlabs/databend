// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datasources::Partitions;
use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{FormatterSettings, PlanNode};

#[derive(Clone)]
pub struct ReadDataSourcePlan {
    pub table: String,
    pub table_type: &'static str,
    pub schema: DataSchemaRef,
    pub partitions: Partitions,
    pub description: String,
}

impl ReadDataSourcePlan {
    pub fn try_create(
        ctx: Arc<FuseQueryContext>,
        scan: &PlanNode,
        pushdowns: Vec<PlanNode>,
    ) -> FuseQueryResult<PlanNode> {
        match scan {
            PlanNode::Scan(v) => {
                let table = ctx.get_table(&ctx.get_current_database()?, v.table_name.as_str())?;
                Ok(PlanNode::ReadSource(table.read_plan(pushdowns)?))
            }

            _ => Err(FuseQueryError::Internal(format!(
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
            setting.prefix,
            self.partitions.len(),
            self.description
        )
    }
}
