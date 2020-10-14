// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::Result;

use crate::planners::{FormatterSettings, IPlanNode, ScanPlan};

pub struct ReadDataSourcePlan {
    pub(crate) table_type: &'static str,
    pub(crate) read_parts: usize,
}

impl ReadDataSourcePlan {
    pub fn build_plan(
        ctx: Context,
        scan: Arc<ScanPlan>,
        pushdowns: Vec<Arc<dyn IPlanNode>>,
    ) -> Result<Arc<dyn IPlanNode>> {
        let table = ctx.table("", scan.table_name.as_str())?;
        Ok(Arc::new(table.read_plan(pushdowns)?))
    }
}

impl IPlanNode for ReadDataSourcePlan {
    fn name(&self) -> &'static str {
        "ReadDataSourcePlan"
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
        write!(
            f,
            "{} ReadDataSource: table type [{}], scan parts [{}]",
            setting.prefix, self.table_type, self.read_parts
        )
    }
}

impl fmt::Debug for ReadDataSourcePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.read_parts)
    }
}
