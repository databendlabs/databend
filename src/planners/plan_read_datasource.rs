// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::sync::Arc;

use crate::contexts::Context;
use crate::error::Result;

use crate::planners::{EmptyPlan, FormatterSettings, IPlanNode};

pub struct ReadDataSourcePlan {
    pub(crate) read_parts: usize,
}

impl ReadDataSourcePlan {
    pub fn build_plan(_ctx: Context) -> Result<Arc<dyn IPlanNode>> {
        Ok(Arc::new(EmptyPlan {}))
    }
}

impl IPlanNode for ReadDataSourcePlan {
    fn name(&self) -> &'static str {
        "ReadDataSourcePlan"
    }

    fn describe_node(
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
        write!(f, "{} ReadDataSource: {}", setting.prefix, self.read_parts)
    }
}

impl fmt::Debug for ReadDataSourcePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {:?}", self.name(), self.read_parts)
    }
}
