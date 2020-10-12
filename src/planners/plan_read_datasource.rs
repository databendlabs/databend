// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct ReadDataSourcePlan {
    pub(crate) read_parts: usize,
}

impl ReadDataSourcePlan {
    pub fn build_plan() -> Result<PlanNode> {
        Ok(PlanNode::Empty(EmptyPlan {}))
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
        write!(f, "{} ReadDataSource: {}", setting.prefix, self.read_parts)
    }
}
