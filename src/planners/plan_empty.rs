// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;

use crate::planners::{FormatterSettings, IPlanNode};

pub struct EmptyPlan {}

impl IPlanNode for EmptyPlan {
    fn name(&self) -> &'static str {
        "EmptyPlan"
    }

    fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        _setting: &mut FormatterSettings,
    ) -> fmt::Result {
        write!(f, "")
    }
}

impl fmt::Debug for EmptyPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}
