// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;

use super::*;

#[derive(Clone, PartialEq)]
pub struct EmptyPlan {}

impl EmptyPlan {
    pub fn describe_node(
        &self,
        f: &mut fmt::Formatter,
        _setting: &mut FormatterSettings,
    ) -> fmt::Result {
        write!(f, "")
    }
}
