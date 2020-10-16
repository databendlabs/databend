// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::planners::FormatterSettings;

#[derive(Clone)]
pub struct EmptyPlan {}

impl EmptyPlan {
    pub fn name(&self) -> &'static str {
        "EmptyPlan"
    }

    pub fn set_description(&mut self, _description: &str) {}

    pub fn format(&self, f: &mut fmt::Formatter, _setting: &mut FormatterSettings) -> fmt::Result {
        write!(f, "")
    }
}
