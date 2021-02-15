// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::planners::ExpressionPlan;

pub fn field(name: &str) -> ExpressionPlan {
    ExpressionPlan::Field(name.to_string())
}
