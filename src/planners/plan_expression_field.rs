// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::planners::ExpressionPlan;

pub fn field(name: &str) -> ExpressionPlan {
    ExpressionPlan::Field(name.to_string())
}
