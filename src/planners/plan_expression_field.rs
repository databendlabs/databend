// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::planners::ExpressionPlan;

pub fn field(name: &str) -> ExpressionPlan {
    ExpressionPlan::Field(name.to_string())
}
