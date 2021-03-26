// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::ExpressionPlan;

pub fn col(name: &str) -> ExpressionPlan {
    ExpressionPlan::Column(name.to_string())
}
