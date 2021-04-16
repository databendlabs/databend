// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::col;
use crate::ExpressionPlan;

pub fn sort(name: &str, asc: bool, nulls_first: bool) -> ExpressionPlan {
    ExpressionPlan::Sort {
        expr: Box::new(col(name)),
        asc,
        nulls_first,
    }
}
