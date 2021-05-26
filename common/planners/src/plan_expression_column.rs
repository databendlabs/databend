// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::Expression;

pub fn col(name: &str) -> Expression {
    Expression::Column(name.to_string())
}
