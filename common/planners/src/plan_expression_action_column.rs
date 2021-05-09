// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::ExpressionAction;

pub fn col(name: &str) -> ExpressionAction {
    ExpressionAction::Column(name.to_string())
}
