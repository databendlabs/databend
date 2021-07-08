// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::Expression;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct InListExpr {
    expr: Box<Expression>,
    list: Vec<Expression>,
    negated: bool,
}

impl InListExpr {
    /// Create a new InList expression
    pub fn new(expr: Box<Expression>, list: Vec<Expression>, negated: bool) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Expression {
        &self.expr
    }

    /// List to search in
    pub fn list(&self) -> &[Expression] {
        &self.list
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }
}
