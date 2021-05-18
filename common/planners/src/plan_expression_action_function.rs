// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::ExpressionAction;

/// return a new expression l <op> r.
fn binary_expr(l: ExpressionAction, op: &str, r: ExpressionAction) -> ExpressionAction {
    ExpressionAction::ScalarFunction {
        op: op.to_string(),
        args: vec![l, r],
    }
}

/// Add binary function.
pub fn add(left: ExpressionAction, right: ExpressionAction) -> ExpressionAction {
    binary_expr(left, "+", right)
}

/// Mod binary function.
pub fn modular(left: ExpressionAction, right: ExpressionAction) -> ExpressionAction {
    binary_expr(left, "%", right)
}

/// sum() aggregate function.
pub fn sum(other: ExpressionAction) -> ExpressionAction {
    ExpressionAction::AggregateFunction {
        op: "sum".to_string(),
        args: vec![other]
    }
}

/// avg() aggregate function.
pub fn avg(other: ExpressionAction) -> ExpressionAction {
    ExpressionAction::AggregateFunction {
        op: "avg".to_string(),
        args: vec![other]
    }
}

impl ExpressionAction {
    // And.
    pub fn and(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), "and", other)
    }

    /// Equal.
    pub fn eq(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), "=", other)
    }

    /// Not equal.
    pub fn not_eq(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), "!=", other)
    }

    /// Greater than.
    pub fn gt(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), ">", other)
    }

    /// Greater than or equal to.
    pub fn gt_eq(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), ">=", other)
    }

    /// Less than.
    pub fn lt(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), "<", other)
    }

    /// Less than or equal to.
    pub fn lt_eq(&self, other: ExpressionAction) -> ExpressionAction {
        binary_expr(self.clone(), "<=", other)
    }

    /// Alias.
    pub fn alias(&self, alias: &str) -> ExpressionAction {
        ExpressionAction::Alias(alias.to_string(), Box::from(self.clone()))
    }
}
