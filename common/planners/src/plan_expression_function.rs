// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::Expression;

/// return a new expression l <op> r.
fn binary_expr(l: Expression, op: &str, r: Expression) -> Expression {
    Expression::BinaryExpression {
        op: op.to_string(),
        left: Box::new(l),
        right: Box::new(r),
    }
}

/// Add binary function.
pub fn add(left: Expression, right: Expression) -> Expression {
    binary_expr(left, "+", right)
}

/// Not.
pub fn not(other: Expression) -> Expression {
    Expression::UnaryExpression {
        op: "not".to_string(),
        expr: Box::new(other),
    }
}

/// Mod binary function.
pub fn modular(left: Expression, right: Expression) -> Expression {
    binary_expr(left, "%", right)
}

/// sum() aggregate function.
pub fn sum(other: Expression) -> Expression {
    Expression::AggregateFunction {
        op: "sum".to_string(),
        args: vec![other],
    }
}

/// avg() aggregate function.
pub fn avg(other: Expression) -> Expression {
    Expression::AggregateFunction {
        op: "avg".to_string(),
        args: vec![other],
    }
}

impl Expression {
    /// And.
    pub fn and(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "and", other)
    }

    /// Equal.
    pub fn eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "=", other)
    }

    /// Not equal.
    pub fn not_eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "!=", other)
    }

    /// Greater than.
    pub fn gt(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), ">", other)
    }

    /// Greater than or equal to.
    pub fn gt_eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), ">=", other)
    }

    /// Less than.
    pub fn lt(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "<", other)
    }

    /// Less than or equal to.
    pub fn lt_eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "<=", other)
    }

    /// Alias.
    pub fn alias(&self, alias: &str) -> Expression {
        Expression::Alias(alias.to_string(), Box::from(self.clone()))
    }
}
