// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::ExpressionPlan;

/// return a new expression l <op> r.
fn binary_expr(l: ExpressionPlan, op: &str, r: ExpressionPlan) -> ExpressionPlan {
    ExpressionPlan::BinaryExpression {
        left: Box::new(l),
        op: op.to_string(),
        right: Box::new(r)
    }
}

/// Add binary function.
pub fn add(left: ExpressionPlan, right: ExpressionPlan) -> ExpressionPlan {
    binary_expr(left, "+", right)
}

/// Mod binary function.
pub fn modular(left: ExpressionPlan, right: ExpressionPlan) -> ExpressionPlan {
    binary_expr(left, "%", right)
}

/// sum() aggregate function.
pub fn sum(other: ExpressionPlan) -> ExpressionPlan {
    ExpressionPlan::Function {
        name: "sum".to_string(),
        args: vec![other]
    }
}

/// avg() aggregate function.
pub fn avg(other: ExpressionPlan) -> ExpressionPlan {
    ExpressionPlan::Function {
        name: "avg".to_string(),
        args: vec![other]
    }
}

impl ExpressionPlan {
    // And.
    pub fn and(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), "and", other)
    }

    /// Equal.
    pub fn eq(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), "=", other)
    }

    /// Not equal.
    pub fn not_eq(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), "!=", other)
    }

    /// Greater than.
    pub fn gt(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), ">", other)
    }

    /// Greater than or equal to.
    pub fn gt_eq(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), ">=", other)
    }

    /// Less than.
    pub fn lt(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), "<", other)
    }

    /// Less than or equal to.
    pub fn lt_eq(&self, other: ExpressionPlan) -> ExpressionPlan {
        binary_expr(self.clone(), "<=", other)
    }

    /// Alias.
    pub fn alias(&self, alias: &str) -> ExpressionPlan {
        ExpressionPlan::Alias(alias.to_string(), Box::from(self.clone()))
    }
}
