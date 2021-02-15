// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::planners::ExpressionPlan;

/// return a new expression l <op> r.
fn binary_expr(l: ExpressionPlan, op: &str, r: ExpressionPlan) -> ExpressionPlan {
    ExpressionPlan::BinaryExpression {
        left: Box::new(l),
        op: op.to_string(),
        right: Box::new(r),
    }
}

// Add.
pub fn add(left: ExpressionPlan, right: ExpressionPlan) -> ExpressionPlan {
    binary_expr(left, "+", right)
}

/// sum() aggregate function.
pub fn sum(other: ExpressionPlan) -> ExpressionPlan {
    ExpressionPlan::Function {
        op: "sum".to_string(),
        args: vec![other],
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
