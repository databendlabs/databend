// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

/// Sub binary function.
pub fn sub(left: Expression, right: Expression) -> Expression {
    binary_expr(left, "-", right)
}

/// Not.
pub fn not(other: Expression) -> Expression {
    Expression::UnaryExpression {
        op: "not".to_string(),
        expr: Box::new(other),
    }
}

// Neg.
pub fn neg(other: Expression) -> Expression {
    Expression::UnaryExpression {
        op: "negate".to_string(),
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
        distinct: false,
        params: vec![],
        args: vec![other],
    }
}

/// avg() aggregate function.
pub fn avg(other: Expression) -> Expression {
    Expression::AggregateFunction {
        op: "avg".to_string(),
        distinct: false,
        params: vec![],
        args: vec![other],
    }
}

impl Expression {
    /// And.
    #[must_use]
    pub fn and(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "and", other)
    }

    #[must_use]
    pub fn or(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "or", other)
    }

    /// Equal.
    #[must_use]
    pub fn eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "=", other)
    }

    /// Not equal.
    #[must_use]
    pub fn not_eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "!=", other)
    }

    /// Greater than.
    #[must_use]
    pub fn gt(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), ">", other)
    }

    /// Greater than or equal to.
    #[must_use]
    pub fn gt_eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), ">=", other)
    }

    /// Less than.
    #[must_use]
    pub fn lt(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "<", other)
    }

    /// Less than or equal to.
    #[must_use]
    pub fn lt_eq(&self, other: Expression) -> Expression {
        binary_expr(self.clone(), "<=", other)
    }

    /// Alias.
    #[must_use]
    pub fn alias(&self, alias: &str) -> Expression {
        Expression::Alias(alias.to_string(), Box::from(self.clone()))
    }
}
