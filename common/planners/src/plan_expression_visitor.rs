// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::Expression;

/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: ExpressionVisitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Encode the traversal of an expression tree. When passed to
/// `Expr::accept`, `ExpressionVisitor::visit` is invoked
/// recursively on all nodes of an expression tree. See the comments
/// on `Expr::accept` for details on its use
pub trait ExpressionVisitor: Sized {
    /// Invoked before any children of `expr` are visisted.
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>>;

    /// Invoked after all children of `expr` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _expr: &Expression) -> Result<Self> {
        Ok(self)
    }
}

impl Expression {
    /// Performs a depth first walk of an expression and
    /// its children, calling [`ExpressionVisitor::pre_visit`] and
    /// `visitor.post_visit`.
    ///
    /// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
    /// separate expression algorithms from the structure of the
    /// `Expr` tree and make it easier to add new types of expressions
    /// and algorithms that walk the tree.
    ///
    /// For an expression tree such as
    /// ```text
    /// BinaryExpr (GT)
    ///    left: Column("foo")
    ///    right: Column("bar")
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(ScalarFunction(GT))
    /// pre_visit(Column("foo"))
    /// pre_visit(Column("bar"))
    /// post_visit(Column("bar"))
    /// post_visit(Column("bar"))
    /// post_visit(ScalarFunction(GT))
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    pub fn accept<V: ExpressionVisitor>(&self, visitor: V) -> Result<V> {
        let visitor = match visitor.pre_visit(self)? {
            Recursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            Recursion::Stop(visitor) => return Ok(visitor),
        };

        // recurse (and cover all expression types)
        let visitor = match self {
            Expression::Alias(_, expr) => expr.accept(visitor),
            Expression::BinaryExpression { left, right, .. } => {
                let mut visitor = visitor;
                visitor = left.accept(visitor)?;
                visitor = right.accept(visitor)?;
                Ok(visitor)
            }
            Expression::UnaryExpression { expr, .. } => {
                let mut visitor = visitor;
                visitor = expr.accept(visitor)?;
                Ok(visitor)
            }
            Expression::ScalarFunction { args, .. } => {
                let mut visitor = visitor;
                for arg in args {
                    visitor = arg.accept(visitor)?;
                }
                Ok(visitor)
            }
            Expression::AggregateFunction { args, .. } => {
                let mut visitor = visitor;
                for arg in args {
                    visitor = arg.accept(visitor)?;
                }
                Ok(visitor)
            }
            Expression::Cast { expr, .. } => expr.accept(visitor),
            Expression::Sort { expr, .. } => expr.accept(visitor),
            Expression::Column(_)
            | Expression::Literal(_)
            | Expression::Exists(_)
            | Expression::InList { .. }
            | Expression::Wildcard => Ok(visitor),
        }?;

        visitor.post_visit(self)
    }
}
