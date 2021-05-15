// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::ExpressionAction;
use common_exception::{Result, ErrorCodes};
use common_functions::FunctionFactory;


/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: ExpressionVisitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Encode the traversal of an expression tree. When passed to
/// `Expr::accept`, `ExprVisitor::visit` is invoked
/// recursively on all nodes of an expression tree. See the comments
/// on `Expr::accept` for details on its use
pub trait ExprVisitor: Sized {
    /// Invoked before any children of `expr` are visisted.
    fn pre_visit(self, expr: &ExpressionAction) -> Result<Recursion<Self>>;

    /// Invoked after all children of `expr` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _expr: &ExpressionAction) -> Result<Self> {
        Ok(self)
    }
}

/// Trait for potentially recursively rewriting an [`Expr`] expression
/// tree. When passed to `Expr::rewrite`, `ExprVisitor::mutate` is
/// invoked recursively on all nodes of an expression tree. See the
/// comments on `Expr::rewrite` for details on its use
pub trait ExprRewriter: Sized {
    /// Invoked before any children of `expr` are rewritten /
    /// visited. Default implementation returns `Ok(true)`
    fn pre_visit(&mut self, _expr: &ExpressionAction) -> Result<bool> {
        Ok(true)
    }

    /// Invoked after all children of `expr` have been mutated and
    /// returns a potentially modified expr.
    fn mutate(&mut self, expr: ExpressionAction) -> Result<ExpressionAction>;
}

impl ExpressionAction {
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
            ExpressionAction::Alias(_, expr) => expr.accept(visitor),
            ExpressionAction::ScalarFunction { op, args } => {
                let mut visitor = visitor;
                for arg in args {
                    visitor = arg.accept(visitor)?;
                }
                visitor
            }
            ExpressionAction::AggregateFunction { op, args } => {
                let mut visitor = visitor;
                for arg in args {
                    visitor = arg.accept(visitor)?;
                }
                visitor
            }
            ExpressionAction::Cast { expr, data_type } => expr.accept(visitor),
            ExpressionAction::Sort { expr, .. } => expr.accept(visitor),

            _ => Ok(visitor)
        }?;

        visitor.post_visit(self)
    }

    /// Performs a depth first walk of an expression and its children
    /// to rewrite an expression, consuming `self` producing a new
    /// [`Expr`].
    ///
    /// Implements a modified version of the [visitor
    /// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
    /// separate algorithms from the structure of the `Expr` tree and
    /// make it easier to write new, efficient expression
    /// transformation algorithms.
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
    /// pre_visit(BinaryExpr(GT))
    /// pre_visit(Column("foo"))
    /// mutatate(Column("foo"))
    /// pre_visit(Column("bar"))
    /// mutate(Column("bar"))
    /// mutate(BinaryExpr(GT))
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`false`] is returned on a call to pre_visit, no
    /// children of that expression are visited, nor is mutate
    /// called on that expression
    ///
    pub fn rewrite<R>(self, rewriter: &mut R) -> Result<Self>
        where
            R: ExprRewriter,
    {
        if !rewriter.pre_visit(&self)? {
            return Ok(self);
        };
        // recurse into all sub expressions(and cover all expression types)
        let expr = match self {
            ExpressionAction::Alias(name, expr) => {
                let expr = expr.rewrite(rewriter)?;
                ExpressionAction::Alias(name, Box::new(expr))?
            },
            ExpressionAction::ScalarFunction { op, args } => {
                let mut visitor = visitor;
                for arg in args {
                    visitor = arg.accept(visitor)?;
                }
                visitor
            }
            ExpressionAction::AggregateFunction { op, args } => {
                let mut visitor = visitor;
                for arg in args {
                    visitor = arg.accept(visitor)?;
                }
                visitor
            }
            ExpressionAction::Cast { expr, data_type } => expr.accept(visitor),
            ExpressionAction::Sort { expr, .. } => expr.accept(visitor),

            _ => Ok(self)
        };

        // now rewrite this expression itself
        rewriter.mutate(expr)
    }
}