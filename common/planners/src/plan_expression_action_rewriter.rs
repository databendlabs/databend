// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::FunctionFactory;

use crate::ExpressionAction;

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
        where R: ExprRewriter {
        if !rewriter.pre_visit(&self)? {
            return Ok(self);
        };
        // recurse into all sub expressions(and cover all expression types)
        let expr = match self {
            ExpressionAction::Alias(name, expr) => {
                let expr = expr.rewrite(rewriter)?;
                ExpressionAction::Alias(name, Box::new(expr))
            }
            ExpressionAction::BinaryExpression { op, left, right } => {
                ExpressionAction::BinaryExpression { op, left: Box::new(left.rewrite(rewriter)?), right: Box::new(right.rewrite(rewriter)?) }
            }
            ExpressionAction::ScalarFunction { op, args } => {
                let mut new_args = Vec::with_capacity(args.len());
                for arg in args {
                    new_args.push(arg.rewrite(rewriter)?);
                }
                ExpressionAction::ScalarFunction { op, args: new_args }
            }
            ExpressionAction::AggregateFunction { op, args } => {
                let mut new_args = Vec::with_capacity(args.len());
                for arg in args {
                    new_args.push(arg.rewrite(rewriter)?);
                }
                ExpressionAction::AggregateFunction { op, args: new_args }
            }
            ExpressionAction::Cast { expr, data_type } => {
                let expr = expr.rewrite(rewriter)?;
                ExpressionAction::Cast {
                    expr: Box::new(expr),
                    data_type,
                }
            }
            ExpressionAction::Sort {
                expr,
                asc,
                nulls_first
            } => {
                let expr = expr.rewrite(rewriter)?;
                ExpressionAction::Sort {
                    expr: Box::new(expr),
                    asc,
                    nulls_first,
                }
            }
            _ => self
        };

        // now rewrite this expression itself
        rewriter.mutate(expr)
    }
}
