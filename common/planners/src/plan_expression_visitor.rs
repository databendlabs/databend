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

    fn visit(mut self, predecessor_expr: &Expression) -> Result<Self> {
        let mut stack = vec![RecursionProcessing::Call(predecessor_expr)];
        while let Some(element) = stack.pop() {
            match element {
                RecursionProcessing::Ret(expr) => {
                    self = self.post_visit(expr)?;
                }
                RecursionProcessing::Call(expr) => {
                    stack.push(RecursionProcessing::Ret(expr));
                    self = match self.pre_visit(expr)? {
                        Recursion::Stop(visitor) => visitor,
                        Recursion::Continue(visitor) => {
                            match expr {
                                Expression::Alias(_, expr) => {
                                    stack.push(RecursionProcessing::Call(expr))
                                }
                                Expression::BinaryExpression { left, right, .. } => {
                                    stack.push(RecursionProcessing::Call(left));
                                    stack.push(RecursionProcessing::Call(right));
                                }
                                Expression::UnaryExpression { expr, .. } => {
                                    stack.push(RecursionProcessing::Call(expr));
                                }
                                Expression::ScalarFunction { args, .. } => {
                                    for arg in args {
                                        stack.push(RecursionProcessing::Call(arg));
                                    }
                                }
                                Expression::AggregateFunction { args, .. } => {
                                    for arg in args {
                                        stack.push(RecursionProcessing::Call(arg));
                                    }
                                }
                                Expression::Cast { expr, .. } => {
                                    stack.push(RecursionProcessing::Call(expr));
                                }
                                Expression::Sort { expr, .. } => {
                                    stack.push(RecursionProcessing::Call(expr));
                                }
                                _ => {}
                            };

                            visitor
                        }
                    }
                }
            }
        }

        Ok(self)
    }

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
    /// post_visit(Column("foo"))
    /// pre_visit(Column("bar"))
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

        let visitor = visitor.visit(self)?;
        visitor.post_visit(self)
    }
}

enum RecursionProcessing<'a> {
    Call(&'a Expression),
    Ret(&'a Expression),
}
