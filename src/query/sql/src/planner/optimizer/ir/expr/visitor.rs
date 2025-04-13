// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::ir::expr::SExpr;
// use crate::plans::RelOperator;

/// Action to take after visiting a node
#[derive(Clone)]
pub enum VisitAction {
    /// Continue traversing the children
    Continue,
    /// Skip the children of the current node
    SkipChildren,
    /// Stop the entire traversal
    Stop,
    /// Replace the current node with a new expression
    Replace(SExpr),
}

// Synchronous Visitor Implementation
//

/// Visitor for SExpr nodes (synchronous version)
pub trait SExprVisitor {
    /// Visit an expression node
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction>;

    /// Post-visit an expression node after its children have been visited
    fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
        Ok(VisitAction::Continue)
    }
}

/// Traverse an expression tree using a synchronous visitor
#[recursive::recursive]
pub fn visit_sexpr<V: SExprVisitor>(visitor: &mut V, expr: &SExpr) -> Result<Option<SExpr>> {
    // Pre-order visit
    match visitor.visit(expr)? {
        VisitAction::Continue => {}
        VisitAction::SkipChildren => {
            return visitor.post_visit(expr).map(|action| match action {
                VisitAction::Replace(new_expr) => Some(new_expr),
                _ => None,
            })
        }
        VisitAction::Stop => return Ok(None),
        VisitAction::Replace(new_expr) => return Ok(Some(new_expr)),
    }

    // Visit children
    let mut children = Vec::with_capacity(expr.arity());
    let mut children_changed = false;

    for i in 0..expr.arity() {
        let child = expr.child(i)?;
        if let Some(new_child) = visit_sexpr(visitor, child)? {
            children.push(Arc::new(new_child));
            children_changed = true;
        } else {
            children.push(Arc::new(child.clone()));
        }
    }

    // Create new expression if children changed
    let current_expr = if children_changed {
        expr.replace_children(children)
    } else {
        expr.clone()
    };

    // Post-order visit
    match visitor.post_visit(&current_expr)? {
        VisitAction::Replace(new_expr) => Ok(Some(new_expr)),
        _ => {
            if children_changed {
                Ok(Some(current_expr))
            } else {
                Ok(None)
            }
        }
    }
}

// Asynchronous Visitor Implementation
//

/// Visitor for SExpr nodes (asynchronous version)
#[async_trait::async_trait]
pub trait AsyncSExprVisitor {
    /// Visit an expression node
    async fn visit(&mut self, expr: &SExpr) -> Result<VisitAction>;

    /// Post-visit an expression node after its children have been visited
    async fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
        Ok(VisitAction::Continue)
    }
}

/// Traverse an expression tree using an async visitor
#[allow(clippy::multiple_bound_locations)]
#[async_recursion::async_recursion(# [recursive::recursive])]
pub async fn visit_sexpr_async<T: AsyncSExprVisitor + Send>(
    visitor: &mut T,
    expr: &SExpr,
) -> Result<Option<SExpr>> {
    // Pre-order visit
    match visitor.visit(expr).await? {
        VisitAction::Continue => {}
        VisitAction::SkipChildren => {
            return visitor.post_visit(expr).await.map(|action| match action {
                VisitAction::Replace(new_expr) => Some(new_expr),
                _ => None,
            })
        }
        VisitAction::Stop => return Ok(None),
        VisitAction::Replace(new_expr) => return Ok(Some(new_expr)),
    }

    // Visit children
    let mut children = Vec::with_capacity(expr.arity());
    let mut children_changed = false;

    for i in 0..expr.arity() {
        let child = expr.child(i)?;
        if let Some(new_child) = visit_sexpr_async(visitor, child).await? {
            children.push(Arc::new(new_child));
            children_changed = true;
        } else {
            children.push(Arc::new(child.clone()));
        }
    }

    // Create new expression if children changed
    let current_expr = if children_changed {
        expr.replace_children(children)
    } else {
        expr.clone()
    };

    // Post-order visit
    match visitor.post_visit(&current_expr).await? {
        VisitAction::Replace(new_expr) => Ok(Some(new_expr)),
        _ => {
            if children_changed {
                Ok(Some(current_expr))
            } else {
                Ok(None)
            }
        }
    }
}

/// Extension methods for SExpr
impl SExpr {
    /// Apply a synchronous visitor to this expression
    pub fn accept<V: SExprVisitor>(&self, visitor: &mut V) -> Result<Option<SExpr>> {
        visit_sexpr(visitor, self)
    }

    /// Apply an asynchronous visitor to this expression
    pub async fn accept_async<T: AsyncSExprVisitor + Send>(
        &self,
        visitor: &mut T,
    ) -> Result<Option<SExpr>> {
        visit_sexpr_async(visitor, self).await
    }
}
