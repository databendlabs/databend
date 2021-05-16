// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::result::Result;

use crate::PlanNode;
use crate::PlanVisitor;

#[derive(PartialEq)]
enum WalkOrder {
    PreOrder,
    PostOrder
}

struct PreOrderWalker<'a, E> {
    callback: &'a mut dyn FnMut(&PlanNode) -> Result<bool, E>,
    state: Result<(), E>
}

impl<'plan, 'a, E> PlanVisitor<'plan> for PreOrderWalker<'a, E> {
    fn visit_plan_node(&mut self, node: &PlanNode) {
        if let PlanNode::Empty(_) = node {
            return;
        }
        match (self.callback)(node) {
            Ok(true) => {
                println!("Preorder: visiting {}", node.name());
                self.visit_plan_node(node.input().as_ref());
            }
            Ok(false) => {
                return;
            }
            Err(e) => {
                self.state = Result::Err(e);
            }
        }
    }
}

impl<'a, E> PreOrderWalker<'a, E> {
    fn new(callback: &'a mut dyn FnMut(&PlanNode) -> Result<bool, E>) -> PreOrderWalker<E> {
        PreOrderWalker {
            callback: callback,
            state: Ok(())
        }
    }

    fn finalize(self) -> Result<(), E> {
        self.state
    }
}

struct PostOrderWalker<'a, E> {
    callback: &'a mut dyn FnMut(&PlanNode) -> Result<bool, E>,
    state: Result<bool, E>
}

impl<'plan, 'a, E> PlanVisitor<'plan> for PostOrderWalker<'a, E> {
    fn visit_plan_node(&mut self, node: &PlanNode) {
        if let PlanNode::Empty(_) = node {
            return;
        }
        self.visit_plan_node(node.input().as_ref());
        match self.state {
            Ok(true) => {
                self.state = (self.callback)(node);
            }
            _ => {
                return;
            }
        }
    }
}

impl<'a, E> PostOrderWalker<'a, E> {
    fn new(callback: &'a mut dyn FnMut(&PlanNode) -> Result<bool, E>) -> PostOrderWalker<E> {
        PostOrderWalker {
            callback: callback,
            state: Ok(true)
        }
    }

    fn finalize(self) -> Result<(), E> {
        self.state.map(|_| ())
    }
}

impl PlanNode {
    fn walk_base<'a, E>(
        order: WalkOrder,
        node: &PlanNode,
        callback: &'a mut dyn FnMut(&PlanNode) -> Result<bool, E>
    ) -> Result<(), E> {
        match order {
            WalkOrder::PreOrder => {
                let mut visitor = PreOrderWalker::<'a, E>::new(callback);
                visitor.visit_plan_node(node);
                visitor.finalize()
            }
            WalkOrder::PostOrder => {
                let mut visitor = PostOrderWalker::<'a, E>::new(callback);
                visitor.visit_plan_node(node);
                visitor.finalize()
            }
        }
    }

    /// Preorder walk is when each node is visited before any of its inputs:
    /// A(Projection)
    /// |
    /// B(Filter)
    /// |
    /// C(ReadSource)
    /// A Preorder walk of this graph is A B C
    pub fn walk_preorder<E>(
        &self,
        mut visitor: impl FnMut(&PlanNode) -> Result<bool, E>
    ) -> Result<(), E> {
        Self::walk_base(WalkOrder::PreOrder, self, &mut visitor)
    }

    /// Postorder walk is when each node is visited after all of its inputs:
    /// A(Projection)
    /// |
    /// B(Filter)
    /// |
    /// C(ReadSource)
    /// A Postorder walk of this graph is C B A
    pub fn walk_postorder<E>(
        &self,
        mut visitor: impl FnMut(&PlanNode) -> Result<bool, E>
    ) -> Result<(), E> {
        Self::walk_base(WalkOrder::PostOrder, self, &mut visitor)
    }
}
