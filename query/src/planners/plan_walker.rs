// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::FuseQueryResult;
use crate::planners::PlanNode;

#[derive(PartialEq)]
enum WalkOrder {
    PreOrder,
    PostOrder,
}

impl PlanNode {
    fn walk_base(
        order: WalkOrder,
        node: &PlanNode,
        mut visitor: impl FnMut(&PlanNode) -> FuseQueryResult<bool>,
    ) -> FuseQueryResult<()> {
        let mut nodes = vec![];
        let mut tmp = node.clone();

        loop {
            if let PlanNode::Empty(_) = tmp {
                break;
            }
            nodes.push(tmp.clone());
            tmp = tmp.input().as_ref().clone();
        }

        if order == WalkOrder::PostOrder {
            nodes.reverse();
        }

        for plan in nodes {
            if !visitor(&plan)? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Preorder walk is when each node is visited before any of its inputs:
    /// A(Projection)
    /// |
    /// B(Filter)
    /// |
    /// C(ReadSource)
    /// A Preorder walk of this graph is A B C
    pub fn walk_preorder(
        &self,
        visitor: impl FnMut(&PlanNode) -> FuseQueryResult<bool>,
    ) -> FuseQueryResult<()> {
        Self::walk_base(WalkOrder::PreOrder, self, visitor)
    }

    /// Postorder walk is when each node is visited after all of its inputs:
    /// A(Projection)
    /// |
    /// B(Filter)
    /// |
    /// C(ReadSource)
    /// A Postorder walk of this graph is C B A
    pub fn walk_postorder(
        &self,
        visitor: impl FnMut(&PlanNode) -> FuseQueryResult<bool>,
    ) -> FuseQueryResult<()> {
        Self::walk_base(WalkOrder::PostOrder, self, visitor)
    }
}
