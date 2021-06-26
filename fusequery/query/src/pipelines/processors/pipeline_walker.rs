// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::result::Result;

use crate::pipelines::processors::Pipe;
use crate::pipelines::processors::Pipeline;

#[derive(PartialEq)]
enum WalkOrder {
    PreOrder,
    PostOrder,
}

impl Pipeline {
    fn walk_base<E>(
        order: WalkOrder,
        pipeline: &Pipeline,
        mut visitor: impl FnMut(&Pipe) -> Result<bool, E>,
    ) -> Result<(), E> {
        let mut pipes = vec![];

        for pipe in &pipeline.pipes() {
            pipes.push(pipe.clone());
        }

        if order == WalkOrder::PreOrder {
            pipes.reverse();
        }

        for pipe in pipes {
            if !visitor(&pipe)? {
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
    pub fn walk_preorder<E>(&self, visitor: impl FnMut(&Pipe) -> Result<bool, E>) -> Result<(), E> {
        Self::walk_base(WalkOrder::PreOrder, self, visitor)
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
        visitor: impl FnMut(&Pipe) -> Result<bool, E>,
    ) -> Result<(), E> {
        Self::walk_base(WalkOrder::PostOrder, self, visitor)
    }
}
