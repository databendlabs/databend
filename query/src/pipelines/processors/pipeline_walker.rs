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
