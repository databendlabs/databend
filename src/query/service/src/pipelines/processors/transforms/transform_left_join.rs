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

use common_exception::Result;
use common_expression::DataBlock;

use crate::pipelines::processors::transforms::Compactor;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::processors::TransformCompact;

pub struct LeftJoinCompactor {
    hash_join_state: Arc<dyn HashJoinState>,
}

impl LeftJoinCompactor {
    pub fn create(hash_join_state: Arc<dyn HashJoinState>) -> Self {
        LeftJoinCompactor { hash_join_state }
    }
}

impl Compactor for LeftJoinCompactor {
    fn name() -> &'static str {
        "LeftJoinCompactor"
    }

    fn interrupt(&self) {
        self.hash_join_state.interrupt();
    }

    fn use_partial_compact(&self) -> bool {
        true
    }

    fn compact_partial(&mut self, blocks: &mut Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let res = self.hash_join_state.left_join_blocks(blocks)?;
        // Original blocks are already in res, so clear them.
        blocks.clear();
        Ok(res)
    }

    // `compact_final` is called when all the blocks are pushed
    fn compact_final(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        // If upstream data block is small, all blocks will be in rest blocks
        // So compact partial won't be triggered, we still need to compact final here.
        self.hash_join_state.left_join_blocks(blocks)
    }
}

pub type TransformLeftJoin = TransformCompact<LeftJoinCompactor>;
