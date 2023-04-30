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

const JOIN_MAX_BLOCK_SIZE: usize = 65535;

pub struct RightJoinCompactor {
    use_partial_compact: bool,
    hash_join_state: Arc<dyn HashJoinState>,
}

impl RightJoinCompactor {
    pub fn create(hash_join_state: Arc<dyn HashJoinState>, use_partial_compact: bool) -> Self {
        RightJoinCompactor {
            use_partial_compact,
            hash_join_state,
        }
    }
}

impl Compactor for RightJoinCompactor {
    fn name() -> &'static str {
        "RightJoinCompactor"
    }

    fn use_partial_compact(&self) -> bool {
        self.use_partial_compact
    }

    fn interrupt(&self) {
        self.hash_join_state.interrupt();
    }

    fn compact_partial(&mut self, blocks: &mut Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let rest_len = self.hash_join_state.join_state().rest_pairs.read().1.len();
        let mut res = blocks.clone();
        blocks.clear();
        if rest_len >= JOIN_MAX_BLOCK_SIZE {
            res.extend(self.hash_join_state.left_join_blocks(&[])?);
        }
        Ok(res)
    }

    // `compact_final` is called when all the blocks are pushed
    fn compact_final(&self, blocks: &[DataBlock]) -> Result<Vec<DataBlock>> {
        self.hash_join_state.right_join_blocks(blocks)
    }
}

pub type TransformRightJoin = TransformCompact<RightJoinCompactor>;
