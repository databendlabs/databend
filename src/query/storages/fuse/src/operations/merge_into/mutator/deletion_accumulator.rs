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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::mutation::base_mutator::BlockIndex;
use crate::operations::mutation::base_mutator::SegmentIndex;

pub type BlockDeletionKeys = HashMap<BlockIndex, HashSet<UniqueKeyDigest>>;
#[derive(Default)]
pub struct DeletionAccumulator {
    pub deletions: HashMap<SegmentIndex, BlockDeletionKeys>,
}

impl DeletionAccumulator {
    pub fn add_block_deletion(
        &mut self,
        segment_index: SegmentIndex,
        block_index: BlockIndex,
        key_set: &HashSet<UniqueKeyDigest>,
    ) {
        match self.deletions.entry(segment_index) {
            Entry::Occupied(ref mut v) => {
                let block_deletions = v.get_mut();
                block_deletions
                    .entry(block_index)
                    .and_modify(|es| es.extend(key_set))
                    .or_insert(key_set.clone());
            }
            Entry::Vacant(e) => {
                e.insert(HashMap::from_iter([(block_index, key_set.clone())]));
            }
        }
    }
}
