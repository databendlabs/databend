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

use ahash::HashMap;
use ahash::HashSet;

use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::replace_into::meta::UniqueKeyDigest;

pub type BlockDeletionKeys = HashMap<BlockIndex, (HashSet<UniqueKeyDigest>, Vec<Vec<u64>>)>;
#[derive(Default)]
pub struct DeletionAccumulator {
    pub deletions: HashMap<SegmentIndex, BlockDeletionKeys>,
}

impl DeletionAccumulator {
    pub fn add_block_deletion(
        &mut self,
        segment_index: SegmentIndex,
        block_index: BlockIndex,
        source_on_conflict_key_set: &HashSet<UniqueKeyDigest>,
        source_bloom_hashes: &Vec<Vec<u64>>,
    ) {
        match self.deletions.entry(segment_index) {
            Entry::Occupied(ref mut v) => {
                let block_deletions = v.get_mut();
                block_deletions
                    .entry(block_index)
                    .and_modify(|(unique_digests, bloom_hashes)| {
                        unique_digests.extend(source_on_conflict_key_set);
                        assert_eq!(bloom_hashes.len(), source_bloom_hashes.len());
                        for (idx, acc) in bloom_hashes.iter_mut().enumerate() {
                            let bloom_hashes = &source_bloom_hashes[idx];
                            acc.extend(bloom_hashes);
                        }
                    })
                    .or_insert((
                        source_on_conflict_key_set.clone(),
                        source_bloom_hashes.clone(),
                    ));
            }
            Entry::Vacant(e) => {
                e.insert(HashMap::from_iter([(
                    block_index,
                    (
                        source_on_conflict_key_set.clone(),
                        source_bloom_hashes.clone(),
                    ),
                )]));
            }
        }
    }
}
