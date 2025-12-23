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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::local_block_meta_serde;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;

pub struct BlockPruneResult {
    pub block_metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
}

impl BlockPruneResult {
    pub fn create(block_metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>) -> BlockMetaInfoPtr {
        Box::new(BlockPruneResult { block_metas })
    }
}

impl Debug for BlockPruneResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockPruneResult").finish()
    }
}

local_block_meta_serde!(BlockPruneResult);

#[typetag::serde(name = "block_prune_result")]
impl BlockMetaInfo for BlockPruneResult {}
