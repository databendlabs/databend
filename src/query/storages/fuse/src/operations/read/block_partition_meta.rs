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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::local_block_meta_serde;

pub struct BlockPartitionMeta {
    pub part_ptr: Vec<PartInfoPtr>,
}

impl BlockPartitionMeta {
    pub fn create(part_ptr: Vec<PartInfoPtr>) -> BlockMetaInfoPtr {
        Box::new(BlockPartitionMeta { part_ptr })
    }
}

impl Debug for BlockPartitionMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockPartitionMeta")
            .field("part_ptr", &self.part_ptr)
            .finish()
    }
}

local_block_meta_serde!(BlockPartitionMeta);

#[typetag::serde(name = "block_partition_meta")]
impl BlockMetaInfo for BlockPartitionMeta {}
