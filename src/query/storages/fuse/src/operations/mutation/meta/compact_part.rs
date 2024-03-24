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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;

use crate::operations::common::BlockMetaIndex;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;

/// Compact segment part information.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct CompactLazyPartInfo {
    pub segment_indices: Vec<SegmentIndex>,
    pub compact_segments: Vec<Arc<CompactSegmentInfo>>,
}

#[typetag::serde(name = "compact_lazy")]
impl PartInfo for CompactLazyPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<CompactLazyPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.segment_indices.hash(&mut s);
        s.finish()
    }

    fn part_type(&self) -> PartInfoType {
        PartInfoType::LazyLevel
    }
}

impl CompactLazyPartInfo {
    pub fn create(
        segment_indices: Vec<SegmentIndex>,
        compact_segments: Vec<Arc<CompactSegmentInfo>>,
    ) -> PartInfoPtr {
        Arc::new(Box::new(CompactLazyPartInfo {
            segment_indices,
            compact_segments,
        }))
    }
}

/// Compact block part information.
#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub enum CompactBlockPartInfo {
    CompactExtraInfo(CompactExtraInfo),
    CompactTaskInfo(CompactTaskInfo),
}

#[typetag::serde(name = "compact_part_info")]
impl PartInfo for CompactBlockPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<CompactBlockPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        match self {
            Self::CompactExtraInfo(extra) => extra.hash(),
            Self::CompactTaskInfo(task) => task.hash(),
        }
    }
}

impl CompactBlockPartInfo {
    pub fn from_part(info: &PartInfoPtr) -> Result<&CompactBlockPartInfo> {
        info.as_any()
            .downcast_ref::<CompactBlockPartInfo>()
            .ok_or_else(|| {
                ErrorCode::Internal("Cannot downcast from PartInfo to CompactBlockPartInfo.")
            })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CompactExtraInfo {
    pub segment_index: SegmentIndex,
    pub unchanged_blocks: Vec<(BlockIndex, Arc<BlockMeta>)>,
    pub removed_segment_indexes: Vec<SegmentIndex>,
    pub removed_segment_summary: Statistics,
}

impl CompactExtraInfo {
    pub fn create(
        segment_index: SegmentIndex,
        unchanged_blocks: Vec<(BlockIndex, Arc<BlockMeta>)>,
        removed_segment_indexes: Vec<SegmentIndex>,
        removed_segment_summary: Statistics,
    ) -> Self {
        CompactExtraInfo {
            segment_index,
            unchanged_blocks,
            removed_segment_indexes,
            removed_segment_summary,
        }
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.segment_index.hash(&mut s);
        s.finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct CompactTaskInfo {
    pub blocks: Vec<Arc<BlockMeta>>,
    pub index: BlockMetaIndex,
}

impl CompactTaskInfo {
    pub fn create(blocks: Vec<Arc<BlockMeta>>, index: BlockMetaIndex) -> Self {
        CompactTaskInfo { blocks, index }
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.blocks[0].location.0.hash(&mut s);
        s.finish()
    }
}
