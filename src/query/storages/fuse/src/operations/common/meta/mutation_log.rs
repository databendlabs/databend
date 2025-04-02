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

use databend_common_exception::ErrorCode;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::VirtualDataSchema;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::FormatVersion;
use databend_storages_common_table_meta::meta::Statistics;

use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::CompactExtraInfo;
use crate::operations::mutation::DeletedSegmentInfo;
use crate::operations::mutation::SegmentIndex;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
pub struct MutationLogs {
    pub entries: Vec<MutationLogEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum MutationLogEntry {
    AppendSegment {
        segment_location: String,
        format_version: FormatVersion,
        summary: Statistics,
        virtual_schema: Option<VirtualDataSchema>,
    },
    ReclusterAppendBlock {
        block_meta: Arc<BlockMeta>,
    },
    DeletedBlock {
        index: BlockMetaIndex,
    },
    DeletedSegment {
        deleted_segment: DeletedSegmentInfo,
    },
    ReplacedBlock {
        index: BlockMetaIndex,
        block_meta: Arc<BlockMeta>,
    },
    CompactExtras {
        extras: CompactExtraInfo,
    },
    DoNothing,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BlockMetaIndex {
    pub segment_idx: SegmentIndex,
    pub block_idx: BlockIndex,
    // range is unused for now.
    // pub range: Option<Range<usize>>,
}

#[typetag::serde(name = "block_meta_index")]
impl BlockMetaInfo for BlockMetaIndex {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        BlockMetaIndex::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[typetag::serde(name = "mutation_logs_meta")]
impl BlockMetaInfo for MutationLogs {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        Self::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl From<MutationLogs> for DataBlock {
    fn from(value: MutationLogs) -> Self {
        let block_meta = Box::new(value);
        DataBlock::empty_with_meta(block_meta)
    }
}

impl TryFrom<DataBlock> for MutationLogs {
    type Error = ErrorCode;
    fn try_from(value: DataBlock) -> std::result::Result<Self, Self::Error> {
        let block_meta = value.get_owned_meta().ok_or_else(|| {
            ErrorCode::Internal(
                "converting data block meta to MutationLogs failed, no data block meta found",
            )
        })?;
        MutationLogs::downcast_from(block_meta).ok_or_else(|| {
            ErrorCode::Internal("downcast block meta to MutationLogs failed, type mismatch")
        })
    }
}
