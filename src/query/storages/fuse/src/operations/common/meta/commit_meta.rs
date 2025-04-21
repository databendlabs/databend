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

use databend_common_exception::ErrorCode;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::VirtualDataSchema;
use databend_storages_common_table_meta::meta::Location;

use crate::operations::common::ConflictResolveContext;
use crate::operations::common::SnapshotChanges;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CommitMeta {
    pub conflict_resolve_context: ConflictResolveContext,
    pub new_segment_locs: Vec<Location>,
    pub table_id: u64,
    pub virtual_schema: Option<VirtualDataSchema>,
}

impl CommitMeta {
    pub fn empty(table_id: u64) -> Self {
        CommitMeta {
            conflict_resolve_context: ConflictResolveContext::ModifiedSegmentExistsInLatest(
                SnapshotChanges::default(),
            ),
            new_segment_locs: vec![],
            table_id,
            virtual_schema: None,
        }
    }

    pub fn new(
        conflict_resolve_context: ConflictResolveContext,
        new_segment_locs: Vec<Location>,
        table_id: u64,
        virtual_schema: Option<VirtualDataSchema>,
    ) -> Self {
        CommitMeta {
            conflict_resolve_context,
            new_segment_locs,
            table_id,
            virtual_schema,
        }
    }
}

#[typetag::serde(name = "commit_meta")]
impl BlockMetaInfo for CommitMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        Self::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl TryFrom<DataBlock> for CommitMeta {
    type Error = ErrorCode;
    fn try_from(value: DataBlock) -> std::result::Result<Self, Self::Error> {
        let block_meta = value.get_owned_meta().ok_or_else(|| {
            ErrorCode::Internal(
                "converting data block meta to CommitMeta failed, no data block meta found",
            )
        })?;
        CommitMeta::downcast_from(block_meta).ok_or_else(|| {
            ErrorCode::Internal("downcast block meta to CommitMeta failed, type mismatch")
        })
    }
}

impl From<CommitMeta> for DataBlock {
    fn from(value: CommitMeta) -> Self {
        let block_meta = Box::new(value);
        DataBlock::empty_with_meta(block_meta)
    }
}
