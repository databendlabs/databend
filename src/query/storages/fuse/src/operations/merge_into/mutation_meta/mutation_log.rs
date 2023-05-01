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
use std::ops::Range;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::DataBlock;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;

use crate::operations::mutation::AbortOperation;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
pub struct MutationLogs {
    pub entries: Vec<MutationLogEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum MutationLogEntry {
    Replacement(ReplacementLogEntry),
    Append(AppendOperationLogEntry),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReplacementLogEntry {
    pub index: BlockMetaIndex,
    pub op: Replacement,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Replacement {
    Replaced(Arc<BlockMeta>),
    Deleted, // replace something with nothing
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BlockMetaIndex {
    pub segment_idx: usize,
    pub block_idx: usize,
    pub range: Option<Range<usize>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AppendOperationLogEntry {
    pub segment_location: String,
    pub segment_info: Arc<SegmentInfo>,
    pub format_version: FormatVersion,
}

impl AppendOperationLogEntry {
    pub fn new(
        segment_location: String,
        segment_info: Arc<SegmentInfo>,
        format_version: FormatVersion,
    ) -> Self {
        Self {
            segment_location,
            segment_info,
            format_version,
        }
    }
}

impl MutationLogs {
    pub fn push_append(&mut self, log_entry: AppendOperationLogEntry) {
        self.entries.push(MutationLogEntry::Append(log_entry))
    }
}

#[typetag::serde(name = "mutation_logs_meta")]
impl BlockMetaInfo for MutationLogs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<MutationLogs>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl MutationLogs {
    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&MutationLogs> {
        match info.as_any().downcast_ref::<MutationLogs>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to MutationLogs.",
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CommitMeta {
    pub segments: Vec<Location>,
    pub summary: Statistics,
    pub abort_operation: AbortOperation,
}

impl CommitMeta {
    pub fn new(
        segments: Vec<Location>,
        summary: Statistics,
        abort_operation: AbortOperation,
    ) -> Self {
        CommitMeta {
            segments,
            summary,
            abort_operation,
        }
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&CommitMeta> {
        match info.as_any().downcast_ref::<CommitMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to MutationSinkMeta.",
            )),
        }
    }
}

#[typetag::serde(name = "commit_meta")]
impl BlockMetaInfo for CommitMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<CommitMeta>() {
            None => false,
            Some(other) => self == other,
        }
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
