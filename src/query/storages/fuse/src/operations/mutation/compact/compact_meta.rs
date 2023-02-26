// Copyright 2022 Datafuse Labs.
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
use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use storages_common_table_meta::meta::SegmentInfo;

use super::compact_part::CompactTask;
use crate::operations::mutation::AbortOperation;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CompactSourceMeta {
    pub order: usize,
    pub tasks: VecDeque<CompactTask>,
}

#[typetag::serde(name = "compact_source_meta")]
impl BlockMetaInfo for CompactSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match CompactSourceMeta::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl CompactSourceMeta {
    pub fn create(order: usize, tasks: VecDeque<CompactTask>) -> BlockMetaInfoPtr {
        Box::new(CompactSourceMeta { order, tasks })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&CompactSourceMeta> {
        match CompactSourceMeta::downcast_ref_from(info) {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to CompactSourceMeta.",
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CompactSinkMeta {
    pub order: usize,
    pub segment_location: String,
    pub segment_info: Arc<SegmentInfo>,
    pub abort_operation: AbortOperation,
}

#[typetag::serde(name = "compact_sink_meta")]
impl BlockMetaInfo for CompactSinkMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match CompactSinkMeta::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl CompactSinkMeta {
    pub fn create(
        order: usize,
        segment_location: String,
        segment_info: Arc<SegmentInfo>,
        abort_operation: AbortOperation,
    ) -> BlockMetaInfoPtr {
        Box::new(CompactSinkMeta {
            order,
            segment_location,
            segment_info,
            abort_operation,
        })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&CompactSinkMeta> {
        match CompactSinkMeta::downcast_ref_from(info) {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to CompactSinkMeta.",
            )),
        }
    }
}
