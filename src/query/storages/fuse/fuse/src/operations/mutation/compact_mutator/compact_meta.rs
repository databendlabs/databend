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
use std::sync::Arc;

use common_datablocks::MetaInfo;
use common_datablocks::MetaInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::SegmentInfo;

use super::compact_part::CompactTask;

#[derive(Debug, PartialEq, Eq)]
pub struct CompactSourceMeta {
    pub order: usize,
    pub tasks: Vec<CompactTask>,
}

impl MetaInfo for CompactSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn MetaInfo>) -> bool {
        match info.as_any().downcast_ref::<CompactSourceMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl CompactSourceMeta {
    pub fn create(order: usize, tasks: Vec<CompactTask>) -> MetaInfoPtr {
        Arc::new(Box::new(CompactSourceMeta { order, tasks }))
    }

    pub fn from_meta(info: &MetaInfoPtr) -> Result<&CompactSourceMeta> {
        match info.as_any().downcast_ref::<CompactSourceMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from MetaInfo to CompactSourceMeta.",
            )),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CompactSinkMeta {
    pub order: usize,
    pub segment_location: String,
    pub segment_info: Arc<SegmentInfo>,
}

impl MetaInfo for CompactSinkMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn MetaInfo>) -> bool {
        match info.as_any().downcast_ref::<CompactSinkMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl CompactSinkMeta {
    pub fn create(
        order: usize,
        segment_location: String,
        segment_info: Arc<SegmentInfo>,
    ) -> MetaInfoPtr {
        Arc::new(Box::new(CompactSinkMeta {
            order,
            segment_location,
            segment_info,
        }))
    }

    pub fn from_meta(info: &MetaInfoPtr) -> Result<&CompactSinkMeta> {
        match info.as_any().downcast_ref::<CompactSinkMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from MetaInfo to CompactSinkMeta.",
            )),
        }
    }
}
