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

#[derive(Debug, PartialEq, Eq)]
pub struct CompactMetaInfo {
    pub order: usize,
    pub segment_location: String,
    pub segment_info: Arc<SegmentInfo>,
}

impl MetaInfo for CompactMetaInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn MetaInfo>) -> bool {
        match info.as_any().downcast_ref::<CompactMetaInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl CompactMetaInfo {
    pub fn create(
        order: usize,
        segment_location: String,
        segment_info: Arc<SegmentInfo>,
    ) -> MetaInfoPtr {
        Arc::new(Box::new(CompactMetaInfo {
            order,
            segment_location,
            segment_info,
        }))
    }

    pub fn from_meta(info: &MetaInfoPtr) -> Result<&CompactMetaInfo> {
        match info.as_any().downcast_ref::<CompactMetaInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from MetaInfo to CompactMetaInfo.",
            )),
        }
    }
}
