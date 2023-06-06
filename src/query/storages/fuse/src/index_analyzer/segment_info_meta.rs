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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;
use storages_common_table_meta::meta::CompactSegmentInfo;

use crate::pruning::SegmentLocation;

pub struct CompactSegmentInfoMeta {
    pub info: Arc<CompactSegmentInfo>,
    pub segment_location: SegmentLocation,
}

impl CompactSegmentInfoMeta {
    pub fn create(
        segment_location: SegmentLocation,
        info: Arc<CompactSegmentInfo>,
    ) -> BlockMetaInfoPtr {
        Box::new(CompactSegmentInfoMeta {
            info,
            segment_location,
        })
    }
}

impl Debug for CompactSegmentInfoMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactSegmentInfoMeta")
            .field("location", &self.segment_location)
            .finish()
    }
}

impl serde::Serialize for CompactSegmentInfoMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize CompactSegmentInfoMeta")
    }
}

impl<'de> serde::Deserialize<'de> for CompactSegmentInfoMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize CompactSegmentInfoMeta")
    }
}

#[typetag::serde(name = "snapshot_meta_info")]
impl BlockMetaInfo for CompactSegmentInfoMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals CompactSegmentInfoMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone CompactSegmentInfoMeta")
    }
}
