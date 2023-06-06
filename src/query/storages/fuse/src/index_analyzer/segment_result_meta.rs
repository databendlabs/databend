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
use storages_common_table_meta::meta::{BlockMeta, CompactSegmentInfo};

use crate::pruning::SegmentLocation;

pub struct SegmentFilterResult {
    pub metas: Vec<Arc<BlockMeta>>,
    pub segment_location: SegmentLocation,
}

impl SegmentFilterResult {
    pub fn create(location: SegmentLocation, metas: Vec<Arc<BlockMeta>>) -> BlockMetaInfoPtr {
        Box::new(SegmentFilterResult { segment_location: location, metas })
    }
}

impl Debug for SegmentFilterResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentFilterResult")
            // .field("location", &self.segment_location)
            .finish()
    }
}

impl serde::Serialize for SegmentFilterResult {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        unimplemented!("Unimplemented serialize SegmentFilterResult")
    }
}

impl<'de> serde::Deserialize<'de> for SegmentFilterResult {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize SegmentFilterResult")
    }
}

#[typetag::serde(name = "segment_filter_result")]
impl BlockMetaInfo for SegmentFilterResult {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals SegmentFilterResult")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone SegmentFilterResult")
    }
}
