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
use std::sync::Arc;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::SegmentLocation;

#[derive(Clone)]
pub struct ExtractSegmentResult {
    pub block_metas: Arc<Vec<Arc<BlockMeta>>>,
    pub segment_location: SegmentLocation,
}

impl ExtractSegmentResult {
    pub fn create(
        block_metas: Arc<Vec<Arc<BlockMeta>>>,
        segment_location: SegmentLocation,
    ) -> BlockMetaInfoPtr {
        Box::new(ExtractSegmentResult {
            block_metas,
            segment_location,
        })
    }
}

#[typetag::serde(name = "extract_segment_result")]
impl BlockMetaInfo for ExtractSegmentResult {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals ExtractSegmentResult")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl Debug for ExtractSegmentResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtractSegmentResult")
            .field("segment_location", &self.segment_location)
            .field("block_metas", &self.block_metas)
            .finish()
    }
}

impl serde::Serialize for ExtractSegmentResult {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize ExtractSegmentResult")
    }
}

impl<'de> serde::Deserialize<'de> for ExtractSegmentResult {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize ExtractSegmentResult")
    }
}
