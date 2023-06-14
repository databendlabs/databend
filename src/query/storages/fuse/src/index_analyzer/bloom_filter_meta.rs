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

use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;
use storages_common_index::filters::BlockFilter;

use crate::index_analyzer::block_filter_meta::BlockFilterMeta;
use crate::pruning::SegmentLocation;

pub struct BloomFilterMeta {
    pub inner: BlockFilterMeta,
    pub filter_version: u64,
    pub block_filter: Option<BlockFilter>,
}

impl BloomFilterMeta {
    pub fn create(
        inner: BlockFilterMeta,
        version: u64,
        block_filter: Option<BlockFilter>,
    ) -> BlockMetaInfoPtr {
        Box::new(BloomFilterMeta {
            inner,
            block_filter,
            filter_version: version,
        })
    }
}

impl Debug for BloomFilterMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilterMeta")
            // .field("location", &self.segment_location)
            .finish()
    }
}

impl serde::Serialize for BloomFilterMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize BloomFilterMeta")
    }
}

impl<'de> serde::Deserialize<'de> for BloomFilterMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize BloomFilterMeta")
    }
}

#[typetag::serde(name = "block_bloom_filter_meta")]
impl BlockMetaInfo for BloomFilterMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals BloomFilterMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone BloomFilterMeta")
    }
}
