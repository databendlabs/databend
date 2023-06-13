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
    pub block_filter: Option<BlockFilter>,
}

impl BloomFilterMeta {
    pub fn create(inner: BlockFilterMeta, block_filter: Option<BlockFilter>) -> BlockMetaInfoPtr {
        Box::new(BloomFilterMeta {
            inner,
            block_filter,
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
