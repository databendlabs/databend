use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;

use crate::SegmentLocation;

#[derive(Clone)]
pub struct CompactSegmentMeta {
    pub compact_segment: Arc<CompactSegmentInfo>,
    pub location: SegmentLocation,
}

impl CompactSegmentMeta {
    pub fn create(
        compact_segment: Arc<CompactSegmentInfo>,
        location: SegmentLocation,
    ) -> BlockMetaInfoPtr {
        Box::new(CompactSegmentMeta {
            compact_segment,
            location,
        })
    }
}

#[typetag::serde(name = "compact_segment_meta")]
impl BlockMetaInfo for CompactSegmentMeta {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals CompactSegmentMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl Debug for CompactSegmentMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactSegmentMeta")
            .field("segment_location", &self.location)
            .finish()
    }
}

impl serde::Serialize for CompactSegmentMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize CompactSegmentMeta")
    }
}

impl<'de> serde::Deserialize<'de> for CompactSegmentMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize CompactSegmentMeta")
    }
}
