use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;

use crate::SegmentLocation;

#[derive(Clone)]
pub struct SegmentLocationMeta {
    pub segment_location: SegmentLocation,
}

impl SegmentLocationMeta {
    pub fn create(segment_location: SegmentLocation) -> BlockMetaInfoPtr {
        Box::new(SegmentLocationMeta { segment_location })
    }
}

#[typetag::serde(name = "segment_location_meta")]
impl BlockMetaInfo for SegmentLocationMeta {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals SegmentLocationMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl Debug for SegmentLocationMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentLocationMeta")
            .field("segment_location", &self.segment_location)
            .finish()
    }
}

impl serde::Serialize for SegmentLocationMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize SegmentLocationMeta")
    }
}

impl<'de> serde::Deserialize<'de> for SegmentLocationMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize SegmentLocationMeta")
    }
}
