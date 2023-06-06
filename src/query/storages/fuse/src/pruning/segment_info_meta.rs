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
    pub location: SegmentLocation,
    pub segment_info: Arc<CompactSegmentInfo>,
}

impl CompactSegmentInfoMeta {
    pub fn create(location: SegmentLocation, info: Arc<CompactSegmentInfo>) -> BlockMetaInfoPtr {
        Box::new(CompactSegmentInfoMeta {
            location,
            segment_info: info,
        })
    }
}

impl Debug for CompactSegmentInfoMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactSegmentInfoMeta")
            .field("location", &self.location)
            .finish()
    }
}

impl serde::Serialize for CompactSegmentInfoMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize DataSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for CompactSegmentInfoMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize DataSourceMeta")
    }
}

#[typetag::serde(name = "compact_segment_info")]
impl BlockMetaInfo for CompactSegmentInfoMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals DataSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone DataSourceMeta")
    }
}
