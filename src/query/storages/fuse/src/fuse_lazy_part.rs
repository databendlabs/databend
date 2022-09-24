use std::any::Any;
use std::sync::Arc;

use common_fuse_meta::meta::Location;
use common_legacy_planners::PartInfo;
use common_legacy_planners::PartInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FuseLazyPartInfo {
    pub segment_location: Location,
}

#[typetag::serde(name = "fuse_lazy")]
impl PartInfo for FuseLazyPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<FuseLazyPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl FuseLazyPartInfo {
    pub fn create(segment_location: Location) -> PartInfoPtr {
        Arc::new(Box::new(FuseLazyPartInfo { segment_location }))
    }
}
