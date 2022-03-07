use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfo;
use common_planners::PartInfoPtr;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct FusePartInfo {
    pub location: String,
    pub file_size: u64,
}

#[typetag::serde(name = "fuse")]
impl PartInfo for FusePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FusePartInfo {
    pub fn create(location: String, file_size: u64) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FusePartInfo {
            location,
            file_size,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FusePartInfo> {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::LogicalError(
                "Cannot downcast from PartInfo to FusePartInfo.",
            )),
        }
    }
}
