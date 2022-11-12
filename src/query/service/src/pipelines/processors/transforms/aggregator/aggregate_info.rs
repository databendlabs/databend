use std::any::Any;
use std::sync::Arc;

use common_datablocks::BlockMetaInfo;
use common_datablocks::BlockMetaInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct AggregateInfo {
    pub bucket: isize,
}

impl AggregateInfo {
    pub fn create(bucket: isize) -> BlockMetaInfoPtr {
        Arc::new(Box::new(AggregateInfo { bucket }))
    }
}

#[typetag::serde(name = "aggregate_info")]
impl BlockMetaInfo for AggregateInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<AggregateInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
