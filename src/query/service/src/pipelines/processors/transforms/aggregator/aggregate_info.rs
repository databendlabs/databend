use std::any::Any;
use std::sync::Arc;

use common_datablocks::MetaInfo;
use common_datablocks::MetaInfoPtr;

#[derive(Debug, PartialEq)]
pub struct AggregateInfo {
    pub bucket: isize,
}

impl AggregateInfo {
    pub fn create(bucket: isize) -> MetaInfoPtr {
        Arc::new(Box::new(AggregateInfo { bucket }))
    }
}

impl MetaInfo for AggregateInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn MetaInfo>) -> bool {
        match info.as_any().downcast_ref::<AggregateInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
