use std::any::Any;
use common_datablocks::MetaInfo;

#[derive(Debug, PartialEq)]
pub struct AggregateInfo {
    pub bucket: usize,
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
