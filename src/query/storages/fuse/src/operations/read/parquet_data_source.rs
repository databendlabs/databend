use common_catalog::plan::PartInfoPtr;
use serde::{Deserializer, Serializer};
use common_datablocks::{BlockMetaInfo, BlockMetaInfoPtr};
use std::any::Any;

#[derive(Debug, PartialEq)]
pub struct DataSourceMeta {
    pub part: PartInfoPtr,
    pub data: Option<Vec<(usize, Vec<u8>)>>,
}

impl DataSourceMeta {
    pub fn create(part: PartInfoPtr, data: Vec<(usize, Vec<u8>)>) -> BlockMetaInfoPtr {
        Box::new(DataSourceMeta { part, data: Some(data) })
    }
}

impl serde::Serialize for DataSourceMeta {
    fn serialize<S>(&self, _: S) -> common_exception::Result<S::Ok, S::Error> where S: Serializer {
        unimplemented!("Unimplemented serialize DataSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for DataSourceMeta {
    fn deserialize<D>(_: D) -> common_exception::Result<Self, D::Error> where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize DataSourceMeta")
    }
}

#[typetag::serde(name = "fuse_data_source")]
impl BlockMetaInfo for DataSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone DataSourceMeta")
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<DataSourceMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
