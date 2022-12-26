use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;

use common_arrow::native::read::reader::PaReader;
use common_arrow::native::read::PaReadBuf;
use common_catalog::plan::PartInfoPtr;
use common_datablocks::BlockMetaInfo;
use common_datablocks::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;

pub type DataChunks = Vec<(usize, PaReader<Box<dyn PaReadBuf + Send + Sync>>)>;

pub struct NativeDataSourceMeta {
    pub part: Vec<PartInfoPtr>,
    pub chunks: Vec<DataChunks>,
}

impl NativeDataSourceMeta {
    pub fn create(part: Vec<PartInfoPtr>, chunks: Vec<DataChunks>) -> BlockMetaInfoPtr {
        Box::new(NativeDataSourceMeta { part, chunks })
    }
}

impl Debug for NativeDataSourceMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeDataSourceMeta")
            .field("part", &self.part)
            .finish()
    }
}

impl serde::Serialize for NativeDataSourceMeta {
    fn serialize<S>(&self, _: S) -> common_exception::Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize NativeDataSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for NativeDataSourceMeta {
    fn deserialize<D>(_: D) -> common_exception::Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize NativeDataSourceMeta")
    }
}

#[typetag::serde(name = "fuse_data_source")]
impl BlockMetaInfo for NativeDataSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone NativeDataSourceMeta")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals NativeDataSourceMeta")
    }
}
