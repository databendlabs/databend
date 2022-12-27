//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;

use common_catalog::plan::PartInfoPtr;
use common_datablocks::BlockMetaInfo;
use common_datablocks::BlockMetaInfoPtr;
use common_exception::Result;
use serde::Deserializer;
use serde::Serializer;

pub type ParquetChunks = Vec<Vec<(usize, Vec<u8>)>>;

#[derive(Debug, PartialEq)]
pub struct DataSourceMeta {
    // memory: Vec<u8>,
    pub part: Vec<PartInfoPtr>,
    pub data: ParquetChunks,
}

impl DataSourceMeta {
    pub fn create(part: Vec<PartInfoPtr>, data: Vec<Vec<(usize, Vec<u8>)>>) -> BlockMetaInfoPtr {
        Box::new(DataSourceMeta { part, data })
    }
}

impl serde::Serialize for DataSourceMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize DataSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for DataSourceMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
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
