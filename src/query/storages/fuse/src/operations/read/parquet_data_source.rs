// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;

use crate::io::MergeIOReadResult;
use crate::io::VirtualMergeIOReadResult;

pub enum DataSource {
    AggIndex((PartInfoPtr, MergeIOReadResult)),
    Normal((MergeIOReadResult, Option<VirtualMergeIOReadResult>)),
}

pub struct DataSourceMeta {
    pub parts: Vec<PartInfoPtr>,
    pub data: Vec<DataSource>,
}

impl DataSourceMeta {
    pub fn create(part: Vec<PartInfoPtr>, data: Vec<DataSource>) -> BlockMetaInfoPtr {
        {
            let part_len = part.len();
            let data_len = data.len();
            assert_eq!(
                part_len, data_len,
                "Number of PartInfoPtr {} should equal to number of DataSource {}",
                part_len, data_len,
            );
        }

        Box::new(DataSourceMeta { parts: part, data })
    }
}

impl Debug for DataSourceMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataSourceMeta")
            .field("parts", &self.parts)
            .finish()
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
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals DataSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone DataSourceMeta")
    }
}
