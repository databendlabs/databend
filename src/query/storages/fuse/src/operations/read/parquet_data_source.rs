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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;

use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;

use crate::io::MergeIOReadResult;

pub struct DataSourceMeta {
    pub part: Vec<PartInfoPtr>,
    pub data: Vec<MergeIOReadResult>,
}

impl DataSourceMeta {
    pub fn create(part: Vec<PartInfoPtr>, data: Vec<MergeIOReadResult>) -> BlockMetaInfoPtr {
        Box::new(DataSourceMeta { part, data })
    }
}

impl Debug for DataSourceMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataSourceMeta")
            .field("parts", &self.part)
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
