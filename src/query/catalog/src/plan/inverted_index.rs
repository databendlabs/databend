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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::Location;

/// This meta just indicate the block is from inverted index.
#[derive(Debug, Clone)]
pub struct InvertedIndexMeta {
    pub index_name: String,
    pub index_schema: TableSchemaRef,
    pub segment_locs: Option<Vec<Location>>,
}

impl InvertedIndexMeta {
    pub fn create(
        index_name: String,
        index_schema: TableSchemaRef,
        segment_locs: Option<Vec<Location>>,
    ) -> BlockMetaInfoPtr {
        Box::new(Self {
            index_name,
            index_schema,
            segment_locs,
        })
    }
}

impl serde::Serialize for InvertedIndexMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize InvertedIndexMeta")
    }
}

impl<'de> serde::Deserialize<'de> for InvertedIndexMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize InvertedIndexMeta")
    }
}

#[typetag::serde(name = "inverted_index_meta")]
impl BlockMetaInfo for InvertedIndexMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals InvertedIndexMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
