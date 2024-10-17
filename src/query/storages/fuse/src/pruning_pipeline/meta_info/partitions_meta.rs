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

use databend_common_catalog::plan::Partitions;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;

#[derive(Clone)]
pub struct PartitionsMeta {
    pub partitions: Partitions,
}

impl PartitionsMeta {
    pub fn create(partitions: Partitions) -> BlockMetaInfoPtr {
        Box::new(PartitionsMeta { partitions })
    }
}

#[typetag::serde(name = "partitions_meta")]
impl BlockMetaInfo for PartitionsMeta {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals PartitionsMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl Debug for PartitionsMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionsMeta")
            .field("partitions", &self.partitions)
            .finish()
    }
}

impl serde::Serialize for PartitionsMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize PartitionsMeta")
    }
}

impl<'de> serde::Deserialize<'de> for PartitionsMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize PartitionsMeta")
    }
}
