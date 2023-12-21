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

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;

#[derive(Debug)]
pub struct BlockBatch {
    pub blocks: Vec<DataBlock>,
}

impl BlockBatch {
    pub fn create_block(blocks: Vec<DataBlock>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(BlockBatch { blocks }))
    }
}

impl Clone for BlockBatch {
    fn clone(&self) -> Self {
        unreachable!("Buffers should not be cloned")
    }
}

impl serde::Serialize for BlockBatch {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("Buffers should not be serialized")
    }
}

impl<'de> serde::Deserialize<'de> for BlockBatch {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("Buffers should not be deserialized")
    }
}

#[typetag::serde(name = "unload_block_batch")]
impl BlockMetaInfo for BlockBatch {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("Buffers should not be compared")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("Buffers should not be cloned")
    }
}
