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
pub struct FileOutputBuffers {
    pub buffers: Vec<Vec<u8>>,
}

impl FileOutputBuffers {
    pub fn create_block(buffers: Vec<Vec<u8>>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(FileOutputBuffers { buffers }))
    }
}

impl Clone for FileOutputBuffers {
    fn clone(&self) -> Self {
        unreachable!("Buffers should not be cloned")
    }
}

impl serde::Serialize for FileOutputBuffers {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("FileOutputBuffers should not be serialized")
    }
}

impl<'de> serde::Deserialize<'de> for FileOutputBuffers {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("FileOutputBuffers should not be deserialized")
    }
}

#[typetag::serde(name = "unload_buffers")]
impl BlockMetaInfo for FileOutputBuffers {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("FileOutputBuffers should not be compared")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("FileOutputBuffers should not be cloned")
    }
}
