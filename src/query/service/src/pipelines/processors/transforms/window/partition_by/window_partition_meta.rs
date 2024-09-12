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

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;

pub struct WindowPartitionMeta {
    // Each element in `partitioned_data` is (partition_id, data_block).
    pub partitioned_data: Vec<(usize, DataBlock)>,
}

impl WindowPartitionMeta {
    pub fn create(partitioned_data: Vec<(usize, DataBlock)>) -> BlockMetaInfoPtr {
        Box::new(WindowPartitionMeta { partitioned_data })
    }
}

impl serde::Serialize for WindowPartitionMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }
}

impl<'de> serde::Deserialize<'de> for WindowPartitionMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }
}

impl Debug for WindowPartitionMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("WindowPartitionMeta").finish()
    }
}

impl BlockMetaInfo for WindowPartitionMeta {
    fn typetag_deserialize(&self) {
        unimplemented!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("WindowPartitionMeta does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for WindowPartitionMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for WindowPartitionMeta")
    }
}
