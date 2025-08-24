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

use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodFixedKeys;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::ProjectedBlock;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::RawEntry;
use databend_common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;

pub trait JoinHashTable: Send + Sync + 'static {
    type Entry;

    type Method: HashMethod;

    fn insert(&self, key: &<Self::Method as HashMethod>::HashKey);

    fn entry(&self, key: &<Self::Method as HashMethod>::HashKey, row: RowPtr) -> Self::Entry;
}

// impl JoinHashTable for SerializerHashJoinHashTable {
//     type Method = HashMethodSerializer;
// }
//
// impl JoinHashTable for SingleBinaryHashJoinHashTable {
//     type Method = HashMethodSingleBinary;
// }
//
// impl JoinHashTable for FixedKeyHashJoinHashTable<u8> {
//     type Method = HashMethodFixedKeys<u8>;
// }

// impl JoinHashTable for

pub struct LocalMemoryHashTable<Table: JoinHashTable> {
    global: Arc<GlobalMemoryHashTable<Table>>,
    method: Table::Method,
    // local_space: Vec<u8>,
}

impl<Table: JoinHashTable> LocalMemoryHashTable<Table> {
    pub fn create(global: Arc<GlobalMemoryHashTable<Table>>) -> LocalMemoryHashTable<Table> {
        unimplemented!()
    }

    pub fn insert(&mut self, block: ProjectedBlock, valids: Option<Bitmap>) {
        // let keys_state = self.method.build_keys_state(block, 0)?;
        // let keys_iter = self.method.build_keys_iter(&keys_state)?;

        // match valids

        // keys_iter.zip()
        // let local_space
    }
}

pub struct GlobalMemoryHashTable<Table: JoinHashTable> {}
