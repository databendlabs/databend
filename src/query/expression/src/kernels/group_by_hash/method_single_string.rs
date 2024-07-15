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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_hashtable::hash_join_fast_string_hash;

use crate::types::binary::BinaryIterator;
use crate::Column;
use crate::HashMethod;
use crate::InputColumns;
use crate::KeyAccessor;
use crate::KeysState;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSingleBinary {}

impl HashMethod for HashMethodSingleBinary {
    type HashKey = [u8];

    type HashKeyIter<'a> = BinaryIterator<'a>;

    fn name(&self) -> String {
        "SingleBinary".to_string()
    }

    fn build_keys_state(&self, group_columns: InputColumns, _rows: usize) -> Result<KeysState> {
        Ok(KeysState::Column(group_columns[0].clone()))
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match keys_state {
            KeysState::Column(Column::Binary(col))
            | KeysState::Column(Column::Variant(col))
            | KeysState::Column(Column::Bitmap(col)) => Ok(col.iter()),
            KeysState::Column(Column::String(col)) => Ok(col.iter_binary()),
            _ => unreachable!(),
        }
    }

    fn build_keys_accessor_and_hashes(
        &self,
        keys_state: KeysState,
        hashes: &mut Vec<u64>,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
        match keys_state {
            KeysState::Column(Column::Binary(col))
            | KeysState::Column(Column::Variant(col))
            | KeysState::Column(Column::Bitmap(col)) => {
                hashes.extend(col.iter().map(hash_join_fast_string_hash));
                let (data, offsets) = col.into_buffer();
                Ok(Box::new(BinaryKeyAccessor::new(data, offsets)))
            }
            KeysState::Column(Column::String(col)) => {
                hashes.extend(col.iter_binary().map(hash_join_fast_string_hash));
                let (data, offsets) = col.into_buffer();
                Ok(Box::new(BinaryKeyAccessor::new(data, offsets)))
            }
            _ => unreachable!(),
        }
    }
}

pub struct BinaryKeyAccessor {
    data: Buffer<u8>,
    offsets: Buffer<u64>,
}

impl BinaryKeyAccessor {
    pub fn new(data: Buffer<u8>, offsets: Buffer<u64>) -> Self {
        Self { data, offsets }
    }
}

impl KeyAccessor for BinaryKeyAccessor {
    type Key = [u8];

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        debug_assert!(index + 1 < self.offsets.len());

        &self.data[*self.offsets.get_unchecked(index) as usize
            ..*self.offsets.get_unchecked(index + 1) as usize]
    }
}
