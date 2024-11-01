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

use databend_common_arrow::arrow::array::BinaryViewArray;
use databend_common_exception::Result;
use databend_common_hashtable::hash_join_fast_string_hash;

use crate::types::binary::BinaryIterator;
use crate::types::BinaryColumn;
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

    // Convert string column into binary column in build_keys_state
    fn build_keys_state(&self, group_columns: InputColumns, _rows: usize) -> Result<KeysState> {
        let binary_column = match group_columns[0].clone() {
            Column::String(c) => Column::Binary(BinaryColumn::from(c)),
            c => c,
        };
        Ok(KeysState::Column(binary_column))
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match keys_state {
            KeysState::Column(Column::Binary(col))
            | KeysState::Column(Column::Variant(col))
            | KeysState::Column(Column::Bitmap(col)) => Ok(col.iter()),
            _ => unreachable!(),
        }
    }

    fn build_keys_accessor(
        &self,
        keys_state: KeysState,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
        match keys_state {
            KeysState::Column(Column::Binary(col))
            | KeysState::Column(Column::Variant(col))
            | KeysState::Column(Column::Bitmap(col)) => {
                let data = col.into_inner();
                Ok(Box::new(BinaryViewKeyAccessor::new(data)))
            }
            _ => unreachable!(),
        }
    }

    fn build_keys_hashes(&self, keys_state: &KeysState, hashes: &mut Vec<u64>) {
        match keys_state {
            KeysState::Column(Column::Binary(col))
            | KeysState::Column(Column::Variant(col))
            | KeysState::Column(Column::Bitmap(col)) => {
                hashes.extend(col.iter().map(hash_join_fast_string_hash));
            }
            _ => unreachable!(),
        }
    }
}

pub struct BinaryViewKeyAccessor {
    data: BinaryViewArray,
}

impl BinaryViewKeyAccessor {
    pub fn new(data: BinaryViewArray) -> Self {
        Self { data }
    }
}

impl KeyAccessor for BinaryViewKeyAccessor {
    type Key = [u8];

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        debug_assert!(index < self.data.len());
        self.data.value_unchecked(index)
    }
}
