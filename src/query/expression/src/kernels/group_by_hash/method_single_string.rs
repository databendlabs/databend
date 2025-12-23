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

use databend_common_exception::Result;
use databend_common_hashtable::hash_join_fast_string_hash;

use crate::Column;
use crate::HashMethod;
use crate::KeyAccessor;
use crate::KeysState;
use crate::ProjectedBlock;
use crate::types::BinaryColumn;
use crate::types::binary::BinaryColumnIter;
use crate::utils::bitmap::normalize_bitmap_column;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSingleBinary {}

impl HashMethod for HashMethodSingleBinary {
    type HashKey = [u8];

    type HashKeyIter<'a> = BinaryColumnIter<'a>;

    fn name(&self) -> String {
        "SingleBinary".to_string()
    }

    fn build_keys_state(&self, group_columns: ProjectedBlock, _rows: usize) -> Result<KeysState> {
        Ok(KeysState::Column(match group_columns[0].to_column() {
            Column::Bitmap(col) => match normalize_bitmap_column(&col) {
                std::borrow::Cow::Borrowed(_) => Column::Bitmap(col),
                std::borrow::Cow::Owned(col) => Column::Bitmap(col),
            },
            column => column,
        }))
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
            | KeysState::Column(Column::Bitmap(col)) => Ok(Box::new(col)),
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

impl KeyAccessor for BinaryColumn {
    type Key = [u8];

    /// # Safety
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    unsafe fn key_unchecked(&self, index: usize) -> &Self::Key {
        unsafe {
            debug_assert!(index + 1 < self.offsets().len());
            self.index_unchecked(index)
        }
    }

    fn len(&self) -> usize {
        self.len()
    }
}
