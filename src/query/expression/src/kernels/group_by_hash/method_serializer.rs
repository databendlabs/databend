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

use super::utils::serialize_group_columns;
use crate::types::binary::BinaryIterator;
use crate::BinaryKeyAccessor;
use crate::Column;
use crate::HashMethod;
use crate::InputColumnsWithDataType;
use crate::KeyAccessor;
use crate::KeysState;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSerializer {}

impl HashMethod for HashMethodSerializer {
    type HashKey = [u8];

    type HashKeyIter<'a> = BinaryIterator<'a>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: InputColumnsWithDataType,
        num_rows: usize,
    ) -> Result<KeysState> {
        // The serialize_size is equal to the number of bytes required by serialization.
        let InputColumnsWithDataType { columns, .. } = group_columns;
        let serialize_size = columns.iter().map(|column| column.serialize_size()).sum();
        Ok(KeysState::Column(Column::Binary(serialize_group_columns(
            columns,
            num_rows,
            serialize_size,
        ))))
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::Column(Column::Binary(col)) => Ok(col.iter()),
            _ => unreachable!(),
        }
    }

    fn build_keys_accessor_and_hashes(
        &self,
        keys_state: KeysState,
        hashes: &mut Vec<u64>,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
        match keys_state {
            KeysState::Column(Column::Binary(col)) => {
                hashes.extend(col.iter().map(hash_join_fast_string_hash));
                let (data, offsets) = col.into_buffer();
                Ok(Box::new(BinaryKeyAccessor::new(data, offsets)))
            }
            _ => unreachable!(),
        }
    }
}
