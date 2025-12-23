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
use crate::Column;
use crate::HashMethod;
use crate::KeyAccessor;
use crate::KeysState;
use crate::ProjectedBlock;
use crate::types::binary::BinaryColumnIter;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSerializer {}

impl HashMethod for HashMethodSerializer {
    type HashKey = [u8];

    type HashKeyIter<'a> = BinaryColumnIter<'a>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: ProjectedBlock,
        num_rows: usize,
    ) -> Result<KeysState> {
        // The serialize_size is equal to the number of bytes required by serialization.
        let serialize_size = group_columns
            .iter()
            .map(|entry| entry.as_column().unwrap().serialize_size())
            .sum();
        Ok(KeysState::Column(Column::Binary(serialize_group_columns(
            group_columns,
            num_rows,
            serialize_size,
        ))))
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::Column(Column::Binary(state)) => Ok(state.iter()),
            _ => unreachable!(),
        }
    }

    fn build_keys_accessor(
        &self,
        keys_state: KeysState,
    ) -> Result<Box<dyn KeyAccessor<Key = Self::HashKey>>> {
        match keys_state {
            KeysState::Column(Column::Binary(state)) => Ok(Box::new(state)),
            _ => unreachable!(),
        }
    }

    fn build_keys_hashes(&self, keys_state: &KeysState, hashes: &mut Vec<u64>) {
        match keys_state {
            KeysState::Column(Column::Binary(state)) => {
                hashes.extend(state.iter().map(hash_join_fast_string_hash));
            }
            _ => unreachable!(),
        }
    }
}
