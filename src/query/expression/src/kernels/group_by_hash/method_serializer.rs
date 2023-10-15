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

use std::iter::TrustedLen;

use common_exception::Result;
use common_hashtable::FastHash;

use super::keys_ref::KeysRef;
use super::keys_ref::KeysRefIterator;
use super::utils::serialize_columns;
use super::utils::serialize_columns_in_batch;
use crate::types::string::StringIterator;
use crate::types::DataType;
use crate::Column;
use crate::HashMethod;
use crate::KeysState;

const BATCH_SERIALIZE_BYTES_LIMIT: usize = 16 * 1024 * 1024; // 16 MB

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashMethodSerializer {
    buffer: Vec<u8>,
}

impl HashMethod for HashMethodSerializer {
    type HashKey = [u8];

    type HashKeyIter<'a> = SerializedIterator<'a>;

    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: &[(Column, DataType)],
        num_rows: usize,
    ) -> Result<KeysState> {
        // The serialize_size is equal to the number of bytes required by serialization.
        let mut serialize_size = 0;
        let columns = group_columns
            .iter()
            .map(|(col, _)| {
                serialize_size += col.serialize_size();
                col.clone()
            })
            .collect::<Vec<_>>();
        Ok(KeysState::Column(Column::String(serialize_columns(
            &columns,
            num_rows,
            serialize_size,
        ))))
    }

    fn mutable_build_keys_state(
        &mut self,
        group_columns: &[(Column, DataType)],
        num_rows: usize,
    ) -> Result<KeysState> {
        // We choose the maximum row memory of columns to guranntee that the memory of the serialized column will not exceed it.
        let mut max_bytes_per_row = 0;
        let columns = group_columns
            .iter()
            .map(|(col, _)| {
                max_bytes_per_row += col.max_serialize_size_per_row();
                col.clone()
            })
            .collect::<Vec<_>>();

        if max_bytes_per_row * num_rows > BATCH_SERIALIZE_BYTES_LIMIT {
            // If the memory consumption exceed the limit, we degragde to non-batch serialization,
            // which will not use the buffer.
            self.build_keys_state(group_columns, num_rows)
        } else {
            let keys_ref =
                serialize_columns_in_batch(&columns, num_rows, max_bytes_per_row, &mut self.buffer);
            Ok(KeysState::KeysRef(keys_ref))
        }
    }

    fn build_keys_iter<'a>(&self, key_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match key_state {
            KeysState::KeysRef(keys_ref) => Ok(keys_ref.iter().into()),
            KeysState::Column(Column::String(col)) => Ok(col.iter().into()),
            _ => unreachable!(),
        }
    }

    fn build_keys_iter_and_hashes<'a>(
        &self,
        keys_state: &'a KeysState,
    ) -> Result<(Self::HashKeyIter<'a>, Vec<u64>)> {
        match keys_state {
            KeysState::KeysRef(keys_ref) => {
                let mut hashes = Vec::with_capacity(keys_ref.len());
                hashes.extend(keys_ref.iter().map(|key| key.slice().fast_hash()));
                Ok((keys_ref.iter().into(), hashes))
            }
            KeysState::Column(Column::String(col)) => {
                let mut hashes = Vec::with_capacity(col.len());
                hashes.extend(col.iter().map(|key| key.fast_hash()));
                Ok((col.iter().into(), hashes))
            }
            _ => unreachable!(),
        }
    }
}

pub enum SerializedIterator<'a> {
    KeysRef(KeysRefIterator<'a>),
    String(StringIterator<'a>),
}

impl<'a> From<std::slice::Iter<'a, KeysRef>> for SerializedIterator<'a> {
    fn from(value: std::slice::Iter<'a, KeysRef>) -> Self {
        Self::KeysRef(value.into())
    }
}

impl<'a> From<StringIterator<'a>> for SerializedIterator<'a> {
    fn from(value: StringIterator<'a>) -> Self {
        Self::String(value)
    }
}

impl<'a> Iterator for SerializedIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::KeysRef(iter) => iter.next(),
            Self::String(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::KeysRef(iter) => iter.size_hint(),
            Self::String(iter) => iter.size_hint(),
        }
    }
}

unsafe impl TrustedLen for SerializedIterator<'_> {}
