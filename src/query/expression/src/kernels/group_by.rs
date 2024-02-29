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

use super::group_by_hash::HashMethodKeysU16;
use super::group_by_hash::HashMethodKeysU32;
use super::group_by_hash::HashMethodKeysU64;
use super::group_by_hash::HashMethodKeysU8;
use super::group_by_hash::HashMethodKind;
use super::group_by_hash::HashMethodSerializer;
use super::group_by_hash::HashMethodSingleBinary;
use crate::types::DataType;
use crate::DataBlock;
use crate::HashMethodDictionarySerializer;
use crate::HashMethodKeysU128;
use crate::HashMethodKeysU256;

impl DataBlock {
    pub fn choose_hash_method(
        chunk: &DataBlock,
        indices: &[usize],
        efficiently_memory: bool,
    ) -> Result<HashMethodKind> {
        let hash_key_types = indices
            .iter()
            .map(|&offset| {
                let col = chunk.get_by_offset(offset);
                Ok(col.data_type.clone())
            })
            .collect::<Result<Vec<_>>>();

        let hash_key_types = hash_key_types?;
        Self::choose_hash_method_with_types(&hash_key_types, efficiently_memory)
    }

    pub fn choose_hash_method_with_types(
        hash_key_types: &[DataType],
        efficiently_memory: bool,
    ) -> Result<HashMethodKind> {
        if hash_key_types.len() == 1
            && matches!(
                hash_key_types[0],
                DataType::Binary | DataType::String | DataType::Variant | DataType::Bitmap
            )
        {
            return Ok(HashMethodKind::SingleBinary(
                HashMethodSingleBinary::default(),
            ));
        }

        let mut group_key_len = 0;
        for hash_key_type in hash_key_types {
            let not_null_type = hash_key_type.remove_nullable();

            if not_null_type.is_numeric()
                || not_null_type.is_date_or_date_time()
                || not_null_type.is_decimal()
            {
                group_key_len += not_null_type.numeric_byte_size().unwrap();

                // extra one byte for null flag
                if hash_key_type.is_nullable() {
                    group_key_len += 1;
                }
            } else if !efficiently_memory || hash_key_types.len() == 1 {
                return Ok(HashMethodKind::Serializer(HashMethodSerializer::default()));
            } else {
                return Ok(HashMethodKind::DictionarySerializer(
                    HashMethodDictionarySerializer {
                        dict_keys: hash_key_types.len(),
                    },
                ));
            }
        }

        match group_key_len {
            1 => Ok(HashMethodKind::KeysU8(HashMethodKeysU8::default())),
            2 => Ok(HashMethodKind::KeysU16(HashMethodKeysU16::default())),
            3..=4 => Ok(HashMethodKind::KeysU32(HashMethodKeysU32::default())),
            5..=8 => Ok(HashMethodKind::KeysU64(HashMethodKeysU64::default())),
            9..=16 => Ok(HashMethodKind::KeysU128(HashMethodKeysU128::default())),
            17..=32 => Ok(HashMethodKind::KeysU256(HashMethodKeysU256::default())),
            _ => Ok(HashMethodKind::Serializer(HashMethodSerializer::default())),
        }
    }
}
