// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;

use crate::kernels::HashMethodKeysU16;
use crate::kernels::HashMethodKeysU32;
use crate::kernels::HashMethodKeysU64;
use crate::kernels::HashMethodKeysU8;
use crate::kernels::HashMethodKind;
use crate::kernels::HashMethodSerializer;
use crate::DataBlock;
use crate::HashMethod;

impl DataBlock {
    pub fn choose_hash_method(
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<HashMethodKind> {
        let mut group_key_len = 0;
        for col in column_names {
            let column = block.try_column_by_name(col)?;
            let typ = column.data_type();
            if typ.data_type_id().is_integer() {
                group_key_len += typ.data_type_id().numeric_byte_size()?;
            } else {
                return Ok(HashMethodKind::Serializer(HashMethodSerializer::default()));
            }
        }
        match group_key_len {
            1 => Ok(HashMethodKind::KeysU8(HashMethodKeysU8::default())),
            2 => Ok(HashMethodKind::KeysU16(HashMethodKeysU16::default())),
            3..=4 => Ok(HashMethodKind::KeysU32(HashMethodKeysU32::default())),
            5..=8 => Ok(HashMethodKind::KeysU64(HashMethodKeysU64::default())),
            _ => Ok(HashMethodKind::Serializer(HashMethodSerializer::default())),
        }
    }

    pub fn group_by_blocks(block: &DataBlock, column_names: &[String]) -> Result<Vec<DataBlock>> {
        let method = Self::choose_hash_method(block, column_names)?;
        Ok(match method {
            HashMethodKind::Serializer(s) => {
                let blocks = s
                    .group_by(block, column_names)?
                    .iter()
                    .map(|(_, _, b)| b.clone())
                    .collect();
                blocks
            }
            HashMethodKind::KeysU8(s) => {
                let blocks = s
                    .group_by(block, column_names)?
                    .iter()
                    .map(|(_, _, b)| b.clone())
                    .collect();
                blocks
            }
            HashMethodKind::KeysU16(s) => {
                let blocks = s
                    .group_by(block, column_names)?
                    .iter()
                    .map(|(_, _, b)| b.clone())
                    .collect();
                blocks
            }
            HashMethodKind::KeysU32(s) => {
                let blocks = s
                    .group_by(block, column_names)?
                    .iter()
                    .map(|(_, _, b)| b.clone())
                    .collect();
                blocks
            }
            HashMethodKind::KeysU64(s) => {
                let blocks = s
                    .group_by(block, column_names)?
                    .iter()
                    .map(|(_, _, b)| b.clone())
                    .collect();
                blocks
            }
        })
    }
}
