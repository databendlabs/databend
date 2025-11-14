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
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeysState;
use databend_common_expression::KeysState::U128;
use databend_common_expression::KeysState::U256;
use databend_common_hashtable::FastHash;
use xorf::BinaryFuse16;
use xorf::Filter;

/// Bloom filter for runtime filtering of data rows.
pub struct ExprBloomFilter<'a> {
    filter: &'a BinaryFuse16,
}

impl<'a> ExprBloomFilter<'a> {
    /// Create a new bloom filter.
    pub fn new(filter: &'a BinaryFuse16) -> ExprBloomFilter<'a> {
        Self { filter }
    }

    /// Apply the bloom filter to a column, updating the bitmap.
    pub fn apply(&self, column: Column, bitmap: &mut MutableBitmap) -> Result<()> {
        let data_type = column.data_type();
        let num_rows = column.len();
        let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()])?;
        let entries = &[column.into()];
        let group_columns = entries.into();
        let mut idx = 0;

        match method {
            HashMethodKind::Serializer(method) => {
                let key_state = method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    KeysState::Column(Column::Binary(col)) => col.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if self.filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    }),
                    _ => unreachable!(),
                }
            }
            HashMethodKind::SingleBinary(method) => {
                let key_state = method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    KeysState::Column(Column::Binary(col))
                    | KeysState::Column(Column::Variant(col))
                    | KeysState::Column(Column::Bitmap(col)) => col.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if self.filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    }),
                    KeysState::Column(Column::String(col)) => col.iter().for_each(|key| {
                        let hash = key.as_bytes().fast_hash();
                        if self.filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    }),
                    _ => unreachable!(),
                }
            }
            HashMethodKind::KeysU8(hash_method) => {
                let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    KeysState::Column(Column::Number(NumberColumn::UInt8(c))) => {
                        c.iter().for_each(|key| {
                            let hash = key.fast_hash();
                            if self.filter.contains(&hash) {
                                bitmap.set(idx, true);
                            }
                            idx += 1;
                        })
                    }
                    _ => unreachable!(),
                }
            }
            HashMethodKind::KeysU16(hash_method) => {
                let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    KeysState::Column(Column::Number(NumberColumn::UInt16(c))) => {
                        c.iter().for_each(|key| {
                            let hash = key.fast_hash();
                            if self.filter.contains(&hash) {
                                bitmap.set(idx, true);
                            }
                            idx += 1;
                        })
                    }
                    _ => unreachable!(),
                }
            }
            HashMethodKind::KeysU32(hash_method) => {
                let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    KeysState::Column(Column::Number(NumberColumn::UInt32(c))) => {
                        c.iter().for_each(|key| {
                            let hash = key.fast_hash();
                            if self.filter.contains(&hash) {
                                bitmap.set(idx, true);
                            }
                            idx += 1;
                        })
                    }
                    _ => unreachable!(),
                }
            }
            HashMethodKind::KeysU64(hash_method) => {
                let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    KeysState::Column(Column::Number(NumberColumn::UInt64(c))) => {
                        c.iter().for_each(|key| {
                            let hash = key.fast_hash();
                            if self.filter.contains(&hash) {
                                bitmap.set(idx, true);
                            }
                            idx += 1;
                        })
                    }
                    _ => unreachable!(),
                }
            }
            HashMethodKind::KeysU128(hash_method) => {
                let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    U128(c) => c.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if self.filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    }),
                    _ => unreachable!(),
                }
            }
            HashMethodKind::KeysU256(hash_method) => {
                let key_state = hash_method.build_keys_state(group_columns, num_rows)?;
                match key_state {
                    U256(c) => c.iter().for_each(|key| {
                        let hash = key.fast_hash();
                        if self.filter.contains(&hash) {
                            bitmap.set(idx, true);
                        }
                        idx += 1;
                    }),
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }
}
