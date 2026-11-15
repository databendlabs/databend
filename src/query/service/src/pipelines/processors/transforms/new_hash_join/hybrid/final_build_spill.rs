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

//! Spilling a hash join while its in-memory hash table is being built.
//!
//! `final_build` rewrites every chunk it inserts: the chunk is projected to the build projection
//! and rows with NULL keys are dropped, or moved behind the valid rows when the join scans build
//! rows. The key columns that only feed the join keys are gone afterwards, but the encoded key of
//! every valid row stays in the chunk's hash table arena, in row order. Grace hash join needs
//! blocks in the build schema, so a rewritten chunk is restored from its projected columns and the
//! keys decoded from its arena. This needs no memory beyond the chunk being restored, whose arena
//! is released right after.

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::HashMethodKind;
use databend_common_expression::expr::ColumnRef;
use databend_common_expression::types::DataType;
use ethnum::U256;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RawEntry;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::new_hash_join::memory::BuildArena;

/// A build chunk waiting to be moved into grace hash join after a spill in `final_build`.
pub struct BuildSpillChunk {
    pub block: DataBlock,
    /// Set when `final_build` has rewritten the chunk.
    pub arena: Option<BuildArena>,
}

enum BuildColumnSource {
    /// Offset in a rewritten chunk, which keeps the build projection in schema order.
    Projected(usize),
    /// Only read by the join key at this index, which is a reference to the column.
    Key(usize),
}

pub struct FinalBuildSpill {
    sources: Vec<BuildColumnSource>,
}

impl FinalBuildSpill {
    /// Returns `None` when a rewritten chunk cannot be restored: some build column is neither
    /// projected nor referenced directly by a join key, so its values are lost.
    pub fn try_create(desc: &HashJoinDesc) -> Option<FinalBuildSpill> {
        let fields = desc.build_schema.fields();
        let mut sources = Vec::with_capacity(fields.len());
        let mut projected = 0;

        for (index, field) in fields.iter().enumerate() {
            if desc.build_projection.contains(&index) {
                sources.push(BuildColumnSource::Projected(projected));
                projected += 1;
                continue;
            }

            let key = desc.build_keys.iter().position(|expr| {
                matches!(expr, Expr::ColumnRef(ColumnRef { id, data_type, .. })
                    if *id == index
                        && data_type.remove_nullable() == field.data_type().remove_nullable())
            })?;
            sources.push(BuildColumnSource::Key(key));
        }

        Some(FinalBuildSpill { sources })
    }

    /// Restore a build chunk to the build schema.
    pub fn restore_block(
        &self,
        desc: &HashJoinDesc,
        method: &HashMethodKind,
        chunk: BuildSpillChunk,
    ) -> Result<DataBlock> {
        let Some(arena) = chunk.arena else {
            return Ok(chunk.block);
        };

        let block = chunk.block;
        let num_rows = block.num_rows();
        // Rows behind the valid keys have NULL keys; they are kept only for joins that scan
        // build rows. Any value works for their key columns as long as the row stays invalid.
        let null_key_rows = num_rows - arena.num_keys;

        let needs_keys = self
            .sources
            .iter()
            .any(|source| matches!(source, BuildColumnSource::Key(_)));
        let keys = match needs_keys {
            true => decode_keys(method, &arena)?,
            false => vec![],
        };
        drop(arena);

        let fields = desc.build_schema.fields();
        let mut entries = Vec::with_capacity(self.sources.len());
        for (source, field) in self.sources.iter().zip(fields.iter()) {
            match source {
                BuildColumnSource::Projected(offset) => {
                    entries.push(block.get_by_offset(*offset).clone());
                }
                BuildColumnSource::Key(key) => {
                    let column =
                        restore_key_column(keys[*key].clone(), field.data_type(), null_key_rows);
                    entries.push(column.into());
                }
            }
        }

        Ok(DataBlock::new(entries, num_rows))
    }
}

/// Convert decoded key values back to the type of the build column they came from, then append
/// the rows whose keys are NULL.
fn restore_key_column(keys: Column, data_type: &DataType, null_key_rows: usize) -> Column {
    let keys = match (keys.data_type().is_nullable(), data_type.is_nullable()) {
        (false, true) => keys.wrap_nullable(None),
        (true, false) => keys.remove_nullable(),
        _ => keys,
    };

    if null_key_rows == 0 {
        return keys;
    }

    // A nullable column gets NULL, which keeps the row invalid if this key made it invalid.
    // Otherwise another key of the row is NULL and this value is never compared.
    let mut builder = ColumnBuilder::from_column(keys);
    for _ in 0..null_key_rows {
        builder.push_default();
    }
    builder.build()
}

fn decode_keys(method: &HashMethodKind, arena: &BuildArena) -> Result<Vec<Column>> {
    match method {
        HashMethodKind::Serializer(_) => decode_serialized_keys(arena),
        HashMethodKind::SingleBinary(_) => decode_binary_key(arena),
        HashMethodKind::KeysU8(_) => decode_fixed_keys::<u8>(arena),
        HashMethodKind::KeysU16(_) => decode_fixed_keys::<u16>(arena),
        HashMethodKind::KeysU32(_) => decode_fixed_keys::<u32>(arena),
        HashMethodKind::KeysU64(_) => decode_fixed_keys::<u64>(arena),
        HashMethodKind::KeysU128(_) => decode_fixed_keys::<u128>(arena),
        HashMethodKind::KeysU256(_) => decode_fixed_keys::<U256>(arena),
    }
}

fn string_entries(arena: &BuildArena) -> &[StringRawEntry] {
    if arena.num_keys == 0 {
        return &[];
    }

    // Safety: the binary hash join tables write one entry per key at the start of the arena.
    unsafe {
        std::slice::from_raw_parts(arena.data.as_ptr() as *const StringRawEntry, arena.num_keys)
    }
}

fn string_entry_key(entry: &StringRawEntry) -> &[u8] {
    // Safety: the key bytes live in the same arena as the entry.
    unsafe { std::slice::from_raw_parts(entry.key, entry.length as usize) }
}

/// Keys encoded by `serialize_group_columns`, whose inverse is `ColumnBuilder::push_binary`.
fn decode_serialized_keys(arena: &BuildArena) -> Result<Vec<Column>> {
    let mut builders = arena
        .key_types
        .iter()
        .map(|data_type| ColumnBuilder::with_capacity(data_type, arena.num_keys))
        .collect::<Vec<_>>();

    for entry in string_entries(arena) {
        let mut reader = string_entry_key(entry);
        for builder in builders.iter_mut() {
            builder.push_binary(&mut reader)?;
        }
    }

    Ok(builders
        .into_iter()
        .map(|builder| builder.build())
        .collect())
}

/// A single binary key is stored as its own bytes.
fn decode_binary_key(arena: &BuildArena) -> Result<Vec<Column>> {
    let [data_type] = arena.key_types.as_slice() else {
        return Err(ErrorCode::Internal(
            "single binary hash join key must have exactly one column",
        ));
    };

    let mut builder = ColumnBuilder::with_capacity(data_type, arena.num_keys);
    for entry in string_entries(arena) {
        let key = string_entry_key(entry);
        match &mut builder {
            ColumnBuilder::Binary(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder) => {
                builder.put_slice(key);
                builder.commit_row();
            }
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "unexpected single binary hash join key type: {data_type}"
                )));
            }
        }
    }

    Ok(vec![builder.build()])
}

/// Keys packed by `HashMethodFixedKeys`: values are stored little endian, ordered by their byte
/// size, followed by one NULL flag byte for each nullable key. A single non-nullable key takes the
/// bits of its value directly, which is the same layout on little endian targets.
fn decode_fixed_keys<T>(arena: &BuildArena) -> Result<Vec<Column>> {
    let mut order = (0..arena.key_types.len()).collect::<Vec<_>>();
    // Same stable order as `HashMethodFixedKeys::build_keys_vec`.
    order.sort_by_key(|index| fixed_key_size(&arena.key_types[*index]));

    let mut value_offsets = vec![0; arena.key_types.len()];
    let mut null_offsets = vec![None; arena.key_types.len()];
    let mut offset = 0;
    for index in order.iter() {
        value_offsets[*index] = offset;
        offset += fixed_key_size(&arena.key_types[*index]);
    }
    for index in order.iter() {
        if arena.key_types[*index].is_nullable() {
            null_offsets[*index] = Some(offset);
            offset += 1;
        }
    }

    if offset > std::mem::size_of::<T>() {
        return Err(ErrorCode::Internal(
            "fixed hash join keys exceed the size of the hash key",
        ));
    }

    let mut builders = arena
        .key_types
        .iter()
        .map(|data_type| ColumnBuilder::with_capacity(&data_type.remove_nullable(), arena.num_keys))
        .collect::<Vec<_>>();
    let mut validities = arena
        .key_types
        .iter()
        .map(|data_type| {
            data_type
                .is_nullable()
                .then(|| MutableBitmap::with_capacity(arena.num_keys))
        })
        .collect::<Vec<_>>();

    let entries = match arena.num_keys {
        0 => &[],
        // Safety: the fixed key hash join tables write one entry per key at the start of the
        // arena.
        num_keys => unsafe {
            std::slice::from_raw_parts(arena.data.as_ptr() as *const RawEntry<T>, num_keys)
        },
    };

    for entry in entries {
        // Safety: a fixed key is plain bytes.
        let key = unsafe {
            std::slice::from_raw_parts(
                &entry.key as *const T as *const u8,
                std::mem::size_of::<T>(),
            )
        };

        for (index, builder) in builders.iter_mut().enumerate() {
            let is_null = null_offsets[index].is_some_and(|offset| key[offset] != 0);
            if let Some(validity) = validities[index].as_mut() {
                validity.push(!is_null);
            }

            match is_null {
                true => builder.push_default(),
                false => {
                    let start = value_offsets[index];
                    let size = fixed_key_size(&arena.key_types[index]);
                    let mut value = &key[start..start + size];
                    builder.push_binary(&mut value)?;
                }
            }
        }
    }

    Ok(builders
        .into_iter()
        .zip(validities)
        .map(|(builder, validity)| match validity {
            None => builder.build(),
            Some(validity) => builder.build().wrap_nullable(Some(validity.into())),
        })
        .collect())
}

fn fixed_key_size(data_type: &DataType) -> usize {
    data_type
        .remove_nullable()
        .numeric_byte_size()
        .expect("fixed hash join keys are numeric")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::HashMethodKind;
    use databend_common_expression::types::*;
    use ethnum::U256;
    use jsonb::OwnedJsonb;

    use super::decode_keys;
    use super::restore_key_column;
    use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
    use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
    use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
    use crate::pipelines::processors::transforms::UniqueSerializerHashJoinHashTable;
    use crate::pipelines::processors::transforms::hash_join_table::BinaryHashJoinHashMap;
    use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashMap;
    use crate::pipelines::processors::transforms::new_hash_join::memory::BuildArena;

    /// Insert `keys` into the hash table `final_build` would choose for them, then decode
    /// the keys back from the arena the insert filled.
    fn insert_and_decode(keys: Vec<Column>, unique: bool) -> Vec<Column> {
        let num_rows = keys[0].len();
        let key_types = keys.iter().map(|c| c.data_type()).collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&key_types).unwrap();
        let block = DataBlock::new(keys.into_iter().map(|c| c.into()).collect(), num_rows);

        let mut arena = Vec::with_capacity(0);
        match &method {
            HashMethodKind::Serializer(m) if unique => UniqueSerializerHashJoinHashTable::new(
                BinaryHashJoinHashMap::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::Serializer(m) => SerializerHashJoinHashTable::<false>::new(
                BinaryHashJoinHashMap::<false>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::SingleBinary(m) => SingleBinaryHashJoinHashTable::<false>::new(
                BinaryHashJoinHashMap::<false>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::KeysU8(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u8>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::KeysU16(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u16>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::KeysU32(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u32>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::KeysU64(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u64>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::KeysU128(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u128>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
            HashMethodKind::KeysU256(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<U256>::with_build_row_num(num_rows),
                m.clone(),
            )
            .insert(block, 0, &mut arena),
        }
        .unwrap();

        let arena = BuildArena {
            chunk_index: 0,
            num_keys: num_rows,
            key_types,
            data: arena,
        };
        decode_keys(&method, &arena).unwrap()
    }

    fn assert_round_trip(keys: Vec<Column>) {
        assert_eq!(insert_and_decode(keys.clone(), false), keys);
    }

    #[test]
    fn test_decode_single_fixed_keys() {
        assert_round_trip(vec![UInt8Type::from_data(vec![0, 7, 255])]);
        assert_round_trip(vec![Int8Type::from_data(vec![-50, 0, 49])]);
        assert_round_trip(vec![UInt16Type::from_data(vec![1, 300, 65535])]);
        assert_round_trip(vec![Int32Type::from_data(vec![-200, 0, 199])]);
        assert_round_trip(vec![UInt32Type::from_data(vec![500, 1999, 4_000_000_000])]);
        assert_round_trip(vec![UInt64Type::from_data(vec![0, 7, u64::MAX])]);
        assert_round_trip(vec![Float64Type::from_data(vec![-37.5, 0.25, 1e300])]);
        assert_round_trip(vec![DateType::from_data(vec![0, 364, -1])]);
        assert_round_trip(vec![TimestampType::from_data(vec![0, 599_000_000, -1])]);
    }

    #[test]
    fn test_decode_decimal_keys() {
        let d64 = DecimalSize::new_unchecked(10, 2);
        assert_round_trip(vec![Decimal64Type::from_data_with_size(
            [-39_900i64, 0, 39_900],
            Some(d64),
        )]);

        let d128 = DecimalSize::new_unchecked(20, 2);
        assert_round_trip(vec![Decimal128Type::from_data_with_size(
            [-49_900i128, 0, 10i128.pow(19)],
            Some(d128),
        )]);

        let d256 = DecimalSize::new_unchecked(50, 2);
        assert_round_trip(vec![Decimal256Type::from_data_with_size(
            [
                i256::from(-39_900),
                i256::from(0),
                i256::from(10i128.pow(37)),
            ],
            Some(d256),
        )]);
    }

    #[test]
    fn test_decode_packed_fixed_keys() {
        // Packed ordered by byte size, so the decoder must restore the original key order.
        assert_round_trip(vec![
            UInt16Type::from_data(vec![1, 300, 65535]),
            UInt8Type::from_data(vec![6, 0, 255]),
        ]);

        // Nullable keys keep a NULL flag byte when NULL equals NULL.
        assert_round_trip(vec![
            UInt16Type::from_data_with_validity(vec![1, 0, 399], vec![true, false, true]),
            UInt8Type::from_data_with_validity(vec![4, 3, 0], vec![true, true, false]),
        ]);
    }

    #[test]
    fn test_decode_serialized_keys() {
        assert_round_trip(vec![StringType::from_data(vec![
            "key_0".to_string(),
            String::new(),
            "key_1199".to_string(),
        ])]);

        assert_round_trip(vec![
            StringType::from_data(vec!["a".to_string(), "bb".to_string(), "".to_string()]),
            UInt8Type::from_data(vec![1, 2, 3]),
        ]);

        assert_round_trip(vec![StringType::from_data_with_validity(
            vec!["a".to_string(), String::new(), "c".to_string()],
            vec![true, false, true],
        )]);

        // Semi and anti joins use the unique table, which writes the same arena entries.
        let keys = vec![StringType::from_data(vec![
            "dup".to_string(),
            "dup".to_string(),
            "other".to_string(),
        ])];
        assert_eq!(insert_and_decode(keys.clone(), true), keys);
    }

    #[test]
    fn test_decode_binary_key() {
        let values = [r#"1"#, r#""key""#, r#"{"a":[1,2]}"#]
            .into_iter()
            .map(|json| OwnedJsonb::from_str(json).unwrap().to_vec())
            .collect();
        assert_round_trip(vec![VariantType::from_data(values)]);
    }

    #[test]
    fn test_decode_no_keys() {
        let keys = vec![UInt32Type::from_data(vec![])];
        assert_eq!(insert_and_decode(keys.clone(), false), keys);
    }

    #[test]
    fn test_restore_key_column() {
        // Valid keys come first; rows with NULL keys follow and get NULL again.
        let keys = UInt32Type::from_data(vec![1, 2]);
        let data_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32)));
        assert_eq!(
            restore_key_column(keys, &data_type, 2),
            UInt32Type::from_data_with_validity(vec![1, 2, 0, 0], vec![true, true, false, false])
        );

        // A key decoded as nullable for a non-nullable build column.
        let keys = UInt32Type::from_data_with_validity(vec![1, 2], vec![true, true]);
        let data_type = DataType::Number(NumberDataType::UInt32);
        assert_eq!(
            restore_key_column(keys, &data_type, 0),
            UInt32Type::from_data(vec![1, 2])
        );
    }
}
