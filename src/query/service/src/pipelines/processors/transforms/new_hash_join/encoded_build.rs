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

//! Build rows in the form the hash join holds them once they are in its hash table.
//!
//! `final_build` projects each build chunk to the columns the join outputs and inserts the join
//! key of each valid row, encoded by the hash method, into the chunk's arena. The key columns
//! themselves are dropped. Grace hash join spills build rows in this same form: the build
//! projection followed by one column with the encoded key, NULL for rows whose key is NULL
//! (kept only for joins that output unmatched build rows). So a chunk already inserted into the
//! hash table spills as it is, and restored rows are inserted without evaluating keys again.

use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::hash_util::hash_keys_state_by_method;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt16Type;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use ethnum::u256;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::hash_join_table::RawEntry;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::new_hash_join::memory::BuildArena;

pub struct EncodedBuild {
    desc: Arc<HashJoinDesc>,
    method: HashMethodKind,
    function_ctx: FunctionContext,
    schema: DataSchemaRef,
}

impl EncodedBuild {
    pub fn create(
        desc: Arc<HashJoinDesc>,
        method: HashMethodKind,
        function_ctx: FunctionContext,
    ) -> EncodedBuild {
        let build_fields = desc.build_schema.fields();
        let mut fields = desc
            .build_projection
            .iter()
            .map(|index| build_fields[*index].clone())
            .collect::<Vec<_>>();
        fields.push(DataField::new(
            "_join_build_key",
            key_type(&method).wrap_nullable(),
        ));

        EncodedBuild {
            desc,
            method,
            function_ctx,
            schema: DataSchemaRefExt::create(fields),
        }
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    /// Encode a block of the build schema. Its keys are prepared as `final_build` prepares them.
    pub fn encode(&self, block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let keys = self.desc.build_key(&block, &self.function_ctx)?;
        let mut keys = DataBlock::new(keys, num_rows);
        let valids = self.desc.build_valids_by_keys(&keys)?;
        self.desc.remove_keys_nullable(&mut keys);

        let keys_state = self
            .method
            .build_keys_state(ProjectedBlock::from(keys.columns()), num_rows)?;
        let key = key_column(keys_state).wrap_nullable(valids);

        let mut entries = block.project(&self.desc.build_projection).take_columns();
        entries.push(key.into());
        Ok(DataBlock::new(entries, num_rows))
    }

    /// Encode a chunk that `final_build` has inserted into the hash table. The chunk is already
    /// projected, with the rows of valid keys first, and `arena` holds their encoded keys in row
    /// order. The remaining rows have NULL keys.
    pub fn encode_inserted(&self, chunk: DataBlock, arena: &BuildArena) -> Result<DataBlock> {
        let num_rows = chunk.num_rows();
        let key = arena_keys(&self.method, arena, num_rows);
        let validity = (arena.num_keys != num_rows)
            .then(|| Bitmap::from_trusted_len_iter((0..num_rows).map(|row| row < arena.num_keys)));

        let mut entries = chunk.take_columns();
        entries.push(key.wrap_nullable(validity).into());
        Ok(DataBlock::new(entries, num_rows))
    }

    /// Partition hashes of an encoded block. For equal keys they are the hashes `get_hashes`
    /// gives on the probe side: both hash the key the hash method encodes.
    pub fn hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let key = block.get_last_column().clone().remove_nullable();
        let keys_state = keys_state(&self.method, key)?;
        hash_keys_state_by_method(&self.method, &keys_state, hashes)
    }
}

/// Split an encoded block into the projected rows and their nullable key column.
pub fn split_encoded(block: DataBlock) -> (DataBlock, Column) {
    let num_rows = block.num_rows();
    let mut entries = block.take_columns();
    let key = entries.pop().unwrap().to_column();
    (DataBlock::new(entries, num_rows), key)
}

/// The encoded keys stored in a key column, which is not nullable.
pub fn keys_state(method: &HashMethodKind, key: Column) -> Result<KeysState> {
    match method {
        HashMethodKind::KeysU128(_) => Ok(KeysState::U128(
            fixed_width_keys(&key, u128::from_le_bytes)?.into(),
        )),
        HashMethodKind::KeysU256(_) => Ok(KeysState::U256(
            fixed_width_keys(&key, u256::from_le_bytes)?.into(),
        )),
        _ => Ok(KeysState::Column(key)),
    }
}

/// The column type that holds the keys encoded by `method`.
fn key_type(method: &HashMethodKind) -> DataType {
    match method {
        HashMethodKind::KeysU8(_) => DataType::Number(NumberDataType::UInt8),
        HashMethodKind::KeysU16(_) => DataType::Number(NumberDataType::UInt16),
        HashMethodKind::KeysU32(_) => DataType::Number(NumberDataType::UInt32),
        HashMethodKind::KeysU64(_) => DataType::Number(NumberDataType::UInt64),
        HashMethodKind::KeysU128(_)
        | HashMethodKind::KeysU256(_)
        | HashMethodKind::Serializer(_)
        | HashMethodKind::SingleBinary(_) => DataType::Binary,
    }
}

/// Keys up to 64 bits are encoded as an unsigned integer column already; wider fixed keys are
/// stored as their little endian bytes, and binary-like keys as binary.
fn key_column(keys_state: KeysState) -> Column {
    match keys_state {
        KeysState::Column(Column::Variant(column) | Column::Bitmap(column)) => {
            Column::Binary(column)
        }
        KeysState::Column(column) => column,
        KeysState::U128(keys) => fixed_width_column(keys.iter().map(|key| key.to_le_bytes())),
        KeysState::U256(keys) => fixed_width_column(keys.iter().map(|key| key.to_le_bytes())),
    }
}

/// The encoded keys of a chunk inserted into the hash table, one per row. Rows behind the
/// arena's keys have NULL keys and get a placeholder.
fn arena_keys(method: &HashMethodKind, arena: &BuildArena, num_rows: usize) -> Column {
    match method {
        HashMethodKind::KeysU8(_) => UInt8Type::from_data(fixed_arena_keys::<u8>(arena, num_rows)),
        HashMethodKind::KeysU16(_) => {
            UInt16Type::from_data(fixed_arena_keys::<u16>(arena, num_rows))
        }
        HashMethodKind::KeysU32(_) => {
            UInt32Type::from_data(fixed_arena_keys::<u32>(arena, num_rows))
        }
        HashMethodKind::KeysU64(_) => {
            UInt64Type::from_data(fixed_arena_keys::<u64>(arena, num_rows))
        }
        HashMethodKind::KeysU128(_) => fixed_width_column(
            fixed_arena_keys::<u128>(arena, num_rows)
                .iter()
                .map(|key| key.to_le_bytes()),
        ),
        HashMethodKind::KeysU256(_) => fixed_width_column(
            fixed_arena_keys::<u256>(arena, num_rows)
                .iter()
                .map(|key| key.to_le_bytes()),
        ),
        HashMethodKind::Serializer(_) | HashMethodKind::SingleBinary(_) => {
            string_arena_keys(arena, num_rows)
        }
    }
}

fn fixed_arena_keys<T: Copy + Default>(arena: &BuildArena, num_rows: usize) -> Vec<T> {
    let entries = match arena.num_keys {
        0 => &[],
        // Safety: the fixed key hash tables write one entry per key at the start of the arena.
        num_keys => unsafe {
            std::slice::from_raw_parts(arena.data.as_ptr() as *const RawEntry<T>, num_keys)
        },
    };

    let mut keys = Vec::with_capacity(num_rows);
    keys.extend(entries.iter().map(|entry| entry.key));
    keys.resize(num_rows, T::default());
    keys
}

fn string_arena_keys(arena: &BuildArena, num_rows: usize) -> Column {
    let entries = match arena.num_keys {
        0 => &[],
        // Safety: the binary key hash tables write one entry per key at the start of the arena,
        // followed by the key bytes the entries point to.
        num_keys => unsafe {
            std::slice::from_raw_parts(arena.data.as_ptr() as *const StringRawEntry, num_keys)
        },
    };

    let data_capacity = entries.iter().map(|entry| entry.length as usize).sum();
    let mut builder = BinaryColumnBuilder::with_capacity(num_rows, data_capacity);
    for entry in entries {
        // Safety: the key bytes live in the same arena as the entry.
        let key = unsafe { std::slice::from_raw_parts(entry.key, entry.length as usize) };
        builder.put_slice(key);
        builder.commit_row();
    }
    for _ in entries.len()..num_rows {
        builder.commit_row();
    }
    Column::Binary(builder.build())
}

fn fixed_width_column<const N: usize>(keys: impl Iterator<Item = [u8; N]>) -> Column {
    let (num_rows, _) = keys.size_hint();
    let mut builder = BinaryColumnBuilder::with_capacity(num_rows, num_rows * N);
    for key in keys {
        builder.put_slice(&key);
        builder.commit_row();
    }
    Column::Binary(builder.build())
}

fn fixed_width_keys<T, const N: usize>(key: &Column, decode: fn([u8; N]) -> T) -> Result<Vec<T>> {
    let Column::Binary(column) = key else {
        return Err(ErrorCode::Internal(format!(
            "spilled hash join key must be binary, got {}",
            key.data_type()
        )));
    };

    column
        .iter()
        .map(|bytes| {
            let bytes = bytes.try_into().map_err(|_| {
                ErrorCode::Internal(format!(
                    "spilled hash join key must have {N} bytes, got {}",
                    bytes.len()
                ))
            })?;
            Ok(decode(bytes))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::HashMethodKind;
    use databend_common_expression::ProjectedBlock;
    use databend_common_expression::hash_util::hash_by_method;
    use databend_common_expression::hash_util::hash_keys_state_by_method;
    use databend_common_expression::types::*;
    use ethnum::U256;

    use super::arena_keys;
    use super::key_column;
    use super::keys_state;
    use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
    use crate::pipelines::processors::transforms::SerializerHashJoinHashTable;
    use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
    use crate::pipelines::processors::transforms::hash_join_table::BinaryHashJoinHashMap;
    use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashMap;
    use crate::pipelines::processors::transforms::new_hash_join::memory::BuildArena;

    /// Insert `keys` into the hash table `final_build` would choose for them and check that the
    /// keys taken from the arena are the keys the hash method encodes, and that they hash like
    /// the key columns hash on the probe side.
    fn check_keys(keys: Vec<Column>) {
        let num_keys = keys[0].len();
        let null_key_rows = 2;
        let key_types = keys.iter().map(|c| c.data_type()).collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&key_types).unwrap();
        let block = DataBlock::new(keys.into_iter().map(|c| c.into()).collect(), num_keys);
        let columns = ProjectedBlock::from(block.columns());
        let state = method.build_keys_state(columns, num_keys).unwrap();

        let mut arena = Vec::with_capacity(0);
        match &method {
            HashMethodKind::Serializer(m) => SerializerHashJoinHashTable::<false>::new(
                BinaryHashJoinHashMap::<false>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::SingleBinary(m) => SingleBinaryHashJoinHashTable::<false>::new(
                BinaryHashJoinHashMap::<false>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::KeysU8(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u8>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::KeysU16(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u16>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::KeysU32(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u32>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::KeysU64(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u64>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::KeysU128(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<u128>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
            HashMethodKind::KeysU256(m) => FixedKeyHashJoinHashTable::new(
                HashJoinHashMap::<U256>::with_build_row_num(num_keys),
                m.clone(),
            )
            .insert(&state, num_keys, 0, &mut arena),
        }
        .unwrap();

        let arena = BuildArena {
            chunk_index: 0,
            num_keys,
            data: arena,
        };
        // The chunk also has rows with NULL keys behind the valid ones.
        let from_arena = arena_keys(&method, &arena, num_keys + null_key_rows);
        assert_eq!(from_arena.len(), num_keys + null_key_rows);
        let from_arena = from_arena.slice(0..num_keys);

        let encoded = key_column(state);
        assert_eq!(from_arena, encoded, "{method:?}");

        let mut probe_hashes = vec![];
        hash_by_method(&method, columns, num_keys, &mut probe_hashes).unwrap();
        let mut spill_hashes = vec![];
        let state = keys_state(&method, from_arena).unwrap();
        hash_keys_state_by_method(&method, &state, &mut spill_hashes).unwrap();
        assert_eq!(spill_hashes, probe_hashes, "{method:?}");
    }

    #[test]
    fn test_fixed_keys() {
        check_keys(vec![UInt8Type::from_data(vec![0, 7, 255])]);
        check_keys(vec![Int16Type::from_data(vec![-300, 0, 299])]);
        check_keys(vec![Int32Type::from_data(vec![-200, 0, 199])]);
        check_keys(vec![DateType::from_data(vec![0, 364, -1])]);
        check_keys(vec![Float64Type::from_data(vec![-37.5, 0.25, 1e300])]);
        check_keys(vec![UInt64Type::from_data(vec![0, 7, u64::MAX])]);
        check_keys(vec![TimestampType::from_data(vec![0, 599_000_000, -1])]);
        check_keys(vec![Decimal128Type::from_data_with_size(
            [-49_900i128, 0, 10i128.pow(19)],
            Some(DecimalSize::new_unchecked(20, 2)),
        )]);
        check_keys(vec![
            UInt16Type::from_data(vec![1, 2, 3]),
            UInt8Type::from_data(vec![4, 5, 6]),
        ]);
        check_keys(vec![
            Int64Type::from_data(vec![1, -2, 3]),
            Int64Type::from_data(vec![-4, 5, -6]),
        ]);
        check_keys(vec![
            Int64Type::from_data(vec![1, -2, 3]),
            Int64Type::from_data(vec![-4, 5, -6]),
            Int64Type::from_data(vec![7, 8, -9]),
        ]);
        // Keys that keep NULL as a value, e.g. for INTERSECT.
        check_keys(vec![
            UInt16Type::from_data_with_validity(vec![1, 0, 3], vec![true, false, true]),
            UInt8Type::from_data(vec![4, 5, 6]),
        ]);
    }

    #[test]
    fn test_binary_keys() {
        check_keys(vec![StringType::from_data(vec!["", "k1", "a longer key"])]);
        check_keys(vec![
            StringType::from_data(vec!["a", "b", "c"]),
            UInt8Type::from_data(vec![4, 5, 6]),
        ]);
        check_keys(vec![StringType::from_data_with_validity(
            vec!["x", "", "z"],
            vec![true, false, true],
        )]);
        check_keys(vec![BinaryType::from_data(vec![
            b"".to_vec(),
            b"\x00\x01".to_vec(),
            b"bytes".to_vec(),
        ])]);
    }

    #[test]
    fn test_empty_chunk() {
        check_keys(vec![UInt64Type::from_data(Vec::<u64>::new())]);
        check_keys(vec![StringType::from_data(Vec::<&str>::new())]);
    }
}
