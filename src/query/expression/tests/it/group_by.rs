// Copyright 2022 Datafuse Labs.
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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::*;
use ethnum::u256;

#[test]
fn test_group_by_hash() -> Result<()> {
    let schema = TableSchemaRefExt::create(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int8)),
        TableField::new("b", TableDataType::Number(NumberDataType::Int8)),
        TableField::new("c", TableDataType::Number(NumberDataType::Int8)),
        TableField::new("x", TableDataType::String),
    ]);

    let block = DataBlock::new_from_columns(vec![
        Int8Type::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Int8Type::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Int8Type::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        StringType::from_data(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let method = DataBlock::choose_hash_method(&block, &[0, 3])?;
    assert_eq!(method.name(), HashMethodSerializer::default().name(),);

    let method = DataBlock::choose_hash_method(&block, &[0, 1, 2])?;

    assert_eq!(method.name(), HashMethodKeysU32::default().name());

    let args = vec!["a", "b", "c"]
        .into_iter()
        .map(|col| schema.index_of(col).unwrap())
        .collect::<Vec<_>>();
    let group_columns = ProjectedBlock::project(&args, &block);

    let hash = HashMethodKeysU32::default();
    let state = hash.build_keys_state(group_columns, block.num_rows())?;
    let keys_iter = hash.build_keys_iter(&state)?;
    let keys: Vec<u32> = keys_iter.copied().collect();
    assert_eq!(keys, vec![
        0x10101, 0x10101, 0x20202, 0x10101, 0x20202, 0x30303
    ]);
    Ok(())
}

#[test]
fn test_group_by_hash_decimal() -> Result<()> {
    let size_128 = DecimalSize::new_unchecked(20, 2);
    let size_256 = DecimalSize::new_unchecked(40, 2);

    let decimal_128_values = [123456789_i128, 987654_i128, 123456789_i128];
    let decimal_256_values = [
        i256::from(123456789),
        i256::from(987654),
        i256::from(123456789),
    ];

    let block = DataBlock::new_from_columns(vec![
        Decimal128Type::from_data_with_size(decimal_128_values, Some(size_128)),
        Decimal256Type::from_data_with_size(decimal_256_values, Some(size_128)),
        Decimal128Type::from_data_with_size(decimal_128_values, Some(size_256)),
        Decimal256Type::from_data_with_size(decimal_256_values, Some(size_256)),
    ]);

    let method = DataBlock::choose_hash_method(&block, &[0])?;
    assert_eq!(method.name(), HashMethodKeysU128::default().name());
    let method = DataBlock::choose_hash_method(&block, &[1])?;
    assert_eq!(method.name(), HashMethodKeysU128::default().name());
    let method = DataBlock::choose_hash_method(&block, &[2])?;
    assert_eq!(method.name(), HashMethodKeysU256::default().name());
    let method = DataBlock::choose_hash_method(&block, &[3])?;
    assert_eq!(method.name(), HashMethodKeysU256::default().name());

    for i in [0, 1] {
        let args = &[i];
        let group_columns = ProjectedBlock::project(args, &block);
        let hash = HashMethodKeysU128::default();
        let state = hash.build_keys_state(group_columns, block.num_rows())?;
        let keys_iter = hash.build_keys_iter(&state)?;
        let keys = keys_iter.copied().collect::<Vec<_>>();

        assert_eq!(keys.len(), 3);
        assert_eq!(&keys, &[123456789_u128, 987654_u128, 123456789_u128]);
    }

    for i in [2, 3] {
        let args = &[i];
        let group_columns = ProjectedBlock::project(args, &block);
        let hash = HashMethodKeysU256::default();
        let state = hash.build_keys_state(group_columns, block.num_rows())?;
        let keys_iter = hash.build_keys_iter(&state)?;
        let keys = keys_iter.copied().collect::<Vec<_>>();

        assert_eq!(keys.len(), 3);
        assert_eq!(&keys, &[
            u256::from(123456789_u128),
            u256::from(987654_u128),
            u256::from(123456789_u128)
        ]);
    }

    Ok(())
}
