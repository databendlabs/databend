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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::FromData;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::decimal::Decimal128Type;
use proptest::prelude::*;

const MAX_ROWS: usize = 64;
const MAX_SPLITS: usize = 32;
const MAX_LIMIT: usize = 96;

fn assert_blocks_equal(actual: &DataBlock, expected: &DataBlock) {
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.num_columns(), expected.num_columns());
    for idx in 0..actual.num_columns() {
        assert_eq!(
            actual.get_by_offset(idx).to_column(),
            expected.get_by_offset(idx).to_column()
        );
    }
}

fn assert_sort_limit_equivalent(
    key_column: Column,
    use_const_key: bool,
    const_index: usize,
    raw_cuts: Vec<usize>,
    asc: bool,
    nulls_first: bool,
    limit_kind: u8,
    limit_value: usize,
) -> Result<()> {
    let num_rows = key_column.len();
    let row_ids = (0..num_rows).map(|i| i as u64).collect::<Vec<_>>();
    let payload = (0..num_rows)
        .map(|i| ((i as i32) * 13) - 97)
        .collect::<Vec<_>>();

    let key_entry = if use_const_key {
        let scalar = key_column
            .index(const_index % num_rows)
            .expect("const index out of bounds")
            .to_owned();
        BlockEntry::new_const_column(key_column.data_type(), scalar, num_rows)
    } else {
        key_column.into()
    };

    let full_block = DataBlock::new(
        vec![
            key_entry,
            UInt64Type::from_data(row_ids).into(),
            Int32Type::from_data(payload).into(),
        ],
        num_rows,
    );

    let mut split_points = raw_cuts
        .into_iter()
        .map(|cut| cut % (num_rows + 1))
        .collect::<Vec<_>>();
    split_points.push(num_rows);
    split_points.sort_unstable();

    let mut blocks = Vec::with_capacity(split_points.len());
    let mut start = 0;
    for end in split_points {
        blocks.push(full_block.slice(start..end));
        start = end;
    }

    let sort_desc: Arc<[SortColumnDescription]> = vec![
        SortColumnDescription {
            offset: 0,
            asc,
            nulls_first,
        },
        SortColumnDescription {
            offset: 1,
            asc: true,
            nulls_first: false,
        },
    ]
    .into();

    let limit = match limit_kind % 3 {
        0 => LimitType::None,
        1 => LimitType::LimitRows(limit_value),
        _ => LimitType::LimitRank(limit_value.max(1)),
    };

    let expected = DataBlock::sort_with_type(DataBlock::concat(&blocks)?, &sort_desc, limit)?;
    let actual = DataBlockVec::from_blocks(blocks)?.sort_limit(sort_desc, limit)?;
    assert_blocks_equal(&actual, &expected);
    Ok(())
}

fn arb_sort_key_column() -> impl Strategy<Value = Column> {
    prop_oneof![
        prop::collection::vec(any::<i64>(), 1..=MAX_ROWS).prop_map(Int64Type::from_data),
        prop::collection::vec(prop::option::of(any::<i64>()), 1..=MAX_ROWS)
            .prop_map(Int64Type::from_opt_data),
        prop::collection::vec(any::<bool>(), 1..=MAX_ROWS).prop_map(BooleanType::from_data),
        prop::collection::vec(prop::option::of(any::<bool>()), 1..=MAX_ROWS)
            .prop_map(BooleanType::from_opt_data),
        prop::collection::vec(
            proptest::string::string_regex("[a-z0-9]{0,8}").expect("invalid regex"),
            1..=MAX_ROWS
        )
        .prop_map(StringType::from_data),
        prop::collection::vec(
            prop::option::of(
                proptest::string::string_regex("[a-z0-9]{0,8}").expect("invalid regex")
            ),
            1..=MAX_ROWS
        )
        .prop_map(StringType::from_opt_data),
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8), 1..=MAX_ROWS)
            .prop_map(BinaryType::from_data),
        prop::collection::vec(
            prop::option::of(prop::collection::vec(any::<u8>(), 0..8)),
            1..=MAX_ROWS
        )
        .prop_map(BinaryType::from_opt_data),
        prop::collection::vec(any::<i32>(), 1..=MAX_ROWS).prop_map(DateType::from_data),
        prop::collection::vec(prop::option::of(any::<i32>()), 1..=MAX_ROWS)
            .prop_map(DateType::from_opt_data),
        prop::collection::vec(any::<i64>(), 1..=MAX_ROWS).prop_map(TimestampType::from_data),
        prop::collection::vec(prop::option::of(any::<i64>()), 1..=MAX_ROWS)
            .prop_map(TimestampType::from_opt_data),
        prop::collection::vec(any::<i128>(), 1..=MAX_ROWS)
            .prop_map(|keys| Decimal128Type::from_data_with_size(keys, None)),
        prop::collection::vec(prop::option::of(any::<i128>()), 1..=MAX_ROWS)
            .prop_map(|keys| Decimal128Type::from_opt_data_with_size(keys, None)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn prop_sort_limit_equivalent_random_type(
        key_column in arb_sort_key_column(),
        use_const_key in any::<bool>(),
        const_index in any::<usize>(),
        cuts in prop::collection::vec(0usize..=MAX_ROWS, 0..=MAX_SPLITS),
        asc in any::<bool>(),
        nulls_first in any::<bool>(),
        limit_kind in any::<u8>(),
        limit_value in 0usize..=MAX_LIMIT,
    ) {
        assert_sort_limit_equivalent(
            key_column,
            use_const_key,
            const_index,
            cuts,
            asc,
            nulls_first,
            limit_kind,
            limit_value,
        ).expect("sort_limit equivalence check failed");
    }
}
