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

use std::sync::Arc;
use std::vec;

use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::StringType;
use databend_common_expression::AbortChecker;
use databend_common_expression::CheckAbort;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::SortColumnDescription;

use crate::common::new_block;

#[test]
fn test_block_sort() -> Result<()> {
    let block = new_block(&[
        Int64Type::from_data(vec![6i64, 4, 3, 2, 1, 1, 7]),
        StringType::from_data(vec!["b1", "b2", "b3", "b4", "b5", "b6", "b7"]),
    ]);

    // test cast:
    // - sort descriptions
    // - limit
    // - expected cols
    let test_cases: Vec<(Vec<SortColumnDescription>, Option<usize>, Vec<Column>)> = vec![
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            Some(4),
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Int64Type::from_data(vec![7_i64, 1, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b7", "b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            vec![
                SortColumnDescription {
                    offset: 0,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false,
                },
                SortColumnDescription {
                    offset: 1,
                    asc: false,
                    nulls_first: false,
                    is_nullable: false,
                },
            ],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
    ];

    for (sort_descs, limit, expected) in test_cases {
        let res = DataBlock::sort(&block, &sort_descs, limit)?;

        for (entry, expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                entry.value.as_column().unwrap(),
                expect,
                "the column after sort is wrong, expect: {:?}, got: {:?}",
                expect,
                entry.value
            );
        }
    }

    let decimal_block = new_block(&[
        Decimal128Type::from_data(vec![6i128, 4, 3, 2, 1, 1, 7]),
        StringType::from_data(vec!["b1", "b2", "b3", "b4", "b5", "b6", "b7"]),
    ]);

    // test cast:
    // - sort descriptions
    // - limit
    // - expected cols
    let test_cases: Vec<(Vec<SortColumnDescription>, Option<usize>, Vec<Column>)> = vec![
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Decimal128Type::from_data(vec![1_i128, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            Some(4),
            vec![
                Decimal128Type::from_data(vec![1_i128, 1, 2, 3]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Decimal128Type::from_data(vec![7_i128, 1, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b7", "b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            vec![
                SortColumnDescription {
                    offset: 0,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false,
                },
                SortColumnDescription {
                    offset: 1,
                    asc: false,
                    nulls_first: false,
                    is_nullable: false,
                },
            ],
            None,
            vec![
                Decimal128Type::from_data(vec![1_i128, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
    ];

    for (sort_descs, limit, expected) in test_cases {
        let res = DataBlock::sort(&decimal_block, &sort_descs, limit)?;

        for (entry, expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                entry.value.as_column().unwrap(),
                expect,
                "the column after sort is wrong, expect: {:?}, got: {:?}",
                expect,
                entry.value
            );
        }
    }

    Ok(())
}

#[test]
fn test_blocks_merge_sort() -> Result<()> {
    let blocks = vec![
        new_block(&[
            Int64Type::from_data(vec![4i64, 6]),
            StringType::from_data(vec!["b2", "b1"]),
        ]),
        new_block(&[
            Int64Type::from_data(vec![2i64, 3]),
            StringType::from_data(vec!["b4", "b3"]),
        ]),
        new_block(&[
            Int64Type::from_data(vec![1i64, 1]),
            StringType::from_data(vec!["b6", "b5"]),
        ]),
    ];

    // test cast:
    // - name
    // - sort descriptions
    // - limit
    // - expected cols
    #[allow(clippy::type_complexity)]
    let test_cases: Vec<(
        String,
        Vec<SortColumnDescription>,
        Option<usize>,
        Vec<Column>,
    )> = vec![
        (
            "order by col1".to_string(),
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            "order by col1 limit 4".to_string(),
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            Some(4),
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3"]),
            ],
        ),
        (
            "order by col2 desc".to_string(),
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            "order by col1, col2 desc".to_string(),
            vec![
                SortColumnDescription {
                    offset: 0,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false,
                },
                SortColumnDescription {
                    offset: 1,
                    asc: false,
                    nulls_first: false,
                    is_nullable: false,
                },
            ],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
    ];

    struct NeverAbort;
    impl CheckAbort for NeverAbort {
        fn is_aborting(&self) -> bool {
            false
        }
        fn try_check_aborting(&self) -> Result<()> {
            Ok(())
        }
    }

    let aborting: AbortChecker = Arc::new(NeverAbort);

    for (name, sort_descs, limit, expected) in test_cases {
        let res = DataBlock::merge_sort(&blocks, &sort_descs, limit, aborting.clone())?;

        for (entry, expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                entry.value.as_column().unwrap(),
                expect,
                "{}: the column after sort is wrong, expect: {:?}, got: {:?}",
                name,
                expect,
                entry.value
            );
        }
    }

    Ok(())
}
