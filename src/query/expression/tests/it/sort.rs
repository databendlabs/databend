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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::SortColumnDescription;

use crate::common::new_chunk;

#[test]
fn test_chunk_sort() -> Result<()> {
    let chunk = new_chunk(&[
        (
            DataType::Number(NumberDataType::Int64),
            Column::from_data(vec![6i64, 4, 3, 2, 1, 1, 7]),
        ),
        (
            DataType::String,
            Column::from_data(vec!["b1", "b2", "b3", "b4", "b5", "b6", "b7"]),
        ),
    ]);

    // test cast:
    // - sort descriptions
    // - limit
    // - expected cols
    let test_cases: Vec<(Vec<SortColumnDescription>, Option<usize>, Vec<Column>)> = vec![
        (
            vec![SortColumnDescription {
                index: 0,
                asc: true,
                nulls_first: false,
            }],
            None,
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3, 4, 6, 7]),
                Column::from_data(vec!["b5", "b6", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                index: 0,
                asc: true,
                nulls_first: false,
            }],
            Some(4),
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3]),
                Column::from_data(vec!["b5", "b6", "b4", "b3"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                index: 1,
                asc: false,
                nulls_first: false,
            }],
            None,
            vec![
                Column::from_data(vec![7_i64, 1, 1, 2, 3, 4, 6]),
                Column::from_data(vec!["b7", "b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            vec![
                SortColumnDescription {
                    index: 0,
                    asc: true,
                    nulls_first: false,
                },
                SortColumnDescription {
                    index: 1,
                    asc: false,
                    nulls_first: false,
                },
            ],
            None,
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3, 4, 6, 7]),
                Column::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
    ];

    for (sort_descs, limit, expected) in test_cases {
        let res = Chunk::sort(&chunk, &sort_descs, limit)?;

        for ((col, _), expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                col.as_column().unwrap(),
                expect,
                "the column after sort is wrong, expect: {:?}, got: {:?}",
                expect,
                col
            );
        }
    }

    Ok(())
}

#[test]
fn test_chunks_merge_sort() -> Result<()> {
    let chunks = vec![
        new_chunk(&[
            (
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![4i64, 6]),
            ),
            (DataType::String, Column::from_data(vec!["b2", "b1"])),
        ]),
        new_chunk(&[
            (
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![2i64, 3]),
            ),
            (DataType::String, Column::from_data(vec!["b4", "b3"])),
        ]),
        new_chunk(&[
            (
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![1i64, 1]),
            ),
            (DataType::String, Column::from_data(vec!["b6", "b5"])),
        ]),
    ];

    // test cast:
    // - name
    // - sort descriptions
    // - limit
    // - expected cols
    let test_cases: Vec<(
        String,
        Vec<SortColumnDescription>,
        Option<usize>,
        Vec<Column>,
    )> = vec![
        (
            "order by col1".to_string(),
            vec![SortColumnDescription {
                index: 0,
                asc: true,
                nulls_first: false,
            }],
            None,
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3, 4, 6]),
                Column::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            "order by col1 limit 4".to_string(),
            vec![SortColumnDescription {
                index: 0,
                asc: true,
                nulls_first: false,
            }],
            Some(4),
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3]),
                Column::from_data(vec!["b6", "b5", "b4", "b3"]),
            ],
        ),
        (
            "order by col2 desc".to_string(),
            vec![SortColumnDescription {
                index: 1,
                asc: false,
                nulls_first: false,
            }],
            None,
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3, 4, 6]),
                Column::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            "order by col1, col2 desc".to_string(),
            vec![
                SortColumnDescription {
                    index: 0,
                    asc: true,
                    nulls_first: false,
                },
                SortColumnDescription {
                    index: 1,
                    asc: false,
                    nulls_first: false,
                },
            ],
            None,
            vec![
                Column::from_data(vec![1_i64, 1, 2, 3, 4, 6]),
                Column::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
    ];

    let aborting: Arc<Box<dyn Fn() -> bool + Send + Sync + 'static>> = Arc::new(Box::new(|| false));
    for (name, sort_descs, limit, expected) in test_cases {
        let res = Chunk::merge_sort(&chunks, &sort_descs, limit, aborting.clone())?;

        for ((col, _), expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                col.as_column().unwrap(),
                expect,
                "{}: the column after sort is wrong, expect: {:?}, got: {:?}",
                name,
                expect,
                col
            );
        }
    }

    Ok(())
}
