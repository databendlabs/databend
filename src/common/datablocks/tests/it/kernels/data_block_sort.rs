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

use common_datablocks::*;
use common_datavalues::prelude::*;
use common_exception::Result;
use serde_json::json;

#[test]
fn test_data_block_sort() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let raw = DataBlock::create(schema, vec![
        Series::from_data(vec![6i64, 4, 3, 2, 1, 7]),
        Series::from_data(vec!["b1", "b2", "b3", "b4", "b5", "b6"]),
    ]);

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 1 | b5 |",
            "| 2 | b4 |",
            "| 3 | b3 |",
            "+---+----+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: false,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 7 | b6 |",
            "| 6 | b1 |",
            "| 4 | b2 |",
            "+---+----+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "b".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 6 | b1 |",
            "| 4 | b2 |",
            "| 3 | b3 |",
            "+---+----+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "b".to_owned(),
            asc: false,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 7 | b6 |",
            "| 1 | b5 |",
            "| 2 | b4 |",
            "+---+----+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c", VariantValue::to_data_type()),
        DataField::new("d", ArrayType::new_impl(i64::to_data_type())),
    ]);

    let raw = DataBlock::create(schema, vec![
        Series::from_data(vec![
            VariantValue::from(json!(true)),
            VariantValue::from(json!(123)),
            VariantValue::from(json!(12.34)),
            VariantValue::from(json!("abc")),
            VariantValue::from(json!([1, 2, 3])),
            VariantValue::from(json!({"a":"b"})),
        ]),
        Series::from_data(vec![
            ArrayValue::new(vec![1_i64.into(), 2_i64.into()]),
            ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 3_i64.into()]),
            ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 4_i64.into()]),
            ArrayValue::new(vec![4_i64.into(), 5_i64.into(), 6_i64.into()]),
            ArrayValue::new(vec![6_i64.into()]),
            ArrayValue::new(vec![7_i64.into(), 8_i64.into(), 9_i64.into()]),
        ]),
    ]);

    {
        let options = vec![SortColumnDescription {
            column_name: "c".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+-------+-----------+",
            "| c     | d         |",
            "+-------+-----------+",
            "| true  | [1, 2]    |",
            "| 12.34 | [1, 2, 4] |",
            "| 123   | [1, 2, 3] |",
            "+-------+-----------+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "c".to_owned(),
            asc: false,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+-----------+-----------+",
            "| c         | d         |",
            "+-----------+-----------+",
            "| [1,2,3]   | [6]       |",
            "| {\"a\":\"b\"} | [7, 8, 9] |",
            "| \"abc\"     | [4, 5, 6] |",
            "+-----------+-----------+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "d".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+-------+-----------+",
            "| c     | d         |",
            "+-------+-----------+",
            "| true  | [1, 2]    |",
            "| 123   | [1, 2, 3] |",
            "| 12.34 | [1, 2, 4] |",
            "+-------+-----------+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "d".to_owned(),
            asc: false,
            nulls_first: false,
        }];
        let results = DataBlock::sort_block(&raw, &options, Some(3))?;
        assert_eq!(raw.schema(), results.schema());

        let expected = vec![
            "+-----------+-----------+",
            "| c         | d         |",
            "+-----------+-----------+",
            "| {\"a\":\"b\"} | [7, 8, 9] |",
            "| [1,2,3]   | [6]       |",
            "| \"abc\"     | [4, 5, 6] |",
            "+-----------+-----------+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    Ok(())
}

#[test]
fn test_data_block_merge_sort() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let raw1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![3i64, 5, 7]),
        Series::from_data(vec!["b1", "b2", "b3"]),
    ]);

    let raw2 = DataBlock::create(schema, vec![
        Series::from_data(vec![2i64, 4, 6]),
        Series::from_data(vec!["b4", "b5", "b6"]),
    ]);

    {
        let options = vec![SortColumnDescription {
            column_name: "a".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::merge_sort_block(&raw1, &raw2, &options, None)?;

        assert_eq!(raw1.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 2 | b4 |",
            "| 3 | b1 |",
            "| 4 | b5 |",
            "| 5 | b2 |",
            "| 6 | b6 |",
            "| 7 | b3 |",
            "+---+----+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "b".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::merge_sort_block(&raw1, &raw2, &options, None)?;

        assert_eq!(raw1.schema(), results.schema());

        let expected = vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 3 | b1 |",
            "| 5 | b2 |",
            "| 7 | b3 |",
            "| 2 | b4 |",
            "| 4 | b5 |",
            "| 6 | b6 |",
            "+---+----+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("c", VariantValue::to_data_type()),
        DataField::new("d", ArrayType::new_impl(i64::to_data_type())),
    ]);

    let raw1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![
            VariantValue::from(json!(true)),
            VariantValue::from(json!("abc")),
            VariantValue::from(json!([1, 2, 3])),
        ]),
        Series::from_data(vec![
            ArrayValue::new(vec![1_i64.into(), 2_i64.into()]),
            ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 3_i64.into()]),
            ArrayValue::new(vec![6_i64.into()]),
        ]),
    ]);

    let raw2 = DataBlock::create(schema, vec![
        Series::from_data(vec![
            VariantValue::from(json!(12.34)),
            VariantValue::from(json!(123)),
            VariantValue::from(json!({"a":"b"})),
        ]),
        Series::from_data(vec![
            ArrayValue::new(vec![1_i64.into(), 2_i64.into(), 4_i64.into()]),
            ArrayValue::new(vec![4_i64.into(), 5_i64.into(), 6_i64.into()]),
            ArrayValue::new(vec![7_i64.into(), 8_i64.into(), 9_i64.into()]),
        ]),
    ]);

    {
        let options = vec![SortColumnDescription {
            column_name: "c".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::merge_sort_block(&raw1, &raw2, &options, None)?;

        assert_eq!(raw1.schema(), results.schema());

        let expected = vec![
            "+-----------+-----------+",
            "| c         | d         |",
            "+-----------+-----------+",
            "| true      | [1, 2]    |",
            "| 12.34     | [1, 2, 4] |",
            "| 123       | [4, 5, 6] |",
            "| \"abc\"     | [1, 2, 3] |",
            "| {\"a\":\"b\"} | [7, 8, 9] |",
            "| [1,2,3]   | [6]       |",
            "+-----------+-----------+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    {
        let options = vec![SortColumnDescription {
            column_name: "d".to_owned(),
            asc: true,
            nulls_first: false,
        }];
        let results = DataBlock::merge_sort_block(&raw1, &raw2, &options, None)?;

        assert_eq!(raw1.schema(), results.schema());

        let expected = vec![
            "+-----------+-----------+",
            "| c         | d         |",
            "+-----------+-----------+",
            "| true      | [1, 2]    |",
            "| \"abc\"     | [1, 2, 3] |",
            "| 12.34     | [1, 2, 4] |",
            "| 123       | [4, 5, 6] |",
            "| [1,2,3]   | [6]       |",
            "| {\"a\":\"b\"} | [7, 8, 9] |",
            "+-----------+-----------+",
        ];
        common_datablocks::assert_blocks_eq(expected, &[results]);
    }

    Ok(())
}
