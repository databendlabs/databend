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

#[test]
fn test_data_block_sort() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let raw = DataBlock::create(schema, vec![
        Series::from_data(vec![6, 4, 3, 2, 1, 7]),
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
    Ok(())
}

#[test]
fn test_data_block_merge_sort() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let raw1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![3, 5, 7]),
        Series::from_data(vec!["b1", "b2", "b3"]),
    ]);

    let raw2 = DataBlock::create(schema, vec![
        Series::from_data(vec![2, 4, 6]),
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

    Ok(())
}
